extern crate futures;
extern crate future_utils;
extern crate tokio;
#[macro_use]
extern crate unwrap;
extern crate bytes;
extern crate void;
#[cfg(test)]
#[macro_use]
extern crate net_literals;
#[macro_use]
extern crate log;

use std::{mem, io};
use std::collections::{hash_map, HashMap};
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
use bytes::{BytesMut, Bytes};
use futures::{Async, AsyncSink, Stream, Sink};
use future_utils::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::net::UdpSocket;
use void::{ResultVoidExt};

/// A UDP socket that can easily be shared amongst a bunch of different futures.
pub struct SharedUdpSocket {
    inner: Arc<SharedUdpSocketInner>,
}

pub struct IncomingEndpoints {
    inner: Arc<SharedUdpSocketInner>,
    incoming_rx: UnboundedReceiver<UdpEndpoint>,
    buffer: BytesMut,
}

/// A `Sink`/`Stream` that can be used to send/receive packets to/from a particular address.
/// 
/// These can be created by calling `SharedUdpSocket::endpoint`. `SharedUdpSocket` will also
/// yield these (when used as a `Stream`) when it receives a packet from a new address.
pub struct UdpEndpoint {
    inner: Arc<SharedUdpSocketInner>,
    incoming_rx: UnboundedReceiver<Bytes>,
    addr: SocketAddr,
    buffer: BytesMut,
}

struct SharedUdpSocketInner {
    socket: Mutex<Option<UdpSocket>>,
    endpoints: Mutex<HashMap<SocketAddr, UnboundedSender<Bytes>>>,
    incoming_tx: Mutex<Option<UnboundedSender<UdpEndpoint>>>,
}

impl SharedUdpSocket {
    /// Create a new `SharedUdpSocket` from a `UdpSocket`.
    pub fn share(socket: UdpSocket) -> (SharedUdpSocket, IncomingEndpoints) {
        trace!("creating shared udp socket on address {:?}", socket.local_addr());
        let (tx, rx) = mpsc::unbounded();
        let inner = SharedUdpSocketInner {
            socket: Mutex::new(Some(socket)),
            endpoints: Mutex::new(HashMap::new()),
            incoming_tx: Mutex::new(Some(tx)),
        };
        let inner = Arc::new(inner);
        let shared = SharedUdpSocket {
            inner: inner.clone(),
        };
        let incoming = IncomingEndpoints {
            inner,
            incoming_rx: rx,
            buffer: BytesMut::new(),
        };
        (shared, incoming)
    }

    /// Creates a `UdpEndpoint` object which receives all packets that arrive from the given
    /// address. `UdpEndpoint` can also be used as a `Sink` to send packets. If another
    /// `UdpEndpoint` with the given address already exists then it will no longer receive packets
    /// since the newly created `UdpEndpoint` will take precedence.
    pub fn endpoint(&self, addr: SocketAddr) -> UdpEndpoint {
        let (tx, endpoint) = endpoint_new(&self.inner, addr);
        let mut endpoints = unwrap!(self.inner.endpoints.lock());
        let _ = endpoints.insert(addr, tx);
        endpoint
    }

    /// Creates a `UdpEndpoint` object which receives all packets that arrive from the given
    /// address. `UdpEndpoint` can also be used as a `Sink` to send packets. Unlike the `endpoint`
    /// method, this method will not replace any pre-existing `UdpEndpoint` associated with the
    /// given address and will instead return `None` if one exists.
    pub fn try_endpoint(&self, addr: SocketAddr) -> Option<UdpEndpoint> {
        let mut endpoints = unwrap!(self.inner.endpoints.lock());
        match endpoints.entry(addr) {
            hash_map::Entry::Occupied(..) => None,
            hash_map::Entry::Vacant(ve) => {
                let (tx, endpoint) = endpoint_new(&self.inner, addr);
                let _ = ve.insert(tx);
                Some(endpoint)
            },
        }
    }

    /// Steals the udp socket (if it hasn't already been stolen) causing all other
    /// `SharedUdpSocket` and `UdpEndpoint` streams to end.
    pub fn steal(self) -> Option<UdpSocket> {
        let mut socket_opt = unwrap!(self.inner.socket.lock());
        socket_opt.take()
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

fn pump(inner: &Arc<SharedUdpSocketInner>, buffer: &mut BytesMut) -> io::Result<()> {
    let mut socket_opt = unwrap!(inner.socket.lock());
    let socket = match *socket_opt {
        Some(ref mut socket) => socket,
        None => return Ok(()),
    };

    loop {
        let min_capacity = 64 * 1024 + 1;
        let capacity = buffer.capacity();
        if capacity < min_capacity {
            buffer.reserve(min_capacity - capacity);
        }
        let capacity = buffer.capacity();
        unsafe {
            buffer.set_len(capacity)
        }
        match socket.poll_recv_from(&mut *buffer) {
            Ok(Async::Ready((n, addr))) => {
                if n == buffer.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "failed to recv entire dgram",
                    ));
                }
                let data = buffer.split_to(n).freeze();
                let mut endpoints = unwrap!(inner.endpoints.lock());
                let drop_after_unlock = match endpoints.entry(addr) {
                    hash_map::Entry::Occupied(mut oe) => {
                        match oe.get().unbounded_send(data) {
                            Ok(()) => None,
                            Err(send_error) => {
                                if let Some(ref incoming_tx) = *unwrap!(inner.incoming_tx.lock()) {
                                    let (tx, endpoint) = endpoint_new(inner, addr);

                                    unwrap!(tx.unbounded_send(send_error.into_inner()));
                                    let _ = mem::replace(oe.get_mut(), tx);
                                    match incoming_tx.unbounded_send(endpoint) {
                                        Ok(()) => None,
                                        Err(send_error) => Some(send_error.into_inner()),
                                    }
                                } else {
                                    None
                                }
                            },
                        }
                    },
                    hash_map::Entry::Vacant(ve) => {
                        if let Some(ref incoming_tx) = *unwrap!(inner.incoming_tx.lock()) {
                            let (tx, endpoint) = endpoint_new(inner, addr);

                            unwrap!(tx.unbounded_send(data));
                            ve.insert(tx);
                            match incoming_tx.unbounded_send(endpoint) {
                                Ok(()) => None,
                                Err(send_error) => Some(send_error.into_inner()),
                            }
                        } else {
                            None
                        }
                    },
                };
                drop(endpoints);
                drop(drop_after_unlock);
            },
            Ok(Async::NotReady) => return Ok(()),
            Err(e) => {
                match e.kind() {
                    io::ErrorKind::WouldBlock => return Ok(()),
                    io::ErrorKind::ConnectionReset => continue,
                    _ => return Err(e),
                }
            },
        }
    }
}

fn endpoint_new(inner: &Arc<SharedUdpSocketInner>, addr: SocketAddr) -> (UnboundedSender<Bytes>, UdpEndpoint) {
    let (tx, rx) = mpsc::unbounded();
    let inner = inner.clone();
    let endpoint = UdpEndpoint {
        inner: inner,
        incoming_rx: rx,
        addr: addr,
        buffer: BytesMut::new(),
    };
    (tx, endpoint)
}

impl UdpEndpoint {
    /// Get the remote address that this `UdpEndpoint` sends/receives packets to/from.
    pub fn remote_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Steals the udp socket (if it hasn't already been stolen) causing all other
    /// `SharedUdpSocket` and `UdpEndpoint` streams to end.
    pub fn steal(self) -> Option<UdpSocket> {
        let mut socket_opt = unwrap!(self.inner.socket.lock());
        socket_opt.take()
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

impl SharedUdpSocketInner {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let socket_opt = unwrap!(self.socket.lock());
        match *socket_opt {
            Some(ref socket) => socket.local_addr(),
            None => Err(io::Error::new(io::ErrorKind::Other, "socket has been stolen")),
        }
    }
}

impl Stream for IncomingEndpoints {
    type Item = UdpEndpoint;
    type Error = io::Error;

    fn poll(&mut self) -> io::Result<Async<Option<UdpEndpoint>>> {
        pump(&self.inner, &mut self.buffer)?;

        Ok(self.incoming_rx.poll().void_unwrap())
    }
}

impl Stream for UdpEndpoint {
    type Item = Bytes;
    type Error = io::Error;

    fn poll(&mut self) -> io::Result<Async<Option<Bytes>>> {
        pump(&self.inner, &mut self.buffer)?;

        Ok(self.incoming_rx.poll().void_unwrap())
    }
}

impl Sink for UdpEndpoint {
    type SinkItem = Bytes;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Bytes) -> io::Result<AsyncSink<Bytes>> {
        let mut socket_opt = unwrap!(self.inner.socket.lock());
        let socket = match *socket_opt {
            Some(ref mut socket) => socket,
            None => return Err(io::ErrorKind::NotConnected.into()),
        };

        match socket.poll_send_to(&item, &self.addr) {
            Ok(Async::Ready(n)) => {
                if n != item.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "failed to send entire dgram",
                    ));
                }
                return Ok(AsyncSink::Ready);
            },
            Ok(Async::NotReady) => return Ok(AsyncSink::NotReady(item)),
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    return Ok(AsyncSink::NotReady(item));
                }
                return Err(e);
            },
        }
    }

    fn poll_complete(&mut self) -> io::Result<Async<()>> {
        Ok(Async::Ready(()))
    }
}

impl Drop for SharedUdpSocket {
    fn drop(&mut self) {
        let mut incoming_tx = unwrap!(self.inner.incoming_tx.lock());
        *incoming_tx = None;
    }
}

impl Drop for UdpEndpoint {
    fn drop(&mut self) {
        let mut endpoints = unwrap!(self.inner.endpoints.lock());
        let _ = endpoints.remove(&self.addr);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::Future;

    #[test]
    fn test() {
        let sock0 = unwrap!(UdpSocket::bind(&addr!("127.0.0.1:0")));
        let addr0 = unwrap!(sock0.local_addr());
        let sock1 = unwrap!(UdpSocket::bind(&addr!("127.0.0.1:0")));
        let addr1 = unwrap!(sock1.local_addr());

        let shared = unwrap!(UdpSocket::bind(&addr!("127.0.0.1:0")));
        let shared_addr = unwrap!(shared.local_addr());
        let (_shared, incoming) = SharedUdpSocket::share(shared);

        tokio::run({
            sock0
            .send_dgram(b"qqqq", &shared_addr)
            .map_err(|e| panic!("{}", e))
            .and_then(move |(sock0, _)| {
                incoming
                .into_future()
                .map_err(|(e, _)| panic!("{}", e))
                .and_then(move |(opt, shared)| {
                    let endpoint_0 = unwrap!(opt);
                    assert_eq!(endpoint_0.remote_addr(), addr0);

                    endpoint_0
                    .into_future()
                    .map_err(|(e, _)| panic!("{}", e))
                    .and_then(move |(opt, endpoint_0)| {
                        let data = unwrap!(opt);
                        assert_eq!(&data[..], b"qqqq");

                        sock0
                        .send_dgram(b"wwww", &shared_addr)
                        .map_err(|e| panic!("{}", e))
                        .and_then(move |(sock0, _)| {
                            sock1
                            .send_dgram(b"eeee", &shared_addr)
                            .map_err(|e| panic!("{}", e))
                            .and_then(move |_sock1| {
                                shared
                                .into_future()
                                .map_err(|(e, _)| panic!("{}", e))
                                .and_then(move |(opt, shared)| {
                                    let endpoint_1 = unwrap!(opt);
                                    assert_eq!(endpoint_1.remote_addr(), addr1);
                                    drop(shared);

                                    endpoint_1
                                    .into_future()
                                    .map_err(|(e, _)| panic!("{}", e))
                                    .and_then(move |(opt, _endpoint_1)| {
                                        let data = unwrap!(opt);
                                        assert_eq!(&data[..], b"eeee");

                                        endpoint_0
                                        .into_future()
                                        .map_err(|(e, _)| panic!("{}", e))
                                        .and_then(move |(opt, endpoint_0)| {
                                            let data = unwrap!(opt);
                                            assert_eq!(&data[..], b"wwww");

                                            endpoint_0
                                            .send(Bytes::from(&b"rrrr"[..]))
                                            .map_err(|e| panic!("{}", e))
                                            .and_then(move |endpoint_0| {
                                                let buff = [0; 10];

                                                sock0
                                                .recv_dgram(buff)
                                                .map_err(|e| panic!("{}", e))
                                                .map(move |(_sock0, data, len, addr)| {
                                                    assert_eq!(addr, shared_addr);
                                                    assert_eq!(&data[..len], b"rrrr");
                                                    assert!(endpoint_0.steal().is_some());
                                                })
                                            })
                                        })
                                    })
                                })
                            })
                        })
                    })
                })
            })
        });
    }
}

