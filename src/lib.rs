extern crate futures;
extern crate future_utils;
extern crate tokio_core;
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
use tokio_core::net::UdpSocket;
use void::{ResultVoidExt};

/// A UDP socket that can easily be shared amongst a bunch of different futures.
pub struct SharedUdpSocket {
    some: Option<SomeSharedUdpSocket>,
}

struct SomeSharedUdpSocket {
    inner: Arc<SharedUdpSocketInner>,
    incoming_rx: UnboundedReceiver<WithAddress>,
    buffer: BytesMut,
}

/// A `Sink`/`Stream` that can be used to send/receive packets to/from a particular address.
/// 
/// These can be created by calling `SharedUdpSocket::with_address`. `SharedUdpSocket` will also
/// yield these (when used as a `Stream`) when it receives a packet from a new address.
pub struct WithAddress {
    some: Option<SomeWithAddress>,
}

struct SomeWithAddress {
    inner: Arc<SharedUdpSocketInner>,
    incoming_rx: UnboundedReceiver<Bytes>,
    addr: SocketAddr,
    buffer: BytesMut,
}

struct SharedUdpSocketInner {
    socket: Mutex<Option<UdpSocket>>,
    with_addresses: Mutex<HashMap<SocketAddr, UnboundedSender<Bytes>>>,
    incoming_tx: Mutex<Option<UnboundedSender<WithAddress>>>,
}

impl SharedUdpSocket {
    /// Create a new `SharedUdpSocket` from a `UdpSocket`.
    pub fn share(socket: UdpSocket) -> SharedUdpSocket {
        trace!("creating shared udp socket on address {:?}", socket.local_addr());
        let (tx, rx) = mpsc::unbounded();
        let inner = SharedUdpSocketInner {
            socket: Mutex::new(Some(socket)),
            with_addresses: Mutex::new(HashMap::new()),
            incoming_tx: Mutex::new(Some(tx)),
        };
        SharedUdpSocket {
            some: Some(SomeSharedUdpSocket {
                inner: Arc::new(inner),
                incoming_rx: rx,
                buffer: BytesMut::new(),
            }),
        }
    }

    /// Creates a `WithAddress` object which receives all packets that arrive from the given
    /// address. `WithAddress` can also be used as a `Sink` to send packets.
    pub fn with_address(&self, addr: SocketAddr) -> WithAddress {
        let some = unwrap!(self.some.as_ref());
        let (tx, with_addr) = with_addr_new(&some.inner, addr);
        let mut with_addresses = unwrap!(some.inner.with_addresses.lock());
        let _ = with_addresses.insert(addr, tx);
        with_addr
    }

    /// Steals the udp socket (if it hasn't already been stolen) causing all other
    /// `SharedUdpSocket` and `WithAddress` streams to end.
    pub fn steal(mut self) -> Option<UdpSocket> {
        let some = unwrap!(self.some.take());
        let mut socket_opt = unwrap!(some.inner.socket.lock());
        socket_opt.take()
    }

    pub fn ttl(&self) -> io::Result<u32> {
        let some = unwrap!(self.some.as_ref());
        some.inner.ttl()
    }

    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        let some = unwrap!(self.some.as_ref());
        some.inner.set_ttl(ttl)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let some = unwrap!(self.some.as_ref());
        some.inner.local_addr()
    }
}

fn pump(inner: &Arc<SharedUdpSocketInner>, buffer: &mut BytesMut) -> io::Result<()> {
    let socket_opt = unwrap!(inner.socket.lock());
    let socket = match *socket_opt {
        Some(ref socket) => socket,
        None => return Ok(()),
    };

    while let Async::Ready(()) = socket.poll_read() {
        let min_capacity = 64 * 1024 + 1;
        let capacity = buffer.capacity();
        if capacity < min_capacity {
            buffer.reserve(min_capacity - capacity);
        }
        let capacity = buffer.capacity();
        unsafe {
            buffer.set_len(capacity)
        }
        match socket.recv_from(&mut *buffer) {
            Ok((n, addr)) => {
                if n == buffer.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "failed to recv entire dgram",
                    ));
                }
                let data = buffer.split_to(n).freeze();
                let mut with_addresses = unwrap!(inner.with_addresses.lock());
                let drop_after_unlock = match with_addresses.entry(addr) {
                    hash_map::Entry::Occupied(mut oe) => {
                        match oe.get().unbounded_send(data) {
                            Ok(()) => None,
                            Err(send_error) => {
                                if let Some(ref incoming_tx) = *unwrap!(inner.incoming_tx.lock()) {
                                    let (tx, with_addr) = with_addr_new(inner, addr);

                                    unwrap!(tx.unbounded_send(send_error.into_inner()));
                                    let _ = mem::replace(oe.get_mut(), tx);
                                    match incoming_tx.unbounded_send(with_addr) {
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
                            let (tx, with_addr) = with_addr_new(inner, addr);

                            unwrap!(tx.unbounded_send(data));
                            ve.insert(tx);
                            match incoming_tx.unbounded_send(with_addr) {
                                Ok(()) => None,
                                Err(send_error) => Some(send_error.into_inner()),
                            }
                        } else {
                            None
                        }
                    },
                };
                drop(with_addresses);
                drop(drop_after_unlock);
            },
            Err(e) => {
                match e.kind() {
                    io::ErrorKind::WouldBlock => return Ok(()),
                    io::ErrorKind::ConnectionReset => continue,
                    _ => return Err(e),
                }
            },
        }
    }

    Ok(())
}

fn with_addr_new(inner: &Arc<SharedUdpSocketInner>, addr: SocketAddr) -> (UnboundedSender<Bytes>, WithAddress) {
    let (tx, rx) = mpsc::unbounded();
    let inner = inner.clone();
    let with_addr = WithAddress {
        some: Some(SomeWithAddress {
            inner: inner,
            incoming_rx: rx,
            addr: addr,
            buffer: BytesMut::new(),
        }),
    };
    (tx, with_addr)
}

impl WithAddress {
    /// Get the remote address that this `WithAddress` sends/receives packets to/from.
    pub fn remote_addr(&self) -> SocketAddr {
        let some = unwrap!(self.some.as_ref());
        some.addr
    }

    /// Steals the udp socket (if it hasn't already been stolen) causing all other
    /// `SharedUdpSocket` and `WithAddress` streams to end.
    pub fn steal(mut self) -> Option<UdpSocket> {
        let some = unwrap!(self.some.take());
        let mut socket_opt = unwrap!(some.inner.socket.lock());
        socket_opt.take()
    }

    pub fn ttl(&self) -> io::Result<u32> {
        let some = unwrap!(self.some.as_ref());
        some.inner.ttl()
    }

    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        let some = unwrap!(self.some.as_ref());
        some.inner.set_ttl(ttl)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let some = unwrap!(self.some.as_ref());
        some.inner.local_addr()
    }
}

impl SharedUdpSocketInner {
    pub fn ttl(&self) -> io::Result<u32> {
        let socket_opt = unwrap!(self.socket.lock());
        match *socket_opt {
            Some(ref socket) => socket.ttl(),
            None => Err(io::Error::new(io::ErrorKind::Other, "socket has been stolen")),
        }
    }

    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        let socket_opt = unwrap!(self.socket.lock());
        match *socket_opt {
            Some(ref socket) => socket.set_ttl(ttl),
            None => Err(io::Error::new(io::ErrorKind::Other, "socket has been stolen")),
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let socket_opt = unwrap!(self.socket.lock());
        match *socket_opt {
            Some(ref socket) => socket.local_addr(),
            None => Err(io::Error::new(io::ErrorKind::Other, "socket has been stolen")),
        }
    }
}

impl Stream for SharedUdpSocket {
    type Item = WithAddress;
    type Error = io::Error;

    fn poll(&mut self) -> io::Result<Async<Option<WithAddress>>> {
        let some = unwrap!(self.some.as_mut());
        pump(&some.inner, &mut some.buffer)?;

        Ok(some.incoming_rx.poll().void_unwrap())
    }
}

impl Stream for WithAddress {
    type Item = Bytes;
    type Error = io::Error;

    fn poll(&mut self) -> io::Result<Async<Option<Bytes>>> {
        let some = unwrap!(self.some.as_mut());
        pump(&some.inner, &mut some.buffer)?;

        Ok(some.incoming_rx.poll().void_unwrap())
    }
}

impl Sink for WithAddress {
    type SinkItem = Bytes;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Bytes) -> io::Result<AsyncSink<Bytes>> {
        let some = unwrap!(self.some.as_mut());

        let socket_opt = unwrap!(some.inner.socket.lock());
        let socket = match *socket_opt {
            Some(ref socket) => socket,
            None => return Err(io::ErrorKind::NotConnected.into()),
        };

        if let Async::Ready(()) = socket.poll_write() {
            match socket.send_to(&item, &some.addr) {
                Ok(n) => {
                    if n != item.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "failed to send entire dgram",
                        ));
                    }
                    return Ok(AsyncSink::Ready);
                },
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        return Ok(AsyncSink::NotReady(item));
                    }
                    return Err(e);
                },
            }
        }
        Ok(AsyncSink::NotReady(item))
    }

    fn poll_complete(&mut self) -> io::Result<Async<()>> {
        Ok(Async::Ready(()))
    }
}

impl Drop for SharedUdpSocket {
    fn drop(&mut self) {
        if let Some(some) = self.some.take() {
            let mut incoming_tx = unwrap!(some.inner.incoming_tx.lock());
            *incoming_tx = None;
        }
    }
}

impl Drop for WithAddress {
    fn drop(&mut self) {
        if let Some(some) = self.some.take() {
            let mut with_addresses = unwrap!(some.inner.with_addresses.lock());
            let _ = with_addresses.remove(&some.addr);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::Future;

    #[test]
    fn test() {
        let mut core = unwrap!(tokio_core::reactor::Core::new());
        let handle = core.handle();
        
        let sock0 = unwrap!(UdpSocket::bind(&addr!("127.0.0.1:0"), &handle));
        let addr0 = unwrap!(sock0.local_addr());
        let sock1 = unwrap!(UdpSocket::bind(&addr!("127.0.0.1:0"), &handle));
        let addr1 = unwrap!(sock1.local_addr());

        let shared = unwrap!(UdpSocket::bind(&addr!("127.0.0.1:0"), &handle));
        let shared_addr = unwrap!(shared.local_addr());
        let shared = SharedUdpSocket::share(shared);

        let res = core.run({
            sock0
            .send_dgram(b"qqqq", shared_addr)
            .map_err(|e| panic!("{}", e))
            .and_then(move |(sock0, _)| {
                shared
                .into_future()
                .map_err(|(e, _)| panic!("{}", e))
                .and_then(move |(opt, shared)| {
                    let with_addr0 = unwrap!(opt);
                    assert_eq!(with_addr0.remote_addr(), addr0);

                    with_addr0
                    .into_future()
                    .map_err(|(e, _)| panic!("{}", e))
                    .and_then(move |(opt, with_addr0)| {
                        let data = unwrap!(opt);
                        assert_eq!(&data[..], b"qqqq");

                        sock0
                        .send_dgram(b"wwww", shared_addr)
                        .map_err(|e| panic!("{}", e))
                        .and_then(move |(sock0, _)| {
                            sock1
                            .send_dgram(b"eeee", shared_addr)
                            .map_err(|e| panic!("{}", e))
                            .and_then(move |_sock1| {
                                shared
                                .into_future()
                                .map_err(|(e, _)| panic!("{}", e))
                                .and_then(move |(opt, shared)| {
                                    let with_addr1 = unwrap!(opt);
                                    assert_eq!(with_addr1.remote_addr(), addr1);
                                    drop(shared);

                                    with_addr1
                                    .into_future()
                                    .map_err(|(e, _)| panic!("{}", e))
                                    .and_then(move |(opt, _with_addr1)| {
                                        let data = unwrap!(opt);
                                        assert_eq!(&data[..], b"eeee");

                                        with_addr0
                                        .into_future()
                                        .map_err(|(e, _)| panic!("{}", e))
                                        .and_then(move |(opt, with_addr0)| {
                                            let data = unwrap!(opt);
                                            assert_eq!(&data[..], b"wwww");

                                            with_addr0
                                            .send(Bytes::from(&b"rrrr"[..]))
                                            .and_then(move |with_addr0| {
                                                let buff = [0; 10];

                                                sock0
                                                .recv_dgram(buff)
                                                .map_err(|e| panic!("{}", e))
                                                .map(move |(_sock0, data, len, addr)| {
                                                    assert_eq!(addr, shared_addr);
                                                    assert_eq!(&data[..len], b"rrrr");
                                                    assert!(with_addr0.steal().is_some());
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
        unwrap!(res)
    }
}

