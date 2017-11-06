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

use std::io;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
use bytes::Bytes;
use futures::{Async, AsyncSink, Stream, Sink};
use future_utils::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_core::net::UdpSocket;
use void::{ResultVoidExt};

/// A UDP socket that can easily be shared amongst a bunch of different tasks.
///
/// `SharedUdpSocket` can be used as a `Stream` to receive incoming packets along with their
/// addresses. The `with_address` method can be used to divert packets from a given address to a
/// seperate `Stream` so they can be processed seperately.
pub struct SharedUdpSocket {
    inner: Arc<SharedUdpSocketInner>,
    incoming_rx: UnboundedReceiver<(SocketAddr, Bytes)>,
}

/// A `Sink`/`Stream` that can be used to send/receive packets to/from a particular address.
pub struct WithAddress {
    inner: Arc<SharedUdpSocketInner>,
    incoming_rx: UnboundedReceiver<Bytes>,
    addr: SocketAddr,
}

struct SharedUdpSocketInner {
    socket: UdpSocket,
    with_addresses: Mutex<HashMap<SocketAddr, UnboundedSender<Bytes>>>,
    incoming_tx: UnboundedSender<(SocketAddr, Bytes)>,
}

impl SharedUdpSocket {
    /// Create a new `SharedUdpSocket` from a `UdpSocket`.
    pub fn share(socket: UdpSocket) -> SharedUdpSocket {
        let (tx, rx) = mpsc::unbounded();
        let inner = SharedUdpSocketInner {
            socket: socket,
            with_addresses: Mutex::new(HashMap::new()),
            incoming_tx: tx,
        };
        SharedUdpSocket {
            inner: Arc::new(inner),
            incoming_rx: rx,
        }
    }

    /// Creates a `WithAddress` object which receives all packets that arrive from the given
    /// address. `WithAddress` can also be used as a `Sink` to send packets. When the `WithAddress`
    /// is dropped, any further or unprocessed packets arriving from the given address will instead
    /// be received through the `SharedUdpSocket`.
    pub fn with_address(&self, addr: SocketAddr) -> WithAddress {
        let (tx, rx) = mpsc::unbounded();
        let inner = self.inner.clone();
        let ret = WithAddress {
            inner: inner,
            incoming_rx: rx,
            addr: addr,
        };
        let mut with_addresses = unwrap!(self.inner.with_addresses.lock());
        let _ = with_addresses.insert(addr, tx);
        ret
    }

    fn process(&mut self, addr: SocketAddr, data: Bytes) -> io::Result<Async<Option<(SocketAddr, Bytes)>>> {
        let mut with_addresses = unwrap!(self.inner.with_addresses.lock());
        let res = if let Some(sender) = with_addresses.get(&addr) {
            sender.unbounded_send(data)
        } else {
            return Ok(Async::Ready(Some((addr, data))));
        };
        match res {
            Ok(()) => {
                with_addresses.remove(&addr);
                Ok(Async::NotReady)
            },
            Err(send_error) => return Ok(Async::Ready(Some((addr, send_error.into_inner())))),
        }
    }
}

impl Stream for SharedUdpSocket {
    type Item = (SocketAddr, Bytes);
    type Error = io::Error;

    fn poll(&mut self) -> io::Result<Async<Option<(SocketAddr, Bytes)>>> {
        loop {
            match self.incoming_rx.poll().void_unwrap() {
                Async::Ready(Some((addr, data))) => {
                    let res = self.process(addr, data);
                    if let Ok(Async::NotReady) = res {
                        continue;
                    }
                    return res;
                },
                Async::Ready(None) => unreachable!(),
                Async::NotReady => break,
            }
        }

        while let Async::Ready(()) = self.inner.socket.poll_read() {
            let mut buff = [0; 64 * 1024 + 1];
            match self.inner.socket.recv_from(&mut buff) {
                Ok((n, addr)) => {
                    if n == buff.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "failed to recv entire dgram",
                        ));
                    }
                    let data = Bytes::from(&buff[..n]);

                    let res = self.process(addr, data);
                    if let Ok(Async::NotReady) = res {
                        continue;
                    }
                    return res;
                },
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        //self.inner.socket.need_read();
                        return Ok(Async::NotReady);
                    }
                    return Err(e)
                },
            }
        }

        Ok(Async::NotReady)
    }
}

impl Stream for WithAddress {
    type Item = Bytes;
    type Error = ();

    fn poll(&mut self) -> Result<Async<Option<Bytes>>, ()> {
        match self.incoming_rx.poll().void_unwrap() {
            Async::Ready(Some(data)) => Ok(Async::Ready(Some(data))),
            Async::Ready(None) => Err(()),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

impl Sink for WithAddress {
    type SinkItem = Bytes;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Bytes) -> io::Result<AsyncSink<Bytes>> {
        if let Async::Ready(()) = self.inner.socket.poll_write() {
            match self.inner.socket.send_to(&item, &self.addr) {
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
                        //self.inner.socket.need_write();
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

impl Drop for WithAddress {
    fn drop(&mut self) {
        let mut with_addresses = unwrap!(self.inner.with_addresses.lock());
        let _ = with_addresses.remove(&self.addr);
        drop(with_addresses);

        while let Async::Ready(Some(data)) = self.incoming_rx.poll().void_unwrap() {
            let _ = self.inner.incoming_tx.unbounded_send((self.addr, data));
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
                    let (addr, data) = unwrap!(opt);
                    assert_eq!(&data[..], b"qqqq");
                    assert_eq!(addr, addr0);

                    let with_addr = shared.with_address(addr0);

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
                                let (addr, data) = unwrap!(opt);
                                assert_eq!(&data[..], b"eeee");
                                assert_eq!(addr, addr1);

                                with_addr
                                .into_future()
                                .map_err(|((), _)| panic!())
                                .and_then(move |(opt, with_addr)| {
                                    let data = unwrap!(opt);
                                    assert_eq!(&data[..], b"wwww");

                                    with_addr
                                    .send(Bytes::from(&b"rrrr"[..]))
                                    .map_err(|e| panic!("{}", e))
                                    .and_then(move |with_addr| {
                                        let buff = [0; 10];
                                        sock0
                                        .recv_dgram(buff)
                                        .map_err(|e| panic!("{}", e))
                                        .and_then(move |(sock0, data, len, addr)| {
                                            assert_eq!(addr, shared_addr);
                                            assert_eq!(&data[..len], b"rrrr");
                                            sock0
                                            .send_dgram(b"tttt", shared_addr)
                                            .map_err(|e| panic!("{}", e))
                                            .and_then(move |(_sock0, _)| {
                                                drop(with_addr);

                                                shared
                                                .into_future()
                                                .map_err(|(e, _)| panic!("{}", e))
                                                .map(move |(opt, _shared)| {
                                                    let (addr, data) = unwrap!(opt);
                                                    assert_eq!(&data[..], b"tttt");
                                                    assert_eq!(addr, addr0);
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

