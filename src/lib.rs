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

use std::{mem, io};
use std::collections::{hash_map, HashMap};
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
use bytes::Bytes;
use futures::{Async, AsyncSink, Stream, Sink};
use future_utils::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_core::net::UdpSocket;
use void::{ResultVoidExt};

/// A UDP socket that can easily be shared amongst a bunch of different futures.
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
    ///
    /// # Note:
    /// 
    /// You must continue to `poll` the `SharedUdpSocket` in order for packets to arrive on the
    /// `WithAddress`
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

    fn process(&mut self, addr: SocketAddr, data: Bytes) -> io::Result<Async<Option<WithAddress>>> {
        let mut with_addresses = unwrap!(self.inner.with_addresses.lock());
        match with_addresses.entry(addr) {
            hash_map::Entry::Occupied(mut oe) => {
                match oe.get().unbounded_send(data) {
                    Ok(()) => Ok(Async::NotReady),
                    Err(send_error) => {
                        let (tx, rx) = mpsc::unbounded();
                        let inner = self.inner.clone();
                        let ret = WithAddress {
                            inner: inner,
                            incoming_rx: rx,
                            addr: addr,
                        };

                        unwrap!(tx.unbounded_send(send_error.into_inner()));
                        let _ = mem::replace(oe.get_mut(), tx);
                        Ok(Async::Ready(Some(ret)))
                    },
                }
            },
            hash_map::Entry::Vacant(ve) => {
                let (tx, rx) = mpsc::unbounded();
                let inner = self.inner.clone();
                let ret = WithAddress {
                    inner: inner,
                    incoming_rx: rx,
                    addr: addr,
                };

                unwrap!(tx.unbounded_send(data));
                ve.insert(tx);
                Ok(Async::Ready(Some(ret)))
            },
        }
    }
}

impl WithAddress {
    pub fn remote_addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Stream for SharedUdpSocket {
    type Item = WithAddress;
    type Error = io::Error;

    fn poll(&mut self) -> io::Result<Async<Option<WithAddress>>> {
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
                    let with_addr0 = unwrap!(opt);
                    assert_eq!(with_addr0.remote_addr(), addr0);

                    with_addr0
                    .into_future()
                    .map_err(|((), _)| panic!())
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

                                    with_addr1
                                    .into_future()
                                    .map_err(|((), _)| panic!())
                                    .and_then(move |(opt, _with_addr1)| {
                                        let data = unwrap!(opt);
                                        assert_eq!(&data[..], b"eeee");
                                        drop(with_addr0);

                                        shared
                                        .into_future()
                                        .map_err(|(e, _)| panic!("{}", e))
                                        .and_then(move |(opt, _shared)| {
                                            let with_addr0 = unwrap!(opt);
                                            assert_eq!(with_addr0.remote_addr(), addr0);

                                            with_addr0
                                            .into_future()
                                            .map_err(|((), _)| panic!())
                                            .and_then(move |(opt, with_addr0)| {
                                                let data = unwrap!(opt);
                                                assert_eq!(&data[..], b"wwww");

                                                with_addr0
                                                .send(Bytes::from(&b"rrrr"[..]))
                                                .and_then(move |_with_addr0| {
                                                    let buff = [0; 10];

                                                    sock0
                                                    .recv_dgram(buff)
                                                    .map_err(|e| panic!("{}", e))
                                                    .map(move |(_sock0, data, len, addr)| {
                                                        assert_eq!(addr, shared_addr);
                                                        assert_eq!(&data[..len], b"rrrr");
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
            })
        });
        unwrap!(res)
    }
}

