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
use bytes::{BytesMut, Bytes};
use futures::{future, Async, AsyncSink, Stream, Sink};
use futures::task::{self, Task};
use future_utils::mpsc::{self, UnboundedReceiver, UnboundedSender};
use future_utils::{BoxFuture, FutureExt};
use tokio_core::net::UdpSocket;
use void::{Void, ResultVoidExt};

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
    socket: UdpSocket,
    with_addresses: Mutex<HashMap<SocketAddr, UnboundedSender<Bytes>>>,
    incoming_tx: UnboundedSender<WithAddress>,
    take_task: Mutex<Option<Task>>,
}

impl SharedUdpSocket {
    /// Create a new `SharedUdpSocket` from a `UdpSocket`.
    pub fn share(socket: UdpSocket) -> SharedUdpSocket {
        let (tx, rx) = mpsc::unbounded();
        let inner = SharedUdpSocketInner {
            socket: socket,
            with_addresses: Mutex::new(HashMap::new()),
            incoming_tx: tx,
            take_task: Mutex::new(None),
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

    /// Creates a future which yields the inner `UdpSocket` once all other references to the socket
    /// have been dropped. If a `WithAddress` is already trying to take the socket (using
    /// `WithAddress::try_take`) then this method will error, returning back the `SharedUdpSocket`.
    /// After calling this method, all other `SharedUdpSocket` and `WithAddress` streams that are
    /// reading from the socket will end.
    pub fn try_take(mut self) -> Result<BoxFuture<UdpSocket, Void>, SharedUdpSocket> {
        let some = unwrap!(self.some.take());
        let incoming_rx = some.incoming_rx;
        let buffer = some.buffer;
        try_take(some.inner)
        .map_err(move |inner| {
            SharedUdpSocket {
                some: Some(SomeSharedUdpSocket {
                    inner,
                    incoming_rx,
                    buffer,
                }),
            }
        })
    }

    /// Get a reference to the inner `UdpSocket`
    pub fn get_ref(&self) -> &UdpSocket {
        let some = unwrap!(self.some.as_ref());
        &some.inner.socket
    }
}

fn try_take(inner: Arc<SharedUdpSocketInner>) -> Result<BoxFuture<UdpSocket, Void>, Arc<SharedUdpSocketInner>> {
    let inner = match Arc::try_unwrap(inner) {
        Ok(inner) => return Ok(future::ok(inner.socket).into_boxed()),
        Err(inner) => inner,
    };

    let someone_already_has_dibs = {
        let mut take_task = unwrap!(inner.take_task.lock());
        match *take_task {
            Some(..) => true,
            None => {
                *take_task = Some(task::current());
                false
            },
        }
    };
    if someone_already_has_dibs {
        return Err(inner);
    }

    let inner = match Arc::try_unwrap(inner) {
        Ok(inner) => return Ok(future::ok(inner.socket).into_boxed()),
        Err(inner) => inner,
    };

    let mut inner = Some(inner);

    Ok({
        future::poll_fn(move || {
            inner = match Arc::try_unwrap(unwrap!(inner.take())) {
                Ok(inner) => return Ok(Async::Ready(inner.socket)),
                Err(inner) => Some(inner),
            };
            Ok(Async::NotReady)
        })
        .into_boxed()
    })
}

fn pump(inner: &Arc<SharedUdpSocketInner>, buffer: &mut BytesMut) -> io::Result<()> {
    while let Async::Ready(()) = inner.socket.poll_read() {
        let min_capacity = 64 * 1024 + 1;
        let capacity = buffer.capacity();
        if capacity < min_capacity {
            buffer.reserve(min_capacity - capacity);
        }
        let capacity = buffer.capacity();
        unsafe {
            buffer.set_len(capacity)
        }
        match inner.socket.recv_from(&mut *buffer) {
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
                                let (tx, with_addr) = with_addr_new(inner, addr);

                                unwrap!(tx.unbounded_send(send_error.into_inner()));
                                let _ = mem::replace(oe.get_mut(), tx);
                                match inner.incoming_tx.unbounded_send(with_addr) {
                                    Ok(()) => None,
                                    Err(send_error) => Some(send_error.into_inner()),
                                }
                            },
                        }
                    },
                    hash_map::Entry::Vacant(ve) => {
                        let (tx, with_addr) = with_addr_new(inner, addr);

                        unwrap!(tx.unbounded_send(data));
                        ve.insert(tx);
                        match inner.incoming_tx.unbounded_send(with_addr) {
                            Ok(()) => None,
                            Err(send_error) => Some(send_error.into_inner()),
                        }
                    },
                };
                drop(with_addresses);
                drop(drop_after_unlock);
            },
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    return Ok(());
                }
                return Err(e)
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

    /// Creates a future which returns the underlying `UdpSocket` once all other references to it
    /// have been dropped.
    /// After calling this method, all other `SharedUdpSocket` and `WithAddress` streams that are
    /// reading from the socket will end.
    pub fn try_take(mut self) -> Result<BoxFuture<UdpSocket, Void>, WithAddress> {
        let some = unwrap!(self.some.take());
        let incoming_rx = some.incoming_rx;
        let addr = some.addr;
        let buffer = some.buffer;
        try_take(some.inner)
        .map_err(move |inner| {
            WithAddress {
                some: Some(SomeWithAddress {
                    inner,
                    incoming_rx,
                    addr,
                    buffer,
                }),
            }
        })
    }
    
    /// Get a reference to the inner `UdpSocket`
    pub fn get_ref(&self) -> &UdpSocket {
        let some = unwrap!(self.some.as_ref());
        &some.inner.socket
    }
}

impl Stream for SharedUdpSocket {
    type Item = WithAddress;
    type Error = io::Error;

    fn poll(&mut self) -> io::Result<Async<Option<WithAddress>>> {
        let some = unwrap!(self.some.as_mut());
        if unwrap!(some.inner.take_task.lock()).is_some() {
            return Ok(Async::Ready(None));
        }

        pump(&some.inner, &mut some.buffer)?;

        Ok(some.incoming_rx.poll().void_unwrap())
    }
}

impl Stream for WithAddress {
    type Item = Bytes;
    type Error = io::Error;

    fn poll(&mut self) -> io::Result<Async<Option<Bytes>>> {
        let some = unwrap!(self.some.as_mut());
        if unwrap!(some.inner.take_task.lock()).is_some() {
            return Ok(Async::Ready(None));
        }
        pump(&some.inner, &mut some.buffer)?;

        Ok(some.incoming_rx.poll().void_unwrap())
    }
}

impl Sink for WithAddress {
    type SinkItem = Bytes;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Bytes) -> io::Result<AsyncSink<Bytes>> {
        let some = unwrap!(self.some.as_mut());

        if let Async::Ready(()) = some.inner.socket.poll_write() {
            match some.inner.socket.send_to(&item, &some.addr) {
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
            let take_task = unwrap!(some.inner.take_task.lock());
            if let Some(task) = take_task.as_ref() {
                task.notify();
            }
        }
    }
}

impl Drop for WithAddress {
    fn drop(&mut self) {
        if let Some(some) = self.some.take() {
            {
                let mut with_addresses = unwrap!(some.inner.with_addresses.lock());
                let _ = with_addresses.remove(&some.addr);
            }
            let take_task = unwrap!(some.inner.take_task.lock());
            if let Some(task) = take_task.as_ref() {
                task.notify();
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::{IntoFuture, Future};

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
                                                .and_then(move |(_sock0, data, len, addr)| {
                                                    assert_eq!(addr, shared_addr);
                                                    assert_eq!(&data[..len], b"rrrr");

                                                    with_addr0
                                                    .try_take()
                                                    .into_future()
                                                    .map_err(|_| panic!())
                                                    .and_then(|f| {
                                                        f
                                                        .infallible()
                                                        .map(|_socket| ())
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

