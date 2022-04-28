// socket.rs

use std::os::unix::io::RawFd;

use nix::sys::select::{select, FdSet};
use nix::sys::socket;
use nix::sys::socket::{AddressFamily, SockAddr, SockFlag, SockType, UnixAddr};
use nix::sys::time::{TimeVal, TimeValLike};

use super::{rand_in, Message, Result, StorageError, ID};

/// Unix Domain Socket
pub struct Socket {
    socket_fd: RawFd,
}

impl Socket {
    /// Create new Unix Domain Socket with `connect` syscall
    pub fn new_active_socket(socket_addr: ID) -> Result<Socket> {
        let socket_fd = socket::socket(
            AddressFamily::Unix,
            SockType::SeqPacket,
            SockFlag::empty(),
            None,
        )?;
        let addr = UnixAddr::new(socket_addr.as_str())?;
        socket::connect(socket_fd, &SockAddr::Unix(addr))?;
        Ok(Socket { socket_fd })
    }

    /// Create new Unix Domain Socket with `bind` syscall
    pub fn new_passive_socket(socket_addr: &ID) -> Result<Socket> {
        let socket_fd = socket::socket(
            AddressFamily::Unix,
            SockType::SeqPacket,
            SockFlag::empty(),
            None,
        )?;
        let addr = UnixAddr::new(socket_addr.as_str())?;
        socket::connect(socket_fd, &SockAddr::Unix(addr))?;
        Ok(Socket { socket_fd })
    }

    /// Send Message through the socket
    pub fn send_msg(&self, msg: Message) -> Result<usize> {
        let size = socket::send(self.socket_fd, &msg.to_vec()?, socket::MsgFlags::empty())?;
        Ok(size)
    }

    /// Read Message from the socket
    pub fn read_msg(&self) -> Result<Message> {
        let mut buf: [u8; 16384] = [0; 16384];
        let msize = socket::recv(self.socket_fd, &mut buf, socket::MsgFlags::empty())?;
        let msg: Message = serde_json::from_slice(&buf[..msize])?;
        Ok(msg)
    }

    /// Set given millisecond timeout to the socket
    pub fn set_timeout(&self, millis: i64) -> Result<()> {
        let timeout = TimeVal::milliseconds(millis);
        Ok(socket::setsockopt(
            self.socket_fd,
            socket::sockopt::ReceiveTimeout,
            &timeout,
        )?)
    }

    /// Set random timeout in the given millisecond range to the socket
    pub fn set_random_timeout(&self, millis_range: std::ops::Range<i64>) -> Result<()> {
        self.set_timeout(rand_in(millis_range))
    }

    /// Return Ok(_), when read available, otherwise return Err(_).
    /// `timeout` is in millis
    pub fn wait_for_read(&self, timeout: i64) -> Result<()> {
        let mut read_fdset = FdSet::new();
        read_fdset.insert(self.socket_fd);

        let mut timeout = TimeVal::milliseconds(timeout);

        match select(None, Some(&mut read_fdset), None, None, Some(&mut timeout))? {
            1 if read_fdset.contains(self.socket_fd) => Ok(()),
            0 if !read_fdset.contains(self.socket_fd) => Err(StorageError("select error".into())),
            _ => unreachable!("Unreachable arm for select"),
        }
    }

    /// Is socket ready for a read?
    pub fn ready_for_read(&self) -> bool {
        match self.wait_for_read(0) {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}
