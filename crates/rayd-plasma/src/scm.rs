//! `SCM_RIGHTS` send/recv helpers for the UDS data plane.
//!
//! The protocol always frames messages as `[u32 LE length][body]`. When the
//! sender attaches a memfd, it goes through ancillary data on the *first*
//! `sendmsg` call carrying the length prefix; the receiver's first
//! `recvmsg` therefore reads the length plus any attached fds, and a
//! subsequent plain `read` collects the body.

use std::io::{IoSlice, IoSliceMut, Read, Write};
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;

use nix::cmsg_space;
use nix::sys::socket::{recvmsg, sendmsg, ControlMessage, ControlMessageOwned, MsgFlags};

use crate::error::PlasmaError;

const HEADER_SIZE: usize = 4;

/// Maximum number of file descriptors a single frame may carry. We never
/// attach more than one (the arena memfd) but allocate space for a few in
/// case the protocol grows.
pub(crate) const MAX_FDS_PER_FRAME: usize = 4;

/// Send `[u32 length][body]` over the stream, optionally attaching `fd` via
/// `SCM_RIGHTS` ancillary data.
pub(crate) fn send_frame(
    stream: &UnixStream,
    body: &[u8],
    fd: Option<BorrowedFd<'_>>,
) -> Result<(), PlasmaError> {
    let len = u32::try_from(body.len())
        .map_err(|_| PlasmaError::Protocol(format!("frame too large: {} bytes", body.len())))?
        .to_le_bytes();
    let header_iov = [IoSlice::new(&len)];

    let raw_fds: [RawFd; 1] = match fd {
        Some(b) => [b.as_raw_fd()],
        None => [-1],
    };
    let cmsgs: Vec<ControlMessage<'_>> = match fd {
        Some(_) => vec![ControlMessage::ScmRights(&raw_fds)],
        None => Vec::new(),
    };

    let n = sendmsg::<()>(
        stream.as_raw_fd(),
        &header_iov,
        &cmsgs,
        MsgFlags::empty(),
        None,
    )?;
    if n != HEADER_SIZE {
        return Err(PlasmaError::Protocol(format!(
            "short header send: wrote {n} bytes, expected {HEADER_SIZE}"
        )));
    }

    // Body is sent without ancillary data; SCM_RIGHTS only travels on the
    // header sendmsg.
    let mut written = 0usize;
    while written < body.len() {
        let n = (&*stream).write(&body[written..])?;
        if n == 0 {
            return Err(PlasmaError::Protocol("EOF mid-body during send".into()));
        }
        written += n;
    }
    Ok(())
}

/// Receive a length-prefixed frame, returning the body and any attached fds.
pub(crate) fn recv_frame(stream: &UnixStream) -> Result<(Vec<u8>, Vec<OwnedFd>), PlasmaError> {
    let mut header = [0u8; HEADER_SIZE];
    let mut iov = [IoSliceMut::new(&mut header)];
    let mut cmsg_buf = cmsg_space!([RawFd; MAX_FDS_PER_FRAME]);
    let msg = recvmsg::<()>(
        stream.as_raw_fd(),
        &mut iov,
        Some(&mut cmsg_buf),
        MsgFlags::empty(),
    )?;

    if msg.bytes != HEADER_SIZE {
        return Err(PlasmaError::Protocol(format!(
            "short header recv: read {} bytes, expected {HEADER_SIZE}",
            msg.bytes
        )));
    }

    let mut fds: Vec<OwnedFd> = Vec::new();
    for cmsg in msg.cmsgs()? {
        if let ControlMessageOwned::ScmRights(raw) = cmsg {
            // SAFETY: the kernel just produced these fds for us; they're
            // valid and uniquely owned by this process. Wrapping in
            // `OwnedFd` gives RAII-style cleanup.
            for r in raw {
                fds.push(unsafe { OwnedFd::from_raw_fd(r) });
            }
        }
    }

    let len = u32::from_le_bytes(header) as usize;
    if len > crate::codec::MAX_FRAME_BYTES as usize {
        return Err(PlasmaError::Protocol(format!(
            "frame too large: {len} bytes (max {})",
            crate::codec::MAX_FRAME_BYTES
        )));
    }

    let mut body = vec![0u8; len];
    let mut read_total = 0usize;
    while read_total < len {
        let n = (&*stream).read(&mut body[read_total..])?;
        if n == 0 {
            return Err(PlasmaError::Protocol("EOF mid-body during recv".into()));
        }
        read_total += n;
    }
    Ok((body, fds))
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::*;

    #[test]
    fn round_trip_no_fd() {
        let (a, b) = UnixStream::pair().expect("pair");
        send_frame(&a, b"hello", None).expect("send");
        let (body, fds) = recv_frame(&b).expect("recv");
        assert_eq!(body, b"hello");
        assert!(fds.is_empty());
    }

    #[test]
    fn round_trip_with_fd() {
        // Send a memfd we just created, then verify the receiver reads it
        // and can write through it (i.e., it's the same backing memory).
        let (a, b) = UnixStream::pair().expect("pair");
        let arena = crate::arena::Arena::create(0, 4096, "rayd_scm_test").expect("arena");

        send_frame(&a, b"payload", Some(arena.as_borrowed_fd())).expect("send");
        let (body, mut fds) = recv_frame(&b).expect("recv");
        assert_eq!(body, b"payload");
        assert_eq!(fds.len(), 1);
        let received_fd = fds.pop().unwrap();

        // Map the received fd and write a marker; the sender's mmap should
        // see it because both fds refer to the same memfd object.
        let mut recv_map = unsafe {
            memmap2::MmapOptions::new()
                .len(4096)
                .map_mut(&received_fd)
                .expect("mmap")
        };
        recv_map[0..3].copy_from_slice(b"abc");
        recv_map.flush().ok();

        // The arena's mmap (via read_copy) should reflect the write.
        let observed = arena.read_copy(0, 3);
        assert_eq!(observed, b"abc");
    }

    #[test]
    fn writes_are_observed_across_fds() {
        // Sanity check: separate `MmapMut::map_mut` calls on the *same* memfd
        // really do alias the same memory. (Ensures `MAP_SHARED` defaults.)
        let arena = crate::arena::Arena::create(1, 4096, "rayd_alias_test").expect("arena");
        let mut second = unsafe {
            memmap2::MmapOptions::new()
                .len(4096)
                .map_mut(&arena.as_borrowed_fd())
        }
        .expect("mmap");
        second[100..104].copy_from_slice(b"\x01\x02\x03\x04");
        second.flush().ok();
        assert_eq!(arena.read_copy(100, 4), vec![1, 2, 3, 4]);
    }

    #[test]
    fn unix_stream_roundtrip_baseline() {
        // Diagnostic: confirm UnixStream pair works for plain read/write.
        let (mut a, mut b) = UnixStream::pair().expect("pair");
        a.write_all(b"hi").expect("write");
        let mut buf = [0u8; 2];
        b.read_exact(&mut buf).expect("read");
        assert_eq!(&buf, b"hi");
    }
}
