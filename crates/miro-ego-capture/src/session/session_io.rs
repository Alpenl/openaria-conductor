//! Non-blocking framed MJPEG stdin pipe writer (`MGOF`) for `miro-ego-encoder`
//! plus re-exported artifact verification primitives from `miro-ego-core`.

pub use miro_ego_core::artifact_verify::{
    ArtifactDescriptor, ExpectedArtifactIdentity, FileDigest, FileIdentity, SessionIoError,
    finalize_artifact, finalize_artifact_with_digest, finalize_segment_artifacts, hash_file,
    open_relative_regular, verify_device_session_artifacts,
};

const ENCODER_FRAME_MAGIC: &[u8; 4] = b"MGOF";

pub fn write_encoder_frame(fd: libc::c_int, jpeg: &[u8]) -> Result<u64, SessionIoError> {
    write_encoder_frame_nonblocking(
        fd,
        None,
        jpeg,
        std::time::Duration::from_secs(10),
        |_, _| Ok(()),
    )
}

pub fn write_encoder_frame_nonblocking<F>(
    fd: libc::c_int,
    wake_fd: Option<libc::c_int>,
    jpeg: &[u8],
    timeout: std::time::Duration,
    mut check_state: F,
) -> Result<u64, SessionIoError>
where
    F: FnMut(bool, bool) -> Result<(), SessionIoError>,
{
    if fd < 0 {
        return Err(SessionIoError::new(
            "write_failed",
            "invalid encoder pipe descriptor",
        ));
    }
    let length = u32::try_from(jpeg.len()).map_err(|_| {
        SessionIoError::new("write_failed", "encoder frame exceeds u32 length limit")
    })?;
    let mut header = [0_u8; 8];
    header[..4].copy_from_slice(ENCODER_FRAME_MAGIC);
    header[4..].copy_from_slice(&length.to_le_bytes());

    let chunks: [&[u8]; 2] = [&header, jpeg];
    let deadline = std::time::Instant::now() + timeout;
    let mut index = 0_usize;
    let mut offset = 0_usize;
    let mut total = 0_u64;

    while index < chunks.len() {
        while index < chunks.len() && chunks[index].is_empty() {
            index += 1;
            offset = 0;
        }
        if index >= chunks.len() {
            break;
        }
        check_state(total > 0, false)?;

        let mut vectors = [iovec(&[]); 2];
        let mut count = 0_usize;
        for (chunk_idx, chunk) in chunks[index..].iter().enumerate() {
            let slice = if chunk_idx == 0 {
                &chunk[offset..]
            } else {
                *chunk
            };
            if !slice.is_empty() {
                vectors[count] = iovec(slice);
                count += 1;
            }
        }

        // SAFETY: `vectors[..count]` reference live stack/slice buffers.
        let written = unsafe { libc::writev(fd, vectors.as_ptr(), count as libc::c_int) };
        if written > 0 {
            let mut remaining = written as usize;
            total += written as u64;
            while index < chunks.len() {
                if chunks[index].is_empty() {
                    index += 1;
                    offset = 0;
                    continue;
                }
                let available = chunks[index].len() - offset;
                if remaining < available {
                    offset += remaining;
                    break;
                }
                remaining -= available;
                index += 1;
                offset = 0;
                if remaining == 0 {
                    break;
                }
            }
            continue;
        }
        if written == 0 {
            return Err(SessionIoError::new(
                "write_failed",
                "writev wrote zero bytes",
            ));
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        if error.kind() == std::io::ErrorKind::WouldBlock {
            loop {
                check_state(total > 0, true)?;
                let remaining_time = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining_time.is_zero() {
                    return Err(SessionIoError::new(
                        "encoder_backpressure_timeout",
                        "timed out waiting for encoder stdin pipe",
                    ));
                }
                let slice_timeout = remaining_time.min(std::time::Duration::from_millis(50));
                let timeout_ms =
                    libc::c_int::try_from(slice_timeout.as_millis().max(1)).unwrap_or(50);
                let mut pfds = [
                    libc::pollfd {
                        fd,
                        events: libc::POLLOUT,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: wake_fd.unwrap_or(-1),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                ];
                let nfds: libc::nfds_t = if wake_fd.is_some() { 2 } else { 1 };
                // SAFETY: `pfds` points to valid stack `pollfd` entries for `nfds` items.
                let rc = unsafe { libc::poll(pfds.as_mut_ptr(), nfds, timeout_ms) };
                if rc < 0 {
                    let poll_err = std::io::Error::last_os_error();
                    if poll_err.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(SessionIoError::os(
                        "write_failed",
                        "poll encoder stdin failed",
                        poll_err,
                    ));
                }
                if let Some(wfd) = wake_fd
                    && (pfds[1].revents & libc::POLLIN) != 0
                {
                    let mut val = 0_u64;
                    // SAFETY: Draining 8-byte eventfd counter into stack `u64`.
                    let _ = unsafe { libc::read(wfd, (&raw mut val).cast::<libc::c_void>(), 8) };
                }
                if (pfds[0].revents
                    & (libc::POLLOUT | libc::POLLERR | libc::POLLHUP | libc::POLLNVAL))
                    != 0
                {
                    break;
                }
            }
            continue;
        }
        return Err(SessionIoError::os("write_failed", "writev failed", error));
    }
    Ok(total)
}

fn iovec(payload: &[u8]) -> libc::iovec {
    libc::iovec {
        iov_base: payload.as_ptr().cast::<libc::c_void>().cast_mut(),
        iov_len: payload.len(),
    }
}
