use std::io;

pub(crate) fn clock_ns(clock: libc::clockid_t) -> io::Result<u64> {
    let mut timestamp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `timestamp` points to valid writable storage.
    if unsafe { libc::clock_gettime(clock, &mut timestamp) } != 0 {
        return Err(io::Error::last_os_error());
    }
    if timestamp.tv_sec < 0 || !(0..1_000_000_000).contains(&timestamp.tv_nsec) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "clock_gettime returned out-of-range timespec",
        ));
    }
    (timestamp.tv_sec as u64)
        .checked_mul(1_000_000_000)
        .and_then(|seconds| seconds.checked_add(timestamp.tv_nsec as u64))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "clock timestamp overflow"))
}

pub(crate) fn monotonic_ns() -> io::Result<u64> {
    clock_ns(libc::CLOCK_MONOTONIC)
}

#[cfg(test)]
mod tests {
    use super::{clock_ns, monotonic_ns};

    #[test]
    fn monotonic_and_raw_clocks_advance_and_reject_invalid_clock_id() {
        let first = monotonic_ns().unwrap();
        let second = monotonic_ns().unwrap();
        assert!(first > 0);
        assert!(second >= first);
        assert!(clock_ns(libc::CLOCK_MONOTONIC_RAW).unwrap() > 0);
        assert_eq!(clock_ns(-1).unwrap_err().raw_os_error(), Some(libc::EINVAL));
    }
}
