pub(crate) mod clock;
pub(crate) mod dynlib;
pub(crate) mod fs;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

macro_rules! try_reserve_impl {
    ($fn_name:ident, $atomic:ty, $val:ty) => {
        pub(crate) fn $fn_name(counter: &$atomic, delta: $val, max: $val) -> Option<$val> {
            let mut current = counter.load(Ordering::Acquire);
            loop {
                let next = current.checked_add(delta).filter(|&total| total <= max)?;
                match counter.compare_exchange_weak(
                    current,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(prev) => return Some(prev),
                    Err(actual) => current = actual,
                }
            }
        }
    };
}

try_reserve_impl!(try_reserve_bounded, AtomicUsize, usize);
try_reserve_impl!(try_reserve_bounded_u64, AtomicU64, u64);
