pub mod clock;
pub mod dynlib;
pub mod fs;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

macro_rules! try_reserve_impl {
    ($fn_name:ident, $atomic:ty, $val:ty) => {
        pub fn $fn_name(counter: &$atomic, delta: $val, max: $val) -> Option<$val> {
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

#[cfg(test)]
mod tests {
    use super::{try_reserve_bounded, try_reserve_bounded_u64};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    #[test]
    fn bounded_reservation_enforces_ceiling_and_overflow() {
        let counter = AtomicUsize::new(8);
        assert_eq!(try_reserve_bounded(&counter, 2, 10), Some(8));
        assert_eq!(counter.load(Ordering::Acquire), 10);
        assert_eq!(try_reserve_bounded(&counter, 1, 10), None);
        assert_eq!(try_reserve_bounded(&counter, usize::MAX, usize::MAX), None);

        let counter_u64 = AtomicU64::new(4);
        assert_eq!(try_reserve_bounded_u64(&counter_u64, 6, 10), Some(4));
        assert_eq!(try_reserve_bounded_u64(&counter_u64, 1, 10), None);
    }
}
