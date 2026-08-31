use crate::error::{EvidenceError, Result};
use std::cell::Cell;

pub(crate) const RSS_TARGET_BYTES: u64 = 167_772_160;
pub(crate) const MAX_FRAMES: usize = 4_096;
pub(crate) const MAX_SERIES: usize = 4_096;
pub(crate) const MAX_OBSERVATIONS: usize = 1_048_576;
pub(crate) const MAX_FRAME_BYTES: usize = 8_388_632;
pub(crate) const SCRATCH_BYTES: usize = 128 * 1_024;
pub(crate) const FRAME_META_LEDGER_BYTES: usize = 64;
pub(crate) const OBSERVATION_WORK_BYTES: usize = 96;

pub(crate) const CONTROLLED_BASE_BYTES: u64 = 2 * MAX_FRAME_BYTES as u64
    + MAX_OBSERVATIONS as u64 * OBSERVATION_WORK_BYTES as u64
    + 2 * MAX_FRAMES as u64 * FRAME_META_LEDGER_BYTES as u64
    + SCRATCH_BYTES as u64;
pub(crate) const RSS_MARGIN_BYTES: u64 = RSS_TARGET_BYTES - CONTROLLED_BASE_BYTES;

thread_local! {
    static ACTIVE_CONTROLLED_BYTES: Cell<u64> = const { Cell::new(0) };
}

pub(crate) struct ControlledGuard {
    bytes: u64,
}

impl ControlledGuard {
    pub(crate) fn acquire(bytes: u64) -> Result<Self> {
        if bytes > CONTROLLED_BASE_BYTES {
            return Err(EvidenceError::Bounds);
        }
        ACTIVE_CONTROLLED_BYTES.with(|active| {
            let total = active
                .get()
                .checked_add(bytes)
                .ok_or(EvidenceError::Bounds)?;
            if total > CONTROLLED_BASE_BYTES {
                return Err(EvidenceError::Bounds);
            }
            active.set(total);
            Ok(())
        })?;
        Ok(Self { bytes })
    }
}

impl Drop for ControlledGuard {
    fn drop(&mut self) {
        ACTIVE_CONTROLLED_BYTES.with(|active| {
            let remaining = active
                .get()
                .checked_sub(self.bytes)
                .expect("controlled ledger guard imbalance");
            active.set(remaining);
        });
    }
}

pub(crate) fn active_controlled_bytes() -> u64 {
    ACTIVE_CONTROLLED_BYTES.with(Cell::get)
}

pub(crate) fn actual_metadata_bytes(
    append_capacity: usize,
    series_order_capacity: usize,
    observation_capacity: usize,
) -> Result<u64> {
    if append_capacity > MAX_FRAMES
        || series_order_capacity > MAX_FRAMES
        || observation_capacity > MAX_OBSERVATIONS
    {
        return Err(EvidenceError::Bounds);
    }
    let append_bytes = append_capacity
        .checked_mul(FRAME_META_LEDGER_BYTES)
        .ok_or(EvidenceError::Bounds)?;
    let series_order_bytes = series_order_capacity
        .checked_mul(FRAME_META_LEDGER_BYTES)
        .ok_or(EvidenceError::Bounds)?;
    let observation_bytes = observation_capacity
        .checked_mul(OBSERVATION_WORK_BYTES)
        .ok_or(EvidenceError::Bounds)?;
    u64::try_from(
        append_bytes
            .checked_add(series_order_bytes)
            .ok_or(EvidenceError::Bounds)?
            .checked_add(observation_bytes)
            .and_then(|bytes| bytes.checked_add(SCRATCH_BYTES))
            .ok_or(EvidenceError::Bounds)?,
    )
    .map_err(|_| EvidenceError::Bounds)
}

pub(crate) fn print_ledger() {
    println!("schema=och-v2-evidence-ledger-v1");
    println!("rss_target_bytes={RSS_TARGET_BYTES}");
    println!("external_sort_workspace_bytes=0");
    println!("frame_buffers_bytes={}", 2 * MAX_FRAME_BYTES);
    println!(
        "observation_work_bytes={}",
        MAX_OBSERVATIONS * OBSERVATION_WORK_BYTES
    );
    println!(
        "frame_metadata_bytes={}",
        2 * MAX_FRAMES * FRAME_META_LEDGER_BYTES
    );
    println!("scratch_bytes={SCRATCH_BYTES}");
    println!("controlled_base_bytes={CONTROLLED_BASE_BYTES}");
    println!("rss_margin_bytes={RSS_MARGIN_BYTES}");
    println!("controlled_current_bytes={}", active_controlled_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reviewed_resource_formula_is_exact() {
        assert_eq!(CONTROLLED_BASE_BYTES, 118_095_920);
        assert_eq!(RSS_MARGIN_BYTES, 49_676_240);
        assert_eq!(
            actual_metadata_bytes(MAX_FRAMES, MAX_FRAMES, MAX_OBSERVATIONS),
            Ok(CONTROLLED_BASE_BYTES - 2 * MAX_FRAME_BYTES as u64)
        );
        assert_eq!(
            actual_metadata_bytes(1, 2, 3),
            Ok((SCRATCH_BYTES + FRAME_META_LEDGER_BYTES * 3 + OBSERVATION_WORK_BYTES * 3) as u64)
        );
        assert_eq!(active_controlled_bytes(), 0);
    }

    #[test]
    fn nested_guards_checked_add_and_never_exceed_the_controlled_ceiling() {
        let first =
            ControlledGuard::acquire(CONTROLLED_BASE_BYTES - 1).expect("first bounded guard");
        assert_eq!(active_controlled_bytes(), CONTROLLED_BASE_BYTES - 1);
        assert!(matches!(
            ControlledGuard::acquire(2),
            Err(EvidenceError::Bounds)
        ));
        assert_eq!(active_controlled_bytes(), CONTROLLED_BASE_BYTES - 1);
        let second = ControlledGuard::acquire(1).expect("exact ceiling guard");
        assert_eq!(active_controlled_bytes(), CONTROLLED_BASE_BYTES);
        assert!(matches!(
            ControlledGuard::acquire(1),
            Err(EvidenceError::Bounds)
        ));
        drop(second);
        drop(first);
        assert_eq!(active_controlled_bytes(), 0);
    }
}
