use super::fault::FaultId;
use super::v2_io::{self, FlowKind};
use crate::error::{EvidenceError, Result};
use std::path::Path;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FoundationWitness {
    pub(crate) descriptor_count: usize,
    pub(crate) source_site_count: usize,
    pub(crate) site_executions: usize,
    pub(crate) flow_count: usize,
    pub(crate) deferred_crash_obligations: usize,
}

pub(crate) fn run_foundation(root: &Path) -> Result<FoundationWitness> {
    v2_io::validate_source_closure()?;
    let mut flow_count = 0_usize;
    for kind in [
        FlowKind::P0P7Present,
        FlowKind::P0P7Absent,
        FlowKind::Rollback,
        FlowKind::EagerOpenClean,
        FlowKind::EagerOpenConvergence,
    ] {
        let witness = v2_io::run_flow(root, kind)?;
        if witness.kind != kind || witness.trace.is_empty() {
            return Err(EvidenceError::InvalidHarness);
        }
        flow_count = flow_count.checked_add(1).ok_or(EvidenceError::Bounds)?;
    }
    let site_executions = v2_io::exercise_all_sites(root)?;
    let source_site_count = v2_io::source_sites().len();
    if source_site_count != FaultId::ALL.len() {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(FoundationWitness {
        descriptor_count: FaultId::ALL.len(),
        source_site_count,
        site_executions,
        flow_count,
        deferred_crash_obligations: source_site_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    struct Temp(PathBuf);

    impl Temp {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "och-v2-g1-foundation-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir(&path).expect("create foundation root");
            Self(path)
        }
    }

    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn complete_g1_foundation_executes_and_reclaims_every_disposable_file() {
        let temp = Temp::new();
        let witness = run_foundation(&temp.0).expect("run g1 foundation");
        assert_eq!(witness.descriptor_count, 173);
        assert_eq!(witness.source_site_count, 173);
        assert_eq!(witness.flow_count, 5);
        assert_eq!(witness.deferred_crash_obligations, 173);
        assert_eq!(
            fs::read_dir(&temp.0).expect("read reclaimed root").count(),
            0
        );
    }
}
