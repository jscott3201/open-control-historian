use super::fault::FaultId;
use super::{FlowKind, V2StoreChild};
use crate::error::{EvidenceError, Result};
use crate::root::FoundationSummary;

pub(super) fn run_foundation(child: &V2StoreChild) -> Result<FoundationSummary> {
    super::validate_compiled_registry_bijection()?;
    let mut flow_count = 0_usize;
    for kind in [
        FlowKind::P0P7Present,
        FlowKind::P0P7Absent,
        FlowKind::Rollback,
        FlowKind::EagerOpenClean,
        FlowKind::EagerOpenConvergence,
    ] {
        let witness = super::run_flow(child, kind)?;
        if witness.kind != kind || witness.trace.is_empty() {
            return Err(EvidenceError::InvalidHarness);
        }
        flow_count = flow_count.checked_add(1).ok_or(EvidenceError::Bounds)?;
    }
    let site_executions = super::exercise_all_sites(child)?;
    let source_site_count = super::source_sites().len();
    if source_site_count != FaultId::ALL.len() {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(FoundationSummary {
        schema: super::schema::FOUNDATION_SCHEMA,
        descriptor_count: FaultId::ALL.len(),
        source_site_count,
        site_executions,
        flow_count,
        registered_g2_crash_targets: source_site_count,
    })
}

#[cfg(test)]
mod tests {
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
        let root = crate::root::EvidenceRoot::prepare(&temp.0.join("evidence"))
            .expect("prepare evidence root");
        let witness = root.run_v2_foundation().expect("run g1 foundation");
        assert_eq!(witness.schema, "m03-pr03g1-v1");
        assert_eq!(witness.descriptor_count, 173);
        assert_eq!(witness.source_site_count, 173);
        assert_eq!(witness.site_executions, 487);
        assert_eq!(witness.flow_count, 5);
        assert_eq!(witness.registered_g2_crash_targets, 173);
        assert_eq!(
            fs::read_dir(temp.0.join("evidence/cases"))
                .expect("read reclaimed cases")
                .count(),
            0
        );
    }
}
