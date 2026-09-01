use crate::error::{EvidenceError, Result};
use crate::fixture;
use och_runtime::__m03_pr03e_native_harness::{
    BoundaryId, BoundaryOutcome, EvidenceStatus, FaultAction, FaultPlan, InjectedErrorKind,
    NativeEvidenceSession,
};
use och_runtime::{
    AdmissionPriority, BarrierDemand, ByteReservationLimits, DurableOutcome, GroupCommitPolicy,
    HandledOutcome, HistorianRuntime, IngressCommand, RegistryOperation, RuntimeHealth,
    ShutdownError, StoreOptions,
};
use och_store::{
    ActiveJournalLimits, ActiveJournalOpenMode, RegistryPersistenceOptions, RetryPersistenceOptions,
};
use std::io::ErrorKind;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::time::Duration;
use tokio::runtime::Builder;

const SESSION_CAPACITY: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeWitness {
    pub(crate) handled: &'static str,
    pub(crate) durable: &'static str,
    pub(crate) health: &'static str,
    pub(crate) pressure: Option<ErrorKind>,
    pub(crate) durable_sequence: u64,
    pub(crate) boundaries: Vec<BoundaryId>,
}

pub(crate) fn success(store: &Path, rotate: bool) -> Result<RuntimeWitness> {
    let session = NativeEvidenceSession::new(SESSION_CAPACITY, FaultPlan::none())
        .map_err(|_| EvidenceError::InvalidHarness)?;
    let witness = current_thread()?.block_on(run_one(
        store,
        ActiveJournalOpenMode::CreateNew,
        Some(session.clone()),
        rotate,
        81,
    ))?;
    let snapshot = session.snapshot();
    if snapshot.status() != EvidenceStatus::Complete
        || snapshot.validate_closed_trace().is_err()
        || snapshot
            .events()
            .iter()
            .any(|event| event.outcome() != BoundaryOutcome::Success)
    {
        return Err(EvidenceError::InvalidHarness);
    }
    let boundaries = snapshot
        .events()
        .iter()
        .map(|event| event.boundary())
        .collect::<Vec<_>>();
    if !ordered_subsequence(
        &boundaries,
        &[
            BoundaryId::JournalAppendWrite,
            BoundaryId::HandledVisibility,
            BoundaryId::JournalSync,
            BoundaryId::CheckpointWrite,
            BoundaryId::CheckpointSync,
            BoundaryId::CheckpointAdopt,
            BoundaryId::RetryStatePublish,
            BoundaryId::ManifestPrepare,
            BoundaryId::ManifestRenameCommit,
            BoundaryId::ManifestPostcommit,
            BoundaryId::ManifestAdopt,
            BoundaryId::InspectionUpdate,
            BoundaryId::DurableBatchReceiptResolution,
        ],
    ) || (rotate
        && !ordered_subsequence(
            &boundaries,
            &[
                BoundaryId::DurableBatchReceiptResolution,
                BoundaryId::RotationDelay,
            ],
        ))
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(RuntimeWitness {
        boundaries,
        ..witness
    })
}

pub(crate) fn pressure(
    store: &Path,
    kind: InjectedErrorKind,
    partial: bool,
) -> Result<RuntimeWitness> {
    let action = if partial {
        FaultAction::ShortPartialWrite {
            bytes: NonZeroUsize::new(7).expect("literal nonzero partial write"),
            error: kind,
        }
    } else {
        FaultAction::PreOperationError(kind)
    };
    let plan = FaultPlan::single(BoundaryId::JournalAppendWrite, NonZeroU32::MIN, action)
        .map_err(|_| EvidenceError::InvalidHarness)?;
    let session = NativeEvidenceSession::new(SESSION_CAPACITY, plan)
        .map_err(|_| EvidenceError::InvalidHarness)?;
    let witness = current_thread()?.block_on(run_one(
        store,
        ActiveJournalOpenMode::CreateNew,
        Some(session.clone()),
        false,
        82,
    ))?;
    let expected = match kind {
        InjectedErrorKind::StorageFull => ErrorKind::StorageFull,
        InjectedErrorKind::QuotaExceeded => ErrorKind::QuotaExceeded,
        InjectedErrorKind::Other => return Err(EvidenceError::InvalidHarness),
    };
    if witness.handled != "WRITER_STOPPED"
        || witness.durable != "WRITER_STOPPED"
        || witness.health != "STORAGE_PRESSURE"
        || witness.pressure != Some(expected)
        || witness.durable_sequence != 0
    {
        return Err(EvidenceError::InvalidHarness);
    }
    let snapshot = session.snapshot();
    let boundaries = snapshot
        .events()
        .iter()
        .map(|event| event.boundary())
        .collect::<Vec<_>>();
    if snapshot.status() != EvidenceStatus::Complete
        || boundaries.last() != Some(&BoundaryId::StorePressureTransition)
    {
        return Err(EvidenceError::InvalidHarness);
    }
    current_thread()?.block_on(reopen(store, 82))?;
    Ok(RuntimeWitness {
        boundaries,
        ..witness
    })
}

async fn run_one(
    store: &Path,
    mode: ActiveJournalOpenMode,
    session: Option<NativeEvidenceSession>,
    rotate: bool,
    seed: u64,
) -> Result<RuntimeWitness> {
    let store_id = fixture::harness_store_id(seed)?;
    let options = options(store, store_id, mode, session, rotate)?;
    let runtime = HistorianRuntime::open(options)
        .await
        .map_err(|_| EvidenceError::InvalidHarness)?;
    register(&runtime, store_id, seed).await?;
    let admission = fixture::harness_admission(store_id, seed)?;
    let receipt = runtime
        .ingress()
        .try_submit(IngressCommand::with_policy(
            admission,
            AdmissionPriority::Normal,
            BarrierDemand::Immediate,
        ))
        .map_err(|_| EvidenceError::InvalidHarness)?
        .into_receipt();
    let handled = receipt.clone().wait_handled().await;
    let durable = receipt.wait_durable().await;
    let inspection = runtime.inspection();
    let shutdown = runtime.shutdown().await;
    let handled = match handled {
        HandledOutcome::WriterHandled(_) => "HANDLED",
        HandledOutcome::WriterStopped => "WRITER_STOPPED",
    };
    let durable = match durable {
        DurableOutcome::Durable(_) => "DURABLE",
        DurableOutcome::WriterStopped => "WRITER_STOPPED",
    };
    let health = match inspection.health() {
        RuntimeHealth::Healthy | RuntimeHealth::RotationRequired => "HEALTHY",
        RuntimeHealth::StoragePressure => "STORAGE_PRESSURE",
        RuntimeHealth::Faulted => "FAULTED",
        RuntimeHealth::Stopped => "STOPPED",
    };
    let pressure = inspection
        .pressure_evidence()
        .map(och_runtime::RuntimePressureEvidence::kind);
    match (pressure, shutdown) {
        (None, Ok(())) => {}
        (Some(expected), Err(ShutdownError::StoragePressure(actual)))
            if expected == actual.kind() => {}
        _ => return Err(EvidenceError::InvalidHarness),
    }
    Ok(RuntimeWitness {
        handled,
        durable,
        health,
        pressure,
        durable_sequence: inspection.committed().durable_cutoff().append_sequence(),
        boundaries: Vec::new(),
    })
}

async fn register(
    runtime: &HistorianRuntime,
    store_id: och_core::StoreId,
    seed: u64,
) -> Result<()> {
    let admission = fixture::harness_admission(store_id, seed)?;
    let declaration = admission.declaration();
    runtime
        .apply_registry(RegistryOperation::Register {
            series_id: declaration.series_id(),
            binding: declaration.binding().clone(),
            payload: declaration.payload().clone(),
            evidence: declaration.evidence().clone(),
        })
        .await
        .map_err(|_| EvidenceError::InvalidHarness)?;
    Ok(())
}

async fn reopen(store: &Path, seed: u64) -> Result<()> {
    let store_id = fixture::harness_store_id(seed)?;
    let runtime = HistorianRuntime::open(options(
        store,
        store_id,
        ActiveJournalOpenMode::OpenExisting,
        None,
        false,
    )?)
    .await
    .map_err(|_| EvidenceError::InvalidHarness)?;
    runtime
        .shutdown()
        .await
        .map_err(|_| EvidenceError::InvalidHarness)
}

fn options(
    store: &Path,
    store_id: och_core::StoreId,
    mode: ActiveJournalOpenMode,
    session: Option<NativeEvidenceSession>,
    rotate: bool,
) -> Result<StoreOptions> {
    let mut options = StoreOptions::new(
        store.to_path_buf(),
        store_id,
        mode,
        ActiveJournalLimits::new(
            och_store::MAX_ADMISSION_PAYLOAD_V1,
            64 * 1_024 * 1_024,
            if rotate { 1 } else { 4_096 },
        )
        .map_err(|_| EvidenceError::InvalidHarness)?,
        ByteReservationLimits::new(64 * 1_024 * 1_024, 0, 0)
            .map_err(|_| EvidenceError::InvalidHarness)?,
        GroupCommitPolicy::new(
            Duration::from_secs(60),
            och_runtime::MAX_OUTSTANDING_COMMANDS,
            64 * 1_024 * 1_024,
            Duration::from_secs(60),
        )
        .map_err(|_| EvidenceError::InvalidHarness)?,
        RegistryPersistenceOptions::new(och_core::SeriesRegistryLimits::new(16, 64))
            .map_err(|_| EvidenceError::InvalidHarness)?,
        RetryPersistenceOptions::new(2, 2).map_err(|_| EvidenceError::InvalidHarness)?,
    )
    .map_err(|_| EvidenceError::InvalidHarness)?;
    if let Some(session) = session {
        options = options.with_native_evidence_session(session);
    }
    Ok(options)
}

fn current_thread() -> Result<tokio::runtime::Runtime> {
    Builder::new_current_thread()
        .build()
        .map_err(|_| EvidenceError::InvalidHarness)
}

fn ordered_subsequence(actual: &[BoundaryId], expected: &[BoundaryId]) -> bool {
    let mut expected = expected.iter();
    let mut next = expected.next();
    for boundary in actual {
        if next == Some(boundary) {
            next = expected.next();
        }
    }
    next.is_none()
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
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "och-v2-harness-runtime-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir(&path).expect("create runtime store child");
            Self(path)
        }
    }

    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn real_feature_on_v1_success_preserves_handled_durable_and_rotation_order() {
        let store = Temp::new("success");
        let witness = success(&store.0, true).expect("real current-V1 success witness");
        assert_eq!(witness.handled, "HANDLED");
        assert_eq!(witness.durable, "DURABLE");
        assert_eq!(witness.durable_sequence, 1);
    }

    #[test]
    fn real_v1_pressure_first_wins_has_no_false_durability_and_reopens() {
        for (index, (kind, partial)) in [
            (InjectedErrorKind::StorageFull, false),
            (InjectedErrorKind::QuotaExceeded, true),
        ]
        .into_iter()
        .enumerate()
        {
            let store = Temp::new(&format!("pressure-{index}"));
            let witness = pressure(&store.0, kind, partial).expect("real pressure witness");
            assert_eq!(witness.durable_sequence, 0);
            assert_eq!(witness.health, "STORAGE_PRESSURE");
        }
    }
}
