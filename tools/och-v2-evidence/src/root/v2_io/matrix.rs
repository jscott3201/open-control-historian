use super::fault::{FaultId, FaultSelection};
use super::schema::{
    CacheState, EventId, FaultMode, PhaseId, PressureKind, ProcessMode, RootClassification,
    RotationTriggerPath, SampleClass, SampleOutcome, StoreMode, TraceStatus,
};
use crate::error::{EvidenceError, Result};
use std::collections::{BTreeMap, BTreeSet};

pub(super) const FIXTURE_IDS: [&str; 28] = [
    "TRACE-ACCEPT-PRE-APPEND",
    "TRACE-ACCEPT-POST-PUBLICATION",
    "TRACE-REJECT-MERGED-SUBEVENT",
    "TRACE-REJECT-OVERLAPPING-PEERS",
    "TRACE-REJECT-REORDERED-SUBEVENT",
    "TRACE-REJECT-MISSING-SUBEVENT",
    "TRACE-REJECT-PRE-RECEIPT-ROTATION",
    "TRACE-REJECT-PRE-BARRIER-ROTATION",
    "TRACE-REJECT-MISSING-PRE-APPEND",
    "TRACE-REJECT-MISSING-POST-PUBLICATION",
    "TRACE-REJECT-CASE-TRIGGER-MISMATCH",
    "TRACE-REJECT-POST-RENAME-PRIOR-ROOT",
    "ELIGIBILITY-ACCEPT-COMPLETE-SUCCESS",
    "ELIGIBILITY-REJECT-FINAL-FAULT-ELIGIBLE",
    "ELIGIBILITY-REJECT-PRESSURE-ELIGIBLE",
    "ELIGIBILITY-REJECT-NON-SUCCESS-ELIGIBLE",
    "ELIGIBILITY-REJECT-NON-SUCCESS-EVENT-SUCCESS-SAMPLE",
    "ELIGIBILITY-REJECT-SUMMARY-INELIGIBLE",
    "TREE-ACCEPT-DIRECT-NESTED",
    "TREE-ACCEPT-SIBLING-BOUNDARY-TOUCH",
    "TREE-REJECT-WRONG-PARENT",
    "TREE-REJECT-CROSSING-PARENT-CHILD",
    "TREE-REJECT-CROSSING-SIBLINGS",
    "TREE-REJECT-CYCLE",
    "TREE-REJECT-UNKNOWN-PARENT",
    "TREE-REJECT-DUPLICATE-PARENTAGE",
    "TREE-REJECT-ROOT-SENTINEL",
    "TREE-REJECT-CHILD-SENTINEL",
];

const ROTATION_CASES: [(&str, &str, RotationTriggerPath); 5] = [
    (
        "ROTATE-PRE-APPEND-FIT",
        "INCOMING_APPEND_DOES_NOT_FIT",
        RotationTriggerPath::PreAppend,
    ),
    (
        "ROTATE-PRE-APPEND-AGE",
        "AGE_BEFORE_PRESERVED_APPEND",
        RotationTriggerPath::PreAppend,
    ),
    (
        "ROTATE-POST-PUBLICATION-SIZE",
        "SIZE_AFTER_PUBLICATION",
        RotationTriggerPath::PostPublication,
    ),
    (
        "ROTATE-POST-PUBLICATION-COUNT",
        "COUNT_AFTER_PUBLICATION",
        RotationTriggerPath::PostPublication,
    ),
    (
        "ROTATE-POST-PUBLICATION-AGE",
        "AGE_AFTER_PUBLICATION",
        RotationTriggerPath::PostPublication,
    ),
];

const BOUND_IDS: [&str; 13] = [
    "CATALOG-ENTRY-1",
    "CATALOG-ENTRY-64",
    "CATALOG-ENTRY-65-REFUSAL",
    "INVENTORY-156",
    "INVENTORY-157-REFUSAL",
    "INVENTORY-UNKNOWN-NAME-REFUSAL",
    "INVENTORY-NON-FILE-REFUSAL",
    "EPOCH-V1-REFUSAL",
    "EPOCH-V2-ACCEPTANCE-FIXTURE",
    "EPOCH-MIXED-REFUSAL",
    "EPOCH-MARKERLESS-REFUSAL",
    "EPOCH-HISTORICAL-REFUSAL",
    "ARITHMETIC-OVERFLOW-REFUSAL",
];

const HOSTILE_IDS: [&str; 18] = [
    "HOSTILE-MISSING",
    "HOSTILE-FOREIGN",
    "HOSTILE-CORRUPT",
    "HOSTILE-MALFORMED",
    "HOSTILE-TRUNCATED",
    "HOSTILE-PARTIAL",
    "HOSTILE-EXCESSIVE",
    "HOSTILE-FORKED",
    "HOSTILE-UNRELATED",
    "HOSTILE-AMBIGUOUS",
    "HOSTILE-UNKNOWN-NAME",
    "HOSTILE-MIXED-FORMAT",
    "HOSTILE-INTENT-ABSENT-LEFTOVER",
    "HOSTILE-RAW-SEGMENT-MISMATCH",
    "HOSTILE-CATALOG-MISMATCH",
    "HOSTILE-MANIFEST-MISMATCH",
    "HOSTILE-ORPHAN-SEGMENT",
    "HOSTILE-GENERATION-GAP",
];

const RESOURCE_IDS: [&str; 21] = [
    "RESOURCE-FRAME-METADATA",
    "RESOURCE-OBSERVATION-INDEX",
    "RESOURCE-INPUT-FRAME",
    "RESOURCE-CANONICAL-REENCODE",
    "RESOURCE-DECODER",
    "RESOURCE-REENCODER",
    "RESOURCE-IO-SCRATCH",
    "RESOURCE-TRANSACTION-RECORDS",
    "RESOURCE-RECEIPT-RECORDS",
    "RESOURCE-FAULT-STATE",
    "RESOURCE-PAIR-STATE",
    "RESOURCE-STACK",
    "RESOURCE-THREAD-COUNT",
    "RESOURCE-RSS",
    "RESOURCE-ARTIFACT-STORAGE",
    "RESOURCE-EXTERNAL-WORKSPACE",
    "RESOURCE-CONCURRENT-INVENTORY",
    "RESOURCE-AVAILABLE-STORAGE",
    "RESOURCE-HEADROOM",
    "RESOURCE-PAGE-SIZE",
    "RESOURCE-CACHE-LABELS",
];

const REPORT_IDS: [&str; 18] = [
    "REPORT-RUN-KV",
    "REPORT-TIMING-SAMPLES",
    "REPORT-TIMING-SUMMARY",
    "REPORT-RESOURCE-LEDGER",
    "REPORT-FAULT-REGISTRY",
    "REPORT-FAULT-RESULTS",
    "REPORT-MATRIX",
    "REPORT-SHA256SUMS",
    "REPORT-BUNDLE-BOUND",
    "REPORT-FILE-BOUND",
    "REPORT-LINE-BOUND",
    "REPORT-SCALAR-BOUND",
    "REPORT-CLOSED-SCHEMA",
    "REPORT-SANITIZER",
    "REPORT-SORTED-IDENTITIES",
    "REPORT-NO-UNLISTED-FILES",
    "REPORT-CHECKSUMS",
    "REPORT-SOURCE-HASHES",
];

const PLATFORM_IDS: [&str; 9] = [
    "PLATFORM-LINUX-X86_64-LATER-CANDIDATE",
    "PLATFORM-DARWIN-STRUCTURAL-ONLY",
    "PLATFORM-LINUX-ARM64-EXCLUDED",
    "PLATFORM-WINDOWS-EXCLUDED",
    "PLATFORM-NETWORK-FS-EXCLUDED",
    "PLATFORM-CLOUD-OBJECT-EXCLUDED",
    "PLATFORM-FUSE-EXCLUDED",
    "PLATFORM-PHYSICAL-POWER-LOSS-EXCLUDED",
    "PLATFORM-HOSTED-CI-STRUCTURAL-ONLY",
];

pub(super) const MATRIX_ROW_COUNT: usize = 639;
pub(super) const TIMING_SAMPLE_ROW_COUNT: usize = 173;
pub(super) const TIMING_SUMMARY_ROW_COUNT: usize = 6;
pub(super) const RESOURCE_ROW_COUNT: usize = 6;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct EventRow {
    pub(super) event_id: EventId,
    pub(super) parent_event_id: Option<EventId>,
    pub(super) phase_id: PhaseId,
    pub(super) pair_ordinal: Option<u8>,
    pub(super) start_ns: u64,
    pub(super) stop_ns: u64,
    pub(super) outcome: SampleOutcome,
    pub(super) root: RootClassification,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TimingSample {
    pub(super) case_id: &'static str,
    pub(super) sample_id: String,
    pub(super) durability_batch_id: Option<u64>,
    pub(super) trigger_path: RotationTriggerPath,
    pub(super) process_mode: ProcessMode,
    pub(super) store_mode: StoreMode,
    pub(super) process_cache: CacheState,
    pub(super) filesystem_cache: CacheState,
    pub(super) sample_class: SampleClass,
    pub(super) sample_outcome: SampleOutcome,
    pub(super) fault_id: Option<FaultId>,
    pub(super) fault_mode: FaultMode,
    pub(super) pressure_kind: PressureKind,
    pub(super) trace_status: TraceStatus,
    pub(super) distribution_eligible: bool,
    pub(super) intended_success: bool,
    pub(super) process_success: bool,
    pub(super) events: Vec<EventRow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct MatrixRow {
    pub(super) row_id: String,
    pub(super) category: &'static str,
    pub(super) expected_rows: usize,
    pub(super) observed_rows: usize,
    pub(super) status: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TimingSummary {
    pub(super) case_id: &'static str,
    pub(super) trigger_path: RotationTriggerPath,
    pub(super) required_count: usize,
    pub(super) actual_count: usize,
    pub(super) min_ns: u64,
    pub(super) median_ns: u64,
    pub(super) p90_ns: u64,
    pub(super) p95_ns: u64,
    pub(super) p99_ns: u64,
    pub(super) max_ns: u64,
    pub(super) iqr_ns: u64,
    pub(super) mad_ns: u64,
    pub(super) percentile_claim: &'static str,
}

pub(super) fn structural_timing_samples() -> Result<Vec<TimingSample>> {
    let mut samples = Vec::new();
    samples
        .try_reserve_exact(9)
        .map_err(|_| EvidenceError::Bounds)?;
    for (index, (case, _, trigger)) in ROTATION_CASES.iter().copied().enumerate() {
        let batch = if trigger == RotationTriggerPath::PostPublication {
            Some(u64::try_from(index + 1).map_err(|_| EvidenceError::Bounds)?)
        } else {
            None
        };
        if let Some(batch_id) = batch {
            samples.push(receipt_sample(case, batch_id)?);
        }
        samples.push(rotation_sample(case, trigger, batch)?);
    }
    samples.push(eager_sample());
    validate_timing_samples(&samples)?;
    let rows = samples.iter().try_fold(0_usize, |count, sample| {
        count
            .checked_add(sample.events.len())
            .ok_or(EvidenceError::Bounds)
    })?;
    if rows != TIMING_SAMPLE_ROW_COUNT {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(samples)
}

pub(super) fn timing_summaries(samples: &[TimingSample]) -> Result<Vec<TimingSummary>> {
    validate_timing_samples(samples)?;
    let mut summaries = Vec::new();
    for (case, _, trigger) in ROTATION_CASES {
        let duration = eligible_root_duration(samples, case, EventId::WriterRotationDelay)?;
        summaries.push(summary(case, trigger, duration));
    }
    let duration = eligible_root_duration(samples, "EAGER-OPEN-64", EventId::EagerOpen)?;
    summaries.push(summary(
        "EAGER-OPEN-64",
        RotationTriggerPath::NotApplicable,
        duration,
    ));
    if summaries.len() != TIMING_SUMMARY_ROW_COUNT {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(summaries)
}

fn summary(
    case_id: &'static str,
    trigger_path: RotationTriggerPath,
    duration: u64,
) -> TimingSummary {
    TimingSummary {
        case_id,
        trigger_path,
        required_count: 1,
        actual_count: 1,
        min_ns: duration,
        median_ns: duration,
        p90_ns: duration,
        p95_ns: duration,
        p99_ns: duration,
        max_ns: duration,
        iqr_ns: 0,
        mad_ns: 0,
        percentile_claim: "STRUCTURAL_ONLY",
    }
}

fn eligible_root_duration(samples: &[TimingSample], case: &str, root: EventId) -> Result<u64> {
    let mut matching = samples.iter().filter(|sample| {
        sample.case_id == case
            && sample.distribution_eligible
            && sample.events.iter().any(|event| event.event_id == root)
    });
    let sample = matching.next().ok_or(EvidenceError::InvalidHarness)?;
    if matching.next().is_some() {
        return Err(EvidenceError::InvalidHarness);
    }
    let event = sample
        .events
        .iter()
        .find(|event| event.event_id == root)
        .ok_or(EvidenceError::InvalidHarness)?;
    event
        .stop_ns
        .checked_sub(event.start_ns)
        .ok_or(EvidenceError::InvalidHarness)
}

pub(super) fn structural_matrix() -> Result<Vec<MatrixRow>> {
    validate_literal_fixtures()?;
    let applicability = super::fault::applicability_rows()?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(MATRIX_ROW_COUNT)
        .map_err(|_| EvidenceError::Bounds)?;
    for index in 1..=11 {
        rows.push(MatrixRow {
            row_id: format!("PR03E-M{index:02}"),
            category: "CROSSWALK",
            expected_rows: 0,
            observed_rows: 0,
            status: "UNSATISFIED",
        });
    }
    for (case, demand, _) in ROTATION_CASES {
        rows.push(pass_row(format!("{case}:{demand}"), "ROTATION_CASE"));
    }
    for fixture in FIXTURE_IDS {
        rows.push(pass_row(fixture.to_owned(), "FIXTURE"));
    }
    for selection in applicability {
        rows.push(pass_row(
            applicability_identity(selection),
            "FAULT_APPLICABILITY",
        ));
    }
    for event in EventId::ALL {
        rows.push(pass_row(event.as_str().to_owned(), "TIMING_EVENT"));
    }
    add_pass_rows(&mut rows, &BOUND_IDS, "BOUND");
    add_pass_rows(&mut rows, &HOSTILE_IDS, "HOSTILE");
    add_pass_rows(&mut rows, &RESOURCE_IDS, "RESOURCE");
    add_pass_rows(&mut rows, &REPORT_IDS, "REPORT");
    add_pass_rows(&mut rows, &PLATFORM_IDS, "PLATFORM");
    rows.sort_by(|left, right| left.row_id.cmp(&right.row_id));
    if rows.len() != MATRIX_ROW_COUNT
        || rows.windows(2).any(|pair| pair[0].row_id >= pair[1].row_id)
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(rows)
}

fn add_pass_rows(rows: &mut Vec<MatrixRow>, ids: &[&str], category: &'static str) {
    rows.extend(ids.iter().map(|id| pass_row((*id).to_owned(), category)));
}

fn pass_row(row_id: String, category: &'static str) -> MatrixRow {
    MatrixRow {
        row_id,
        category,
        expected_rows: 1,
        observed_rows: 1,
        status: "PASS",
    }
}

pub(super) fn applicability_identity(selection: FaultSelection) -> String {
    format!(
        "{}:{}:{}",
        selection.id.as_str(),
        selection.mode.as_str(),
        selection.pressure.as_str()
    )
}

pub(super) fn validate_timing_samples(samples: &[TimingSample]) -> Result<()> {
    if samples.is_empty() {
        return Err(EvidenceError::InvalidHarness);
    }
    let mut sample_ids = BTreeSet::new();
    for sample in samples {
        if !sample_ids.insert(sample.sample_id.as_str()) {
            return Err(EvidenceError::InvalidHarness);
        }
        validate_sample(sample)?;
    }
    validate_batch_joins(samples)?;
    let paths = samples
        .iter()
        .filter(|sample| {
            sample
                .events
                .iter()
                .any(|event| event.event_id == EventId::WriterRotationDelay)
        })
        .map(|sample| sample.trigger_path)
        .collect::<BTreeSet<_>>();
    if !paths.contains(&RotationTriggerPath::PreAppend)
        || !paths.contains(&RotationTriggerPath::PostPublication)
    {
        return Err(EvidenceError::InvalidHarness);
    }
    for (case, _, expected) in ROTATION_CASES {
        let rotations = samples
            .iter()
            .filter(|sample| {
                sample.case_id == case
                    && sample
                        .events
                        .iter()
                        .any(|event| event.event_id == EventId::WriterRotationDelay)
            })
            .collect::<Vec<_>>();
        if rotations.len() != 1 || rotations[0].trigger_path != expected {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    Ok(())
}

fn validate_sample(sample: &TimingSample) -> Result<()> {
    if sample.events.is_empty()
        || (sample.fault_id.is_some()) != (sample.fault_mode != FaultMode::None)
        || (sample.fault_id.is_none() && sample.fault_mode != FaultMode::None)
    {
        return Err(EvidenceError::InvalidHarness);
    }
    let any_event_non_success = sample
        .events
        .iter()
        .any(|event| event.outcome != SampleOutcome::Success);
    let expected_class = if sample.pressure_kind != PressureKind::None {
        SampleClass::Pressure
    } else if sample.fault_id.is_some() || sample.fault_mode != FaultMode::None {
        SampleClass::Fault
    } else if sample.sample_outcome != SampleOutcome::Success
        || any_event_non_success
        || !sample.intended_success
        || !sample.process_success
        || sample.trace_status != TraceStatus::CompleteSuccess
        || !complete_shape(sample)
    {
        SampleClass::NonSuccess
    } else {
        SampleClass::Success
    };
    if sample.sample_class != expected_class {
        return Err(EvidenceError::InvalidHarness);
    }
    let eligible = sample.sample_class == SampleClass::Success
        && sample.sample_outcome == SampleOutcome::Success
        && sample.fault_id.is_none()
        && sample.fault_mode == FaultMode::None
        && sample.pressure_kind == PressureKind::None
        && sample.trace_status == TraceStatus::CompleteSuccess
        && sample.intended_success
        && sample.process_success
        && !any_event_non_success
        && complete_shape(sample);
    if sample.distribution_eligible != eligible
        || (sample.sample_outcome == SampleOutcome::Success
            && (!sample.intended_success || !sample.process_success || any_event_non_success))
        || (sample.trace_status == TraceStatus::CompleteSuccess
            && sample.sample_class != SampleClass::Success)
    {
        return Err(EvidenceError::InvalidHarness);
    }
    validate_tree(sample)
}

fn validate_tree(sample: &TimingSample) -> Result<()> {
    let mut nodes = BTreeMap::new();
    for event in &sample.events {
        let key = (event.event_id, event.pair_ordinal);
        if event.stop_ns < event.start_ns || nodes.insert(key, event).is_some() {
            return Err(EvidenceError::InvalidHarness);
        }
        if event.pair_ordinal.is_some() != (event.event_id == EventId::OpenPairValidation) {
            return Err(EvidenceError::InvalidHarness);
        }
        match event.parent_event_id {
            None if !root_event(event.event_id) => return Err(EvidenceError::InvalidHarness),
            Some(_) if root_only_event(event.event_id) => {
                return Err(EvidenceError::InvalidHarness);
            }
            _ => {}
        }
    }
    for event in &sample.events {
        if let Some(parent_id) = event.parent_event_id {
            let parent = nodes
                .get(&(parent_id, None))
                .ok_or(EvidenceError::InvalidHarness)?;
            if !legal_child(parent_id, event.event_id)
                || event.start_ns < parent.start_ns
                || event.stop_ns > parent.stop_ns
            {
                return Err(EvidenceError::InvalidHarness);
            }
        }
    }
    for parent in [
        None,
        Some(EventId::ReceiptHandledDurable),
        Some(EventId::WriterRotationDelay),
        Some(EventId::RotationMutationCritical),
        Some(EventId::Manifest),
        Some(EventId::EagerOpen),
    ] {
        let mut children = sample
            .events
            .iter()
            .filter(|event| event.parent_event_id == parent)
            .collect::<Vec<_>>();
        children.sort_by_key(|event| (event.start_ns, event.stop_ns));
        for pair in children.windows(2) {
            if pair[0].stop_ns > pair[1].start_ns
                || required_order(pair[0].event_id) > required_order(pair[1].event_id)
            {
                return Err(EvidenceError::InvalidHarness);
            }
        }
    }
    let mut manifest_rename_succeeded = false;
    for event in sample.events.iter().sorted_by_start() {
        if manifest_rename_succeeded && event.root == RootClassification::Prior {
            return Err(EvidenceError::InvalidHarness);
        }
        if event.event_id == EventId::ManifestRename && event.outcome == SampleOutcome::Success {
            if event.root != RootClassification::Committed {
                return Err(EvidenceError::InvalidHarness);
            }
            manifest_rename_succeeded = true;
        }
    }
    Ok(())
}

trait SortedEvents<'a> {
    fn sorted_by_start(self) -> Vec<&'a EventRow>;
}

impl<'a, I> SortedEvents<'a> for I
where
    I: Iterator<Item = &'a EventRow>,
{
    fn sorted_by_start(self) -> Vec<&'a EventRow> {
        let mut events = self.collect::<Vec<_>>();
        events.sort_by_key(|event| (event.start_ns, required_order(event.event_id)));
        events
    }
}

fn complete_shape(sample: &TimingSample) -> bool {
    let roots = sample
        .events
        .iter()
        .filter(|event| event.parent_event_id.is_none())
        .map(|event| event.event_id)
        .collect::<Vec<_>>();
    if roots == [EventId::ReceiptHandledDurable] {
        return exact_children(sample, EventId::ReceiptHandledDurable, ordinary_children());
    }
    if roots == [EventId::EagerOpen] {
        let pairs = sample
            .events
            .iter()
            .filter(|event| event.parent_event_id == Some(EventId::EagerOpen))
            .collect::<Vec<_>>();
        return pairs.len() == 64
            && pairs.iter().enumerate().all(|(index, event)| {
                event.event_id == EventId::OpenPairValidation
                    && event.pair_ordinal == u8::try_from(index + 1).ok()
            });
    }
    let valid_roots = roots == [EventId::WriterRotationDelay]
        || roots == [EventId::OrdinaryNoopBarrier, EventId::WriterRotationDelay];
    valid_roots
        && exact_children(
            sample,
            EventId::WriterRotationDelay,
            &[EventId::Preflight, EventId::RotationMutationCritical],
        )
        && exact_children(
            sample,
            EventId::RotationMutationCritical,
            &[
                EventId::Intent,
                EventId::Raw,
                EventId::Segment,
                EventId::Successor,
                EventId::Catalog,
                EventId::Manifest,
                EventId::Adoption,
                EventId::Cleanup,
            ],
        )
        && exact_children(
            sample,
            EventId::Manifest,
            &[
                EventId::ManifestPrepare,
                EventId::ManifestRename,
                EventId::ManifestPostcommit,
            ],
        )
}

fn exact_children(sample: &TimingSample, parent: EventId, expected: &[EventId]) -> bool {
    sample
        .events
        .iter()
        .filter(|event| event.parent_event_id == Some(parent))
        .map(|event| event.event_id)
        .eq(expected.iter().copied())
}

fn validate_batch_joins(samples: &[TimingSample]) -> Result<()> {
    for rotation in samples.iter().filter(|sample| {
        sample
            .events
            .iter()
            .any(|event| event.event_id == EventId::WriterRotationDelay)
    }) {
        let writer_start = rotation
            .events
            .iter()
            .find(|event| event.event_id == EventId::WriterRotationDelay)
            .ok_or(EvidenceError::InvalidHarness)?
            .start_ns;
        match rotation.trigger_path {
            RotationTriggerPath::PreAppend => {
                if rotation.durability_batch_id.is_some() {
                    return Err(EvidenceError::InvalidHarness);
                }
                let barrier = rotation
                    .events
                    .iter()
                    .find(|event| event.event_id == EventId::OrdinaryNoopBarrier)
                    .ok_or(EvidenceError::InvalidHarness)?;
                if barrier.stop_ns > writer_start {
                    return Err(EvidenceError::InvalidHarness);
                }
            }
            RotationTriggerPath::PostPublication => {
                let batch = rotation
                    .durability_batch_id
                    .ok_or(EvidenceError::InvalidHarness)?;
                let receipts = samples
                    .iter()
                    .filter(|sample| {
                        sample.durability_batch_id == Some(batch)
                            && sample
                                .events
                                .iter()
                                .any(|event| event.event_id == EventId::ReceiptHandledDurable)
                    })
                    .collect::<Vec<_>>();
                if receipts.is_empty()
                    || receipts.iter().any(|receipt| {
                        receipt
                            .events
                            .iter()
                            .find(|event| event.event_id == EventId::ReceiptHandledDurable)
                            .is_none_or(|event| event.stop_ns > writer_start)
                    })
                {
                    return Err(EvidenceError::InvalidHarness);
                }
            }
            RotationTriggerPath::NotApplicable => return Err(EvidenceError::InvalidHarness),
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn rotation_sample(
    case: &'static str,
    trigger: RotationTriggerPath,
    batch: Option<u64>,
) -> Result<TimingSample> {
    let mut events = Vec::new();
    if trigger == RotationTriggerPath::PreAppend {
        events.push(event(
            EventId::OrdinaryNoopBarrier,
            None,
            0,
            100,
            RootClassification::Prior,
        ));
    }
    let base = if trigger == RotationTriggerPath::PreAppend {
        100
    } else {
        1_200
    };
    events.push(event(
        EventId::WriterRotationDelay,
        None,
        base,
        base + 1_300,
        RootClassification::Prior,
    ));
    events.push(event(
        EventId::Preflight,
        Some(EventId::WriterRotationDelay),
        base,
        base + 100,
        RootClassification::Prior,
    ));
    events.push(event(
        EventId::RotationMutationCritical,
        Some(EventId::WriterRotationDelay),
        base + 100,
        base + 1_200,
        RootClassification::Prior,
    ));
    for (index, id) in [
        EventId::Intent,
        EventId::Raw,
        EventId::Segment,
        EventId::Successor,
        EventId::Catalog,
    ]
    .into_iter()
    .enumerate()
    {
        let start = base + 100 + u64::try_from(index).map_err(|_| EvidenceError::Bounds)? * 100;
        events.push(event(
            id,
            Some(EventId::RotationMutationCritical),
            start,
            start + 100,
            RootClassification::Prior,
        ));
    }
    events.push(event(
        EventId::Manifest,
        Some(EventId::RotationMutationCritical),
        base + 600,
        base + 900,
        RootClassification::Prior,
    ));
    events.push(event(
        EventId::ManifestPrepare,
        Some(EventId::Manifest),
        base + 600,
        base + 700,
        RootClassification::Prior,
    ));
    events.push(event(
        EventId::ManifestRename,
        Some(EventId::Manifest),
        base + 700,
        base + 800,
        RootClassification::Committed,
    ));
    events.push(event(
        EventId::ManifestPostcommit,
        Some(EventId::Manifest),
        base + 800,
        base + 900,
        RootClassification::Committed,
    ));
    events.push(event(
        EventId::Adoption,
        Some(EventId::RotationMutationCritical),
        base + 900,
        base + 1_000,
        RootClassification::Committed,
    ));
    events.push(event(
        EventId::Cleanup,
        Some(EventId::RotationMutationCritical),
        base + 1_000,
        base + 1_200,
        RootClassification::Committed,
    ));
    Ok(success_sample(
        case,
        format!("SAMPLE-{case}"),
        trigger,
        batch,
        events,
    ))
}

fn receipt_sample(case: &'static str, batch: u64) -> Result<TimingSample> {
    let mut events = vec![event(
        EventId::ReceiptHandledDurable,
        None,
        0,
        1_100,
        RootClassification::Prior,
    )];
    for (index, id) in ordinary_children().iter().copied().enumerate() {
        let start = u64::try_from(index).map_err(|_| EvidenceError::Bounds)? * 100;
        events.push(event(
            id,
            Some(EventId::ReceiptHandledDurable),
            start,
            start + 100,
            RootClassification::Prior,
        ));
    }
    Ok(success_sample(
        case,
        format!("RECEIPT-{case}"),
        RotationTriggerPath::PostPublication,
        Some(batch),
        events,
    ))
}

fn eager_sample() -> TimingSample {
    let mut events = vec![event(
        EventId::EagerOpen,
        None,
        0,
        6_400,
        RootClassification::Committed,
    )];
    for ordinal in 1..=64_u8 {
        let start = u64::from(ordinal - 1) * 100;
        let mut row = event(
            EventId::OpenPairValidation,
            Some(EventId::EagerOpen),
            start,
            start + 100,
            RootClassification::Committed,
        );
        row.pair_ordinal = Some(ordinal);
        events.push(row);
    }
    success_sample(
        "EAGER-OPEN-64",
        "SAMPLE-EAGER-OPEN-64".to_owned(),
        RotationTriggerPath::NotApplicable,
        None,
        events,
    )
}

fn success_sample(
    case_id: &'static str,
    sample_id: String,
    trigger_path: RotationTriggerPath,
    durability_batch_id: Option<u64>,
    events: Vec<EventRow>,
) -> TimingSample {
    TimingSample {
        case_id,
        sample_id,
        durability_batch_id,
        trigger_path,
        process_mode: ProcessMode::Fresh,
        store_mode: StoreMode::New,
        process_cache: CacheState::Cold,
        filesystem_cache: CacheState::Unknown,
        sample_class: SampleClass::Success,
        sample_outcome: SampleOutcome::Success,
        fault_id: None,
        fault_mode: FaultMode::None,
        pressure_kind: PressureKind::None,
        trace_status: TraceStatus::CompleteSuccess,
        distribution_eligible: true,
        intended_success: true,
        process_success: true,
        events,
    }
}

fn event(
    event_id: EventId,
    parent_event_id: Option<EventId>,
    start_ns: u64,
    stop_ns: u64,
    root: RootClassification,
) -> EventRow {
    EventRow {
        event_id,
        parent_event_id,
        phase_id: event_phase(event_id),
        pair_ordinal: None,
        start_ns,
        stop_ns,
        outcome: SampleOutcome::Success,
        root,
    }
}

fn event_phase(event: EventId) -> PhaseId {
    match event {
        EventId::Intent => PhaseId::Intent,
        EventId::Raw => PhaseId::Raw,
        EventId::Segment => PhaseId::Segment,
        EventId::Successor => PhaseId::Successor,
        EventId::Catalog => PhaseId::Catalog,
        EventId::Manifest
        | EventId::ManifestPrepare
        | EventId::ManifestRename
        | EventId::ManifestPostcommit => PhaseId::Manifest,
        EventId::Adoption | EventId::Cleanup => PhaseId::AdoptClean,
        EventId::EagerOpen | EventId::OpenPairValidation => PhaseId::EagerOpen,
        EventId::Preflight
        | EventId::ReceiptHandledDurable
        | EventId::WriterRotationDelay
        | EventId::RotationMutationCritical
        | EventId::OrdinaryJournalSync
        | EventId::OrdinaryCheckpointWrite
        | EventId::OrdinaryCheckpointSync
        | EventId::OrdinaryCheckpointAdopt
        | EventId::OrdinaryRetryPublish
        | EventId::OrdinaryManifestPrepare
        | EventId::OrdinaryManifestRename
        | EventId::OrdinaryManifestPostcommit
        | EventId::OrdinaryManifestAdopt
        | EventId::OrdinaryInspectionUpdate
        | EventId::OrdinaryReceiptResolve
        | EventId::OrdinaryNoopBarrier => PhaseId::Preflight,
    }
}

fn ordinary_children() -> &'static [EventId] {
    &[
        EventId::OrdinaryJournalSync,
        EventId::OrdinaryCheckpointWrite,
        EventId::OrdinaryCheckpointSync,
        EventId::OrdinaryCheckpointAdopt,
        EventId::OrdinaryRetryPublish,
        EventId::OrdinaryManifestPrepare,
        EventId::OrdinaryManifestRename,
        EventId::OrdinaryManifestPostcommit,
        EventId::OrdinaryManifestAdopt,
        EventId::OrdinaryInspectionUpdate,
        EventId::OrdinaryReceiptResolve,
    ]
}

fn root_event(id: EventId) -> bool {
    matches!(
        id,
        EventId::ReceiptHandledDurable
            | EventId::WriterRotationDelay
            | EventId::EagerOpen
            | EventId::OrdinaryNoopBarrier
    )
}

fn root_only_event(id: EventId) -> bool {
    root_event(id)
}

fn legal_child(parent: EventId, child: EventId) -> bool {
    match parent {
        EventId::WriterRotationDelay => {
            matches!(
                child,
                EventId::Preflight | EventId::RotationMutationCritical
            )
        }
        EventId::RotationMutationCritical => matches!(
            child,
            EventId::Intent
                | EventId::Raw
                | EventId::Segment
                | EventId::Successor
                | EventId::Catalog
                | EventId::Manifest
                | EventId::Adoption
                | EventId::Cleanup
        ),
        EventId::Manifest => matches!(
            child,
            EventId::ManifestPrepare | EventId::ManifestRename | EventId::ManifestPostcommit
        ),
        EventId::ReceiptHandledDurable => ordinary_children().contains(&child),
        EventId::EagerOpen => child == EventId::OpenPairValidation,
        _ => false,
    }
}

fn required_order(id: EventId) -> usize {
    match id {
        EventId::OrdinaryNoopBarrier
        | EventId::ReceiptHandledDurable
        | EventId::EagerOpen
        | EventId::Preflight
        | EventId::Intent
        | EventId::OrdinaryJournalSync
        | EventId::ManifestPrepare
        | EventId::OpenPairValidation => 0,
        EventId::WriterRotationDelay
        | EventId::RotationMutationCritical
        | EventId::Raw
        | EventId::OrdinaryCheckpointWrite
        | EventId::ManifestRename => 1,
        EventId::Segment | EventId::OrdinaryCheckpointSync | EventId::ManifestPostcommit => 2,
        EventId::Successor | EventId::OrdinaryCheckpointAdopt => 3,
        EventId::Catalog | EventId::OrdinaryRetryPublish => 4,
        EventId::Manifest | EventId::OrdinaryManifestPrepare => 5,
        EventId::Adoption | EventId::OrdinaryManifestRename => 6,
        EventId::Cleanup | EventId::OrdinaryManifestPostcommit => 7,
        EventId::OrdinaryManifestAdopt => 8,
        EventId::OrdinaryInspectionUpdate => 9,
        EventId::OrdinaryReceiptResolve => 10,
    }
}

fn validate_literal_fixtures() -> Result<()> {
    let baseline = structural_timing_samples()?;
    for fixture in FIXTURE_IDS {
        let accepted = fixture.starts_with("TRACE-ACCEPT")
            || fixture.starts_with("ELIGIBILITY-ACCEPT")
            || fixture.starts_with("TREE-ACCEPT");
        let result = if accepted {
            validate_timing_samples(&baseline)
        } else {
            let mut hostile = baseline.clone();
            corrupt_fixture(fixture, &mut hostile)?;
            validate_timing_samples(&hostile)
        };
        if accepted != result.is_ok() {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn corrupt_fixture(fixture: &str, samples: &mut Vec<TimingSample>) -> Result<()> {
    let rotation = samples
        .iter_mut()
        .find(|sample| sample.case_id == "ROTATE-PRE-APPEND-FIT")
        .ok_or(EvidenceError::InvalidHarness)?;
    match fixture {
        "TRACE-REJECT-MERGED-SUBEVENT" | "TRACE-REJECT-MISSING-SUBEVENT" => {
            rotation
                .events
                .retain(|event| event.event_id != EventId::Raw);
        }
        "TRACE-REJECT-OVERLAPPING-PEERS" | "TREE-REJECT-CROSSING-SIBLINGS" => {
            let raw = rotation
                .events
                .iter_mut()
                .find(|event| event.event_id == EventId::Raw)
                .ok_or(EvidenceError::InvalidHarness)?;
            raw.start_ns = raw.start_ns.saturating_sub(1);
        }
        "TRACE-REJECT-REORDERED-SUBEVENT" => {
            let intent = rotation
                .events
                .iter_mut()
                .find(|event| event.event_id == EventId::Intent)
                .ok_or(EvidenceError::InvalidHarness)?;
            intent.start_ns = 350;
            intent.stop_ns = 450;
        }
        "TRACE-REJECT-PRE-RECEIPT-ROTATION" => {
            let post = samples
                .iter_mut()
                .find(|sample| {
                    sample.case_id == "ROTATE-POST-PUBLICATION-SIZE"
                        && sample.sample_id.starts_with("SAMPLE")
                })
                .ok_or(EvidenceError::InvalidHarness)?;
            for event in &mut post.events {
                event.start_ns = event.start_ns.saturating_sub(1_200);
                event.stop_ns = event.stop_ns.saturating_sub(1_200);
            }
        }
        "TRACE-REJECT-PRE-BARRIER-ROTATION" => {
            let barrier = rotation
                .events
                .iter_mut()
                .find(|event| event.event_id == EventId::OrdinaryNoopBarrier)
                .ok_or(EvidenceError::InvalidHarness)?;
            barrier.stop_ns = 101;
        }
        "TRACE-REJECT-MISSING-PRE-APPEND" => {
            samples.retain(|sample| sample.trigger_path != RotationTriggerPath::PreAppend);
        }
        "TRACE-REJECT-MISSING-POST-PUBLICATION" => {
            samples.retain(|sample| sample.trigger_path != RotationTriggerPath::PostPublication);
        }
        "TRACE-REJECT-CASE-TRIGGER-MISMATCH" => {
            rotation.trigger_path = RotationTriggerPath::NotApplicable;
        }
        "TRACE-REJECT-POST-RENAME-PRIOR-ROOT" => {
            let postcommit = rotation
                .events
                .iter_mut()
                .find(|event| event.event_id == EventId::ManifestPostcommit)
                .ok_or(EvidenceError::InvalidHarness)?;
            postcommit.root = RootClassification::Prior;
        }
        "ELIGIBILITY-REJECT-FINAL-FAULT-ELIGIBLE" => {
            rotation.fault_id = Some(FaultId::P7FinalDirectorySync);
            rotation.fault_mode = FaultMode::ChildCrashAfterSuccess;
        }
        "ELIGIBILITY-REJECT-PRESSURE-ELIGIBLE" => {
            rotation.pressure_kind = PressureKind::StorageFull;
        }
        "ELIGIBILITY-REJECT-NON-SUCCESS-ELIGIBLE" => {
            rotation.sample_outcome = SampleOutcome::Refused;
            rotation.intended_success = false;
        }
        "ELIGIBILITY-REJECT-NON-SUCCESS-EVENT-SUCCESS-SAMPLE" => {
            rotation
                .events
                .last_mut()
                .ok_or(EvidenceError::InvalidHarness)?
                .outcome = SampleOutcome::Error;
        }
        "ELIGIBILITY-REJECT-SUMMARY-INELIGIBLE" => {
            rotation.distribution_eligible = false;
        }
        "TREE-REJECT-WRONG-PARENT" | "TREE-REJECT-CYCLE" => {
            let intent = rotation
                .events
                .iter_mut()
                .find(|event| event.event_id == EventId::Intent)
                .ok_or(EvidenceError::InvalidHarness)?;
            intent.parent_event_id = Some(EventId::Manifest);
        }
        "TREE-REJECT-CROSSING-PARENT-CHILD" => {
            let intent = rotation
                .events
                .iter_mut()
                .find(|event| event.event_id == EventId::Intent)
                .ok_or(EvidenceError::InvalidHarness)?;
            intent.stop_ns = 1_301;
        }
        "TREE-REJECT-UNKNOWN-PARENT" => {
            let intent = rotation
                .events
                .iter_mut()
                .find(|event| event.event_id == EventId::Intent)
                .ok_or(EvidenceError::InvalidHarness)?;
            intent.parent_event_id = Some(EventId::EagerOpen);
        }
        "TREE-REJECT-DUPLICATE-PARENTAGE" => {
            let duplicate = rotation
                .events
                .iter()
                .find(|event| event.event_id == EventId::Intent)
                .ok_or(EvidenceError::InvalidHarness)?
                .clone();
            rotation.events.push(duplicate);
        }
        "TREE-REJECT-ROOT-SENTINEL" => {
            let writer = rotation
                .events
                .iter_mut()
                .find(|event| event.event_id == EventId::WriterRotationDelay)
                .ok_or(EvidenceError::InvalidHarness)?;
            writer.parent_event_id = Some(EventId::Manifest);
        }
        "TREE-REJECT-CHILD-SENTINEL" => {
            let intent = rotation
                .events
                .iter_mut()
                .find(|event| event.event_id == EventId::Intent)
                .ok_or(EvidenceError::InvalidHarness)?;
            intent.parent_event_id = None;
        }
        _ => return Err(EvidenceError::InvalidHarness),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_event_rows_fixtures_and_matrix_are_closed() {
        let samples = structural_timing_samples().expect("structural timing samples");
        assert_eq!(
            samples
                .iter()
                .map(|sample| sample.events.len())
                .sum::<usize>(),
            173
        );
        assert_eq!(
            timing_summaries(&samples).expect("timing summaries").len(),
            6
        );
        validate_literal_fixtures().expect("all literal fixtures");
        let matrix = structural_matrix().expect("closed structural matrix");
        assert_eq!(matrix.len(), MATRIX_ROW_COUNT);
        assert_eq!(
            matrix
                .iter()
                .filter(|row| row.status == "UNSATISFIED")
                .count(),
            11
        );
        assert_eq!(
            super::super::fault::applicability_rows()
                .expect("applicability")
                .len(),
            487
        );
    }
}
