use super::fault::{FaultId, FaultSelection};
use super::matrix::{self, MatrixRow, TimingSample, TimingSummary};
use super::schema::{
    FaultMode, PressureKind, ReportClassification, RotationTriggerPath, SampleClass, SampleOutcome,
    StoreMode,
};
use super::supervisor::CrashWitness;
use crate::error::{EvidenceError, Result};
use crate::root::EvidenceRoot;
use crate::sha256::{Sha256, digest, hex, parse_hex};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub(super) const DATA_FILES: [&str; 7] = [
    "fault-registry.tsv",
    "fault-results.tsv",
    "matrix.tsv",
    "resource-ledger.tsv",
    "run.kv",
    "timing-samples.tsv",
    "timing-summary.tsv",
];
pub(super) const ALL_FILES: [&str; 8] = [
    "SHA256SUMS",
    "fault-registry.tsv",
    "fault-results.tsv",
    "matrix.tsv",
    "resource-ledger.tsv",
    "run.kv",
    "timing-samples.tsv",
    "timing-summary.tsv",
];
pub(super) const MAX_BUNDLE_BYTES: usize = 64 * 1_024 * 1_024;
pub(super) const MAX_FILE_BYTES: usize = 16 * 1_024 * 1_024;
pub(super) const MAX_LINE_BYTES: usize = 4_096;
pub(super) const MAX_SCALAR_BYTES: usize = 1_024;
const PLAN_ACCEPTANCE_SHA: &str = "af67792cbd28eb74cead673a0044c5f54d27ee6c";
const STRUCTURAL_BUNDLE: &str = "m03-pr03g2-structural";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReportContext {
    pub(super) classification: ReportClassification,
    pub(super) harness_git_sha: String,
    pub(super) measured_source_git_sha: String,
    pub(super) tracked_tree_status: &'static str,
    pub(super) untracked_tree_status: &'static str,
    pub(super) command: &'static str,
    pub(super) collection_authorized: bool,
    pub(super) measured_native_evidence: bool,
}

impl ReportContext {
    pub(super) fn structural() -> Self {
        Self {
            classification: ReportClassification::StructuralSynthetic,
            harness_git_sha: "UNCOMMITTED_STRUCTURAL".to_owned(),
            measured_source_git_sha: "NOT_APPLICABLE".to_owned(),
            tracked_tree_status: "MODIFIED_STRUCTURAL",
            untracked_tree_status: "NOT_MEASURED",
            command: "native-harness-check",
            collection_authorized: false,
            measured_native_evidence: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReportSummary {
    pub(super) bundle_files: usize,
    pub(super) bundle_bytes: usize,
    pub(super) timing_rows: usize,
    pub(super) timing_summary_rows: usize,
    pub(super) resource_rows: usize,
    pub(super) registry_rows: usize,
    pub(super) fault_result_rows: usize,
    pub(super) matrix_rows: usize,
}

pub(super) fn write_and_validate(
    root: &EvidenceRoot,
    context: &ReportContext,
    witnesses: &[CrashWitness],
) -> Result<ReportSummary> {
    let bundle = build_bundle(context, witnesses)?;
    validate_bundle_bytes(&bundle, context, witnesses)?;
    let reports = prepare_reports(root)?;
    let final_path = reports.join(bundle_name(context.classification));
    let staging = reports.join(format!(".{}.staging", bundle_name(context.classification)));
    if staging.exists() {
        return Err(EvidenceError::UnsafeInventory);
    }
    if final_path.exists() {
        validate_bundle_path(&final_path, context, witnesses)?;
        fs::remove_dir_all(&final_path).map_err(|_| EvidenceError::Io)?;
        File::open(&reports)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| EvidenceError::Io)?;
    }
    fs::create_dir(&staging).map_err(|_| EvidenceError::Io)?;
    for (name, bytes) in &bundle {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(staging.join(name))
            .map_err(|_| EvidenceError::Io)?;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| EvidenceError::Io)?;
    }
    File::open(&staging)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| EvidenceError::Io)?;
    validate_bundle_path(&staging, context, witnesses)?;
    fs::rename(&staging, &final_path).map_err(|_| EvidenceError::Io)?;
    File::open(&reports)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| EvidenceError::Io)?;
    validate_bundle_path(&final_path, context, witnesses)?;
    summary(&bundle)
}

pub(super) fn preflight(context: &ReportContext) -> Result<()> {
    validate_structural_context(context)?;
    let witnesses = FaultId::ALL
        .iter()
        .copied()
        .map(|id| {
            let value = hex(&digest(id.as_str().as_bytes())?);
            Ok(CrashWitness {
                id,
                pre_fingerprint: value.clone(),
                immediate_fingerprint: value.clone(),
                reopen_fingerprint: value.clone(),
                final_fingerprint: value,
                root: id.descriptor().expected_root,
                terminal: super::supervisor::structural_terminal(id),
                ready_validated: true,
                reaped: true,
                cleanup_attempts: 1,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let bundle = build_bundle(context, &witnesses)?;
    validate_bundle_bytes(&bundle, context, &witnesses)
}

fn prepare_reports(root: &EvidenceRoot) -> Result<PathBuf> {
    let requested = root.path.join("reports");
    fs::create_dir_all(&requested).map_err(|_| EvidenceError::Io)?;
    let reports = fs::canonicalize(requested).map_err(|_| EvidenceError::Io)?;
    if reports.parent() != Some(root.path.as_path()) {
        return Err(EvidenceError::UnsafeInventory);
    }
    for entry in fs::read_dir(&reports).map_err(|_| EvidenceError::Io)? {
        let entry = entry.map_err(|_| EvidenceError::Io)?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| EvidenceError::UnsafeInventory)?;
        if !entry.file_type().map_err(|_| EvidenceError::Io)?.is_dir() || name != STRUCTURAL_BUNDLE
        {
            return Err(EvidenceError::UnsafeInventory);
        }
    }
    Ok(reports)
}

fn build_bundle(
    context: &ReportContext,
    witnesses: &[CrashWitness],
) -> Result<BTreeMap<String, Vec<u8>>> {
    validate_structural_context(context)?;
    validate_witnesses(witnesses)?;
    let timing = matrix::structural_timing_samples()?;
    let summaries = matrix::timing_summaries(&timing)?;
    let matrix = matrix::structural_matrix()?;
    let applicability = super::fault::applicability_rows()?;
    let mut bundle = BTreeMap::new();
    bundle.insert("run.kv".to_owned(), run_kv(context)?.into_bytes());
    bundle.insert(
        "timing-samples.tsv".to_owned(),
        timing_samples_tsv(&timing)?.into_bytes(),
    );
    bundle.insert(
        "timing-summary.tsv".to_owned(),
        timing_summary_tsv(&summaries)?.into_bytes(),
    );
    bundle.insert(
        "resource-ledger.tsv".to_owned(),
        resource_ledger_tsv(&summaries)?.into_bytes(),
    );
    bundle.insert(
        "fault-registry.tsv".to_owned(),
        fault_registry_tsv()?.into_bytes(),
    );
    bundle.insert(
        "fault-results.tsv".to_owned(),
        fault_results_tsv(&applicability, witnesses)?.into_bytes(),
    );
    bundle.insert("matrix.tsv".to_owned(), matrix_tsv(&matrix)?.into_bytes());
    let checksums = checksum_file(&bundle)?;
    bundle.insert("SHA256SUMS".to_owned(), checksums.into_bytes());
    preflight_bundle(&bundle)?;
    Ok(bundle)
}

fn run_kv(context: &ReportContext) -> Result<String> {
    let hashes = source_hashes()?;
    let platform = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "LINUX_X86_64",
        ("macos", "aarch64") => "DARWIN_ARM64",
        _ => "EXCLUDED_PLATFORM",
    };
    let mut values = BTreeMap::from([
        ("schema_version", super::schema::REPORT_SCHEMA.to_owned()),
        (
            "report_classification",
            context.classification.as_str().to_owned(),
        ),
        ("plan_acceptance_git_sha", PLAN_ACCEPTANCE_SHA.to_owned()),
        ("plan_sha256", hashes.plan),
        ("harness_git_sha", context.harness_git_sha.clone()),
        (
            "measured_source_git_sha",
            context.measured_source_git_sha.clone(),
        ),
        (
            "tracked_tree_status",
            context.tracked_tree_status.to_owned(),
        ),
        (
            "untracked_tree_status",
            context.untracked_tree_status.to_owned(),
        ),
        ("cargo_lock_sha256", hashes.cargo_lock),
        ("harness_source_sha256", hashes.harness),
        ("instrumentation_source_sha256", hashes.instrumentation),
        ("command", context.command.to_owned()),
        ("profile", "STRUCTURAL_ONLY".to_owned()),
        ("rust_version", "1.98.0".to_owned()),
        ("cargo_version", "1.98.0".to_owned()),
        ("platform", platform.to_owned()),
        ("cpu", "UNKNOWN".to_owned()),
        ("memory", "UNKNOWN".to_owned()),
        ("filesystem", "UNKNOWN".to_owned()),
        ("mount_locality", "UNKNOWN".to_owned()),
        ("load", "UNKNOWN".to_owned()),
        ("storage", "UNKNOWN".to_owned()),
        ("headroom", "UNKNOWN".to_owned()),
        ("page_size", "UNKNOWN".to_owned()),
        ("process_cache", "COLD".to_owned()),
        ("filesystem_cache", "UNKNOWN".to_owned()),
        ("store_mode", "NEW".to_owned()),
        (
            "collection_authorized",
            bool_text(context.collection_authorized).to_owned(),
        ),
        (
            "measured_native_evidence",
            bool_text(context.measured_native_evidence).to_owned(),
        ),
        ("v2_product_authority", "false".to_owned()),
        ("writer_delay_threshold", "UNKNOWN".to_owned()),
        ("eager_open_threshold", "UNKNOWN".to_owned()),
        ("rss_threshold", "UNKNOWN".to_owned()),
        ("total_runtime_threshold", "UNKNOWN".to_owned()),
        ("external_workspace_threshold", "UNKNOWN".to_owned()),
        ("native_budgets", "UNKNOWN".to_owned()),
        ("native_slos", "UNKNOWN".to_owned()),
        ("physical_power_loss", "EXCLUDED".to_owned()),
        ("canonical_data", "ABSENT".to_owned()),
    ])
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value))
    .collect::<BTreeMap<String, String>>();
    for index in 1..=11 {
        values.insert(format!("pr03e_m{index:02}"), "UNSATISFIED".to_owned());
    }
    let mut output = String::new();
    for (key, value) in values {
        validate_scalar(&key)?;
        validate_scalar(&value)?;
        writeln!(output, "{key}={value}").map_err(|_| EvidenceError::Bounds)?;
    }
    Ok(output)
}

fn timing_samples_tsv(samples: &[TimingSample]) -> Result<String> {
    matrix::validate_timing_samples(samples)?;
    let header = "schema_version\tevent_id\tparent_event_id\tphase_id\tcase_id\tsample_id\tprocess_mode\tstore_mode\trotation_trigger_path\tdurability_batch_id\tsample_class\tsample_outcome\tfault_id\tfault_mode\tpressure_kind\tprocess_cache\tfilesystem_cache\tstart_ns\tstop_ns\telapsed_ns\toutcome\ttrace_status\tdistribution_eligible\tpair_ordinal\troot_classification";
    let mut output = String::new();
    writeln!(output, "{header}").map_err(|_| EvidenceError::Bounds)?;
    let mut rows = 0_usize;
    for sample in samples {
        for event in &sample.events {
            let elapsed = event
                .stop_ns
                .checked_sub(event.start_ns)
                .ok_or(EvidenceError::InvalidHarness)?;
            write_tsv_row(
                &mut output,
                &[
                    super::schema::REPORT_SCHEMA.to_owned(),
                    event.event_id.as_str().to_owned(),
                    event
                        .parent_event_id
                        .map_or_else(|| "NONE".to_owned(), |id| id.as_str().to_owned()),
                    event.phase_id.as_str().to_owned(),
                    sample.case_id.to_owned(),
                    sample.sample_id.clone(),
                    sample.process_mode.as_str().to_owned(),
                    sample.store_mode.as_str().to_owned(),
                    sample.trigger_path.as_str().to_owned(),
                    optional_u64(sample.durability_batch_id),
                    sample.sample_class.as_str().to_owned(),
                    sample.sample_outcome.as_str().to_owned(),
                    sample
                        .fault_id
                        .map_or_else(|| "NONE".to_owned(), |id| id.as_str().to_owned()),
                    sample.fault_mode.as_str().to_owned(),
                    sample.pressure_kind.as_str().to_owned(),
                    sample.process_cache.as_str().to_owned(),
                    sample.filesystem_cache.as_str().to_owned(),
                    event.start_ns.to_string(),
                    event.stop_ns.to_string(),
                    elapsed.to_string(),
                    event.outcome.as_str().to_owned(),
                    sample.trace_status.as_str().to_owned(),
                    bool_text(sample.distribution_eligible).to_owned(),
                    event
                        .pair_ordinal
                        .map_or_else(|| "NONE".to_owned(), |value| value.to_string()),
                    event.root.as_str().to_owned(),
                ],
            )?;
            rows = rows.checked_add(1).ok_or(EvidenceError::Bounds)?;
        }
    }
    if rows != matrix::TIMING_SAMPLE_ROW_COUNT {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(output)
}

fn timing_summary_tsv(summaries: &[TimingSummary]) -> Result<String> {
    let mut output = String::from(
        "schema_version\treport_classification\tcase_id\trequired_eligible_success_count\tactual_distinct_eligible_success_count\tdeclared_trigger_path\tobserved_trigger_path\tmin_ns\tmedian_ns\tp90_ns\tp95_ns\tp99_ns\tmax_ns\tiqr_ns\tmad_ns\tpercentile_claim\n",
    );
    for summary in summaries {
        write_tsv_row(
            &mut output,
            &[
                super::schema::REPORT_SCHEMA.to_owned(),
                "STRUCTURAL_SYNTHETIC".to_owned(),
                summary.case_id.to_owned(),
                summary.required_count.to_string(),
                summary.actual_count.to_string(),
                summary.trigger_path.as_str().to_owned(),
                summary.trigger_path.as_str().to_owned(),
                summary.min_ns.to_string(),
                summary.median_ns.to_string(),
                summary.p90_ns.to_string(),
                summary.p95_ns.to_string(),
                summary.p99_ns.to_string(),
                summary.max_ns.to_string(),
                summary.iqr_ns.to_string(),
                summary.mad_ns.to_string(),
                summary.percentile_claim.to_owned(),
            ],
        )?;
    }
    if summaries.len() != matrix::TIMING_SUMMARY_ROW_COUNT {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(output)
}

fn resource_ledger_tsv(summaries: &[TimingSummary]) -> Result<String> {
    let header = "schema_version\tcase_id\tframe_metadata_requested\tframe_metadata_actual\tobservation_index_requested\tobservation_index_actual\tinput_frame_requested\tinput_frame_actual\tcanonical_reencode_requested\tcanonical_reencode_actual\tdecoder_requested\tdecoder_actual\treencoder_requested\treencoder_actual\tio_scratch_requested\tio_scratch_actual\ttransaction_records_requested\ttransaction_records_actual\treceipt_records_requested\treceipt_records_actual\tfault_state_requested\tfault_state_actual\tpair_state_requested\tpair_state_actual\tstack_assumption_bytes\tthread_count\trss_value\trss_source\trss_units\tartifact_logical_bytes\tartifact_allocated_bytes\texternal_workspace_logical_requested\texternal_workspace_logical_actual\texternal_workspace_allocated_requested\texternal_workspace_allocated_actual\tmax_concurrent_inventory\tavailable_storage\tplanned_headroom\tpage_size\tprocess_cache\tfilesystem_cache\tstore_mode";
    let mut output = String::new();
    writeln!(output, "{header}").map_err(|_| EvidenceError::Bounds)?;
    for summary in summaries {
        write_tsv_row(
            &mut output,
            &[
                super::schema::REPORT_SCHEMA.to_owned(),
                summary.case_id.to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "65536".to_owned(),
                "173".to_owned(),
                "173".to_owned(),
                "16".to_owned(),
                "16".to_owned(),
                "1".to_owned(),
                "1".to_owned(),
                if summary.case_id == "EAGER-OPEN-64" {
                    "1"
                } else {
                    "0"
                }
                .to_owned(),
                if summary.case_id == "EAGER-OPEN-64" {
                    "1"
                } else {
                    "0"
                }
                .to_owned(),
                "UNKNOWN".to_owned(),
                "1".to_owned(),
                "UNKNOWN".to_owned(),
                "UNAVAILABLE_STRUCTURAL".to_owned(),
                "BYTES".to_owned(),
                "0".to_owned(),
                "0".to_owned(),
                "0".to_owned(),
                "0".to_owned(),
                "0".to_owned(),
                "0".to_owned(),
                "156".to_owned(),
                "UNKNOWN".to_owned(),
                "UNKNOWN".to_owned(),
                "UNKNOWN".to_owned(),
                "COLD".to_owned(),
                "UNKNOWN".to_owned(),
                StoreMode::New.as_str().to_owned(),
            ],
        )?;
    }
    if summaries.len() != matrix::RESOURCE_ROW_COUNT {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(output)
}

fn fault_registry_tsv() -> Result<String> {
    let mut output = String::from(
        "schema_version\tfault_id\tphase_id\tartifact\toperation\tmutation\tshort_write_allowed\tpressure_allowed\tmaximum_occurrence\tallowed_next_fault_ids\tallowed_terminal_states\tcommit_side\texpected_root_class\n",
    );
    for id in FaultId::ALL {
        let descriptor = id.descriptor();
        write_tsv_row(
            &mut output,
            &[
                super::schema::REPORT_SCHEMA.to_owned(),
                id.as_str().to_owned(),
                descriptor.phase.as_str().to_owned(),
                descriptor.artifact.as_str().to_owned(),
                descriptor.operation.as_str().to_owned(),
                bool_text(descriptor.mutation).to_owned(),
                bool_text(descriptor.short_write).to_owned(),
                bool_text(descriptor.pressure).to_owned(),
                descriptor.maximum_occurrence.to_string(),
                joined_ids(descriptor.successors.iter().map(|value| value.as_str())),
                joined_ids(descriptor.terminals.iter().map(|value| value.as_str())),
                descriptor.commit_side.as_str().to_owned(),
                descriptor.expected_root.as_str().to_owned(),
            ],
        )?;
    }
    Ok(output)
}

fn fault_results_tsv(
    applicability: &[FaultSelection],
    witnesses: &[CrashWitness],
) -> Result<String> {
    let witness_map = witnesses
        .iter()
        .map(|witness| (witness.id, witness))
        .collect::<BTreeMap<_, _>>();
    let mut output = String::from(
        "schema_version\tsample_id\tsample_class\tsample_outcome\tdistribution_eligible\tfault_id\tfault_mode\trepetition\texpected_result\tactual_result\tpressure_kind\traw_diagnostic\thandled_stage\tdurable_stage\tdurability_batch_id\trotation_trigger_path\tcompleted_event_prefix\tprocess_result\tlast_successful_boundary\tpre_fingerprint\timmediate_fingerprint\treopen_fingerprint\tfinal_fingerprint\troot_classification\tchild_stop_event\tparent_cleanup_attempts\n",
    );
    for (index, selection) in applicability.iter().copied().enumerate() {
        let crash = selection.mode == FaultMode::ChildCrashAfterSuccess;
        let witness = if crash {
            Some(
                *witness_map
                    .get(&selection.id)
                    .ok_or(EvidenceError::InvalidHarness)?,
            )
        } else {
            None
        };
        let base = synthetic_fingerprint(selection, "PRE")?;
        let immediate = if selection.mode == FaultMode::ShortPartialWrite {
            synthetic_fingerprint(selection, "IMMEDIATE")?
        } else {
            base.clone()
        };
        let (pre, immediate, reopen, final_fingerprint) = witness.map_or_else(
            || (base.clone(), immediate, base.clone(), base),
            |value| {
                (
                    value.pre_fingerprint.clone(),
                    value.immediate_fingerprint.clone(),
                    value.reopen_fingerprint.clone(),
                    value.final_fingerprint.clone(),
                )
            },
        );
        let sample_class = if selection.pressure == PressureKind::None {
            SampleClass::Fault
        } else {
            SampleClass::Pressure
        };
        let outcome = if crash {
            SampleOutcome::Crashed
        } else if selection.pressure != PressureKind::None {
            SampleOutcome::ReopenRequired
        } else {
            SampleOutcome::Error
        };
        let structural_result = witness.map_or_else(
            || "STRUCTURAL_MATCH".to_owned(),
            |value| value.terminal.as_str().to_owned(),
        );
        write_tsv_row(
            &mut output,
            &[
                super::schema::REPORT_SCHEMA.to_owned(),
                format!("FAULT-{:04}", index + 1),
                sample_class.as_str().to_owned(),
                outcome.as_str().to_owned(),
                "false".to_owned(),
                selection.id.as_str().to_owned(),
                selection.mode.as_str().to_owned(),
                "1".to_owned(),
                structural_result.clone(),
                structural_result,
                selection.pressure.as_str().to_owned(),
                "NONE".to_owned(),
                "NOT_APPLICABLE".to_owned(),
                "NOT_APPLICABLE".to_owned(),
                "NONE".to_owned(),
                RotationTriggerPath::NotApplicable.as_str().to_owned(),
                selection.id.as_str().to_owned(),
                if crash {
                    "KILLED_REAPED"
                } else {
                    "IN_PROCESS_INJECTED"
                }
                .to_owned(),
                if crash { selection.id.as_str() } else { "NONE" }.to_owned(),
                pre,
                immediate,
                reopen,
                final_fingerprint,
                selection.id.descriptor().expected_root.as_str().to_owned(),
                if crash {
                    "NOT_EMITTED"
                } else {
                    "NOT_APPLICABLE"
                }
                .to_owned(),
                if crash { "1" } else { "0" }.to_owned(),
            ],
        )?;
    }
    if applicability.len() != 487 || witness_map.len() != FaultId::ALL.len() {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(output)
}

fn matrix_tsv(rows: &[MatrixRow]) -> Result<String> {
    let mut output =
        String::from("schema_version\trow_id\tcategory\texpected_rows\tobserved_rows\tstatus\n");
    for row in rows {
        write_tsv_row(
            &mut output,
            &[
                super::schema::REPORT_SCHEMA.to_owned(),
                row.row_id.clone(),
                row.category.to_owned(),
                row.expected_rows.to_string(),
                row.observed_rows.to_string(),
                row.status.to_owned(),
            ],
        )?;
    }
    if rows.len() != matrix::MATRIX_ROW_COUNT {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(output)
}

fn write_tsv_row(output: &mut String, values: &[String]) -> Result<()> {
    for (index, value) in values.iter().enumerate() {
        validate_scalar(value)?;
        if index != 0 {
            output.push('\t');
        }
        output.push_str(value);
    }
    output.push('\n');
    Ok(())
}

fn joined_ids<'a>(values: impl Iterator<Item = &'a str>) -> String {
    let values = values.collect::<Vec<_>>();
    if values.is_empty() {
        "NONE".to_owned()
    } else {
        values.join(",")
    }
}

fn synthetic_fingerprint(selection: FaultSelection, suffix: &str) -> Result<String> {
    let identity = format!(
        "{}:{}:{}:{suffix}",
        selection.id.as_str(),
        selection.mode.as_str(),
        selection.pressure.as_str()
    );
    Ok(hex(&digest(identity.as_bytes())?))
}

fn validate_witnesses(witnesses: &[CrashWitness]) -> Result<()> {
    let ids = witnesses
        .iter()
        .map(|witness| witness.id)
        .collect::<BTreeSet<_>>();
    if witnesses.len() != FaultId::ALL.len()
        || ids.len() != witnesses.len()
        || FaultId::ALL.iter().any(|id| !ids.contains(id))
        || witnesses.iter().any(|witness| {
            !witness.ready_validated
                || !witness.reaped
                || witness.cleanup_attempts != 1
                || witness.root != witness.id.descriptor().expected_root
                || witness.terminal != super::supervisor::structural_terminal(witness.id)
        })
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

fn checksum_file(bundle: &BTreeMap<String, Vec<u8>>) -> Result<String> {
    if bundle.len() != DATA_FILES.len() || DATA_FILES.iter().any(|name| !bundle.contains_key(*name))
    {
        return Err(EvidenceError::InvalidHarness);
    }
    let mut output = String::new();
    for name in DATA_FILES {
        let bytes = bundle.get(name).ok_or(EvidenceError::InvalidHarness)?;
        writeln!(output, "{}  {name}", hex(&digest(bytes)?)).map_err(|_| EvidenceError::Bounds)?;
    }
    Ok(output)
}

fn preflight_bundle(bundle: &BTreeMap<String, Vec<u8>>) -> Result<()> {
    if bundle.len() != ALL_FILES.len() || ALL_FILES.iter().any(|name| !bundle.contains_key(*name)) {
        return Err(EvidenceError::InvalidHarness);
    }
    for (name, bytes) in bundle {
        validate_report_identity(name)?;
        validate_file_bytes(bytes)?;
        if bytes.len() > MAX_FILE_BYTES {
            return Err(EvidenceError::Replan);
        }
    }
    checked_bundle_total(bundle.values().map(Vec::len))?;
    Ok(())
}

fn checked_bundle_total(mut lengths: impl Iterator<Item = usize>) -> Result<usize> {
    let total = lengths.try_fold(0_usize, |total, length| {
        total.checked_add(length).ok_or(EvidenceError::Replan)
    })?;
    if total > MAX_BUNDLE_BYTES {
        return Err(EvidenceError::Replan);
    }
    Ok(total)
}

fn validate_bundle_bytes(
    actual: &BTreeMap<String, Vec<u8>>,
    context: &ReportContext,
    witnesses: &[CrashWitness],
) -> Result<()> {
    preflight_bundle(actual)?;
    validate_checksums(actual)?;
    let expected = build_expected_without_validation(context, witnesses)?;
    if actual != &expected {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

fn build_expected_without_validation(
    context: &ReportContext,
    witnesses: &[CrashWitness],
) -> Result<BTreeMap<String, Vec<u8>>> {
    let timing = matrix::structural_timing_samples()?;
    let summaries = matrix::timing_summaries(&timing)?;
    let matrix_rows = matrix::structural_matrix()?;
    let applicability = super::fault::applicability_rows()?;
    let mut expected = BTreeMap::from([
        ("run.kv".to_owned(), run_kv(context)?.into_bytes()),
        (
            "timing-samples.tsv".to_owned(),
            timing_samples_tsv(&timing)?.into_bytes(),
        ),
        (
            "timing-summary.tsv".to_owned(),
            timing_summary_tsv(&summaries)?.into_bytes(),
        ),
        (
            "resource-ledger.tsv".to_owned(),
            resource_ledger_tsv(&summaries)?.into_bytes(),
        ),
        (
            "fault-registry.tsv".to_owned(),
            fault_registry_tsv()?.into_bytes(),
        ),
        (
            "fault-results.tsv".to_owned(),
            fault_results_tsv(&applicability, witnesses)?.into_bytes(),
        ),
        (
            "matrix.tsv".to_owned(),
            matrix_tsv(&matrix_rows)?.into_bytes(),
        ),
    ]);
    expected.insert(
        "SHA256SUMS".to_owned(),
        checksum_file(&expected)?.into_bytes(),
    );
    Ok(expected)
}

fn validate_checksums(bundle: &BTreeMap<String, Vec<u8>>) -> Result<()> {
    let checksums = std::str::from_utf8(
        bundle
            .get("SHA256SUMS")
            .ok_or(EvidenceError::InvalidHarness)?,
    )
    .map_err(|_| EvidenceError::InvalidHarness)?;
    let mut names = Vec::new();
    for line in checksums.lines() {
        let (hash, name) = line.split_once("  ").ok_or(EvidenceError::InvalidHarness)?;
        if hash != hash.to_ascii_lowercase()
            || parse_hex(hash).is_err()
            || !DATA_FILES.contains(&name)
            || hex(&digest(
                bundle.get(name).ok_or(EvidenceError::InvalidHarness)?,
            )?) != hash
        {
            return Err(EvidenceError::InvalidHarness);
        }
        names.push(name);
    }
    if names != DATA_FILES {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

fn validate_bundle_path(
    path: &Path,
    context: &ReportContext,
    witnesses: &[CrashWitness],
) -> Result<()> {
    let mut bundle = BTreeMap::new();
    for entry in fs::read_dir(path).map_err(|_| EvidenceError::Io)? {
        let entry = entry.map_err(|_| EvidenceError::Io)?;
        if !entry.file_type().map_err(|_| EvidenceError::Io)?.is_file() {
            return Err(EvidenceError::UnsafeInventory);
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| EvidenceError::UnsafeInventory)?;
        validate_report_identity(&name)?;
        if bundle.contains_key(&name) {
            return Err(EvidenceError::InvalidHarness);
        }
        bundle.insert(name, read_bounded_file(&entry.path())?);
    }
    validate_bundle_bytes(&bundle, context, witnesses)
}

fn read_bounded_file(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path).map_err(|_| EvidenceError::Io)?;
    let metadata = file.metadata().map_err(|_| EvidenceError::Io)?;
    if !metadata.is_file()
        || metadata.len() > u64::try_from(MAX_FILE_BYTES).map_err(|_| EvidenceError::Bounds)?
    {
        return Err(EvidenceError::Bounds);
    }
    let mut bytes = Vec::new();
    file.take(u64::try_from(MAX_FILE_BYTES + 1).map_err(|_| EvidenceError::Bounds)?)
        .read_to_end(&mut bytes)
        .map_err(|_| EvidenceError::Io)?;
    if bytes.len() > MAX_FILE_BYTES {
        return Err(EvidenceError::Bounds);
    }
    Ok(bytes)
}

fn validate_report_identity(name: &str) -> Result<()> {
    if !ALL_FILES.contains(&name)
        || name.starts_with('/')
        || name.contains('\u{005c}')
        || name.contains("..")
    {
        return Err(EvidenceError::UnsafeInventory);
    }
    Ok(())
}

fn validate_file_bytes(bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() || !bytes.ends_with(b"\n") || std::str::from_utf8(bytes).is_err() {
        return Err(EvidenceError::InvalidHarness);
    }
    for physical in bytes.split_inclusive(|byte| *byte == b'\n') {
        if physical.len() > MAX_LINE_BYTES {
            return Err(EvidenceError::Bounds);
        }
        let line = physical
            .strip_suffix(b"\n")
            .ok_or(EvidenceError::InvalidHarness)?;
        let text = std::str::from_utf8(line).map_err(|_| EvidenceError::InvalidHarness)?;
        if text.is_empty() {
            return Err(EvidenceError::InvalidHarness);
        }
        if text.contains('\t') {
            for scalar in text.split('\t') {
                validate_scalar(scalar)?;
            }
        } else if let Some((key, value)) = text.split_once('=') {
            validate_scalar(key)?;
            validate_scalar(value)?;
        } else if let Some((hash, name)) = text.split_once("  ") {
            validate_scalar(hash)?;
            validate_report_identity(name)?;
        } else {
            validate_scalar(text)?;
        }
    }
    Ok(())
}

fn validate_scalar(value: &str) -> Result<()> {
    let lowercase = value.to_ascii_lowercase();
    let forbidden = [
        "username",
        "hostname",
        "cloud_project",
        "instance_id",
        "credential",
        "password",
        "secret_key",
        "environment_dump",
        "canonical_payload",
        "raw_journal_bytes",
        "segment_bytes",
        "core_dump",
    ];
    if value.is_empty()
        || value.len() > MAX_SCALAR_BYTES
        || value.starts_with('/')
        || value.contains('\u{005c}')
        || value.contains("..")
        || value.as_bytes().get(1) == Some(&b':')
        || value.bytes().any(|byte| {
            !(byte.is_ascii_alphanumeric()
                || matches!(byte, b'-' | b'_' | b'.' | b':' | b',' | b'+' | b'='))
        })
        || forbidden.iter().any(|word| lowercase.contains(word))
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

fn source_hashes() -> Result<SourceHashes> {
    Ok(SourceHashes {
        plan: hex(&digest(include_bytes!(
            "../../../../../docs/m03-pr03e-native-execution-evidence-plan.md"
        ))?),
        cargo_lock: hex(&digest(include_bytes!("../../../../../Cargo.lock"))?),
        harness: digest_sources(&[
            include_bytes!("../../main.rs"),
            include_bytes!("../../harness/mod.rs"),
            include_bytes!("../../root.rs"),
            include_bytes!("mod.rs"),
            include_bytes!("fault.rs"),
            include_bytes!("inventory.rs"),
            include_bytes!("matrix.rs"),
            include_bytes!("oracle.rs"),
            include_bytes!("report.rs"),
            include_bytes!("schema.rs"),
            include_bytes!("supervisor.rs"),
            include_bytes!("transaction.rs"),
        ])?,
        instrumentation: digest_sources(&[
            include_bytes!("../../../../../crates/och-store/src/__m03_pr03e_native_harness.rs"),
            include_bytes!("../../../../../crates/och-runtime/src/__m03_pr03e_native_harness.rs"),
            include_bytes!("../../../../../crates/och-store/src/active.rs"),
            include_bytes!("../../../../../crates/och-store/src/manifest.rs"),
            include_bytes!("../../../../../crates/och-runtime/src/ingress.rs"),
            include_bytes!("../../../../../crates/och-runtime/src/store_worker.rs"),
        ])?,
    })
}

fn digest_sources(sources: &[&[u8]]) -> Result<String> {
    let mut hasher = Sha256::new();
    for source in sources {
        hasher.update(
            &u64::try_from(source.len())
                .map_err(|_| EvidenceError::Bounds)?
                .to_be_bytes(),
        )?;
        hasher.update(source)?;
    }
    Ok(hex(&hasher.finish()?))
}

struct SourceHashes {
    plan: String,
    cargo_lock: String,
    harness: String,
    instrumentation: String,
}

fn summary(bundle: &BTreeMap<String, Vec<u8>>) -> Result<ReportSummary> {
    let rows = |name: &str| -> Result<usize> {
        let bytes = bundle.get(name).ok_or(EvidenceError::InvalidHarness)?;
        Ok(bytes.split(|byte| *byte == b'\n').count().saturating_sub(2))
    };
    Ok(ReportSummary {
        bundle_files: bundle.len(),
        bundle_bytes: bundle.values().try_fold(0_usize, |sum, bytes| {
            sum.checked_add(bytes.len()).ok_or(EvidenceError::Bounds)
        })?,
        timing_rows: rows("timing-samples.tsv")?,
        timing_summary_rows: rows("timing-summary.tsv")?,
        resource_rows: rows("resource-ledger.tsv")?,
        registry_rows: rows("fault-registry.tsv")?,
        fault_result_rows: rows("fault-results.tsv")?,
        matrix_rows: rows("matrix.tsv")?,
    })
}

fn bundle_name(classification: ReportClassification) -> &'static str {
    match classification {
        ReportClassification::StructuralSynthetic => STRUCTURAL_BUNDLE,
        ReportClassification::AcceptanceCandidate => "UNAVAILABLE",
    }
}

fn validate_structural_context(context: &ReportContext) -> Result<()> {
    if context.classification != ReportClassification::StructuralSynthetic
        || context.collection_authorized
        || context.measured_native_evidence
        || context.command != "native-harness-check"
    {
        return Err(EvidenceError::Replan);
    }
    Ok(())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "NONE".to_owned(), |value| value.to_string())
}

const fn bool_text(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn witnesses() -> Vec<CrashWitness> {
        FaultId::ALL
            .iter()
            .copied()
            .map(|id| {
                let value = hex(&digest(id.as_str().as_bytes()).expect("witness hash"));
                CrashWitness {
                    id,
                    pre_fingerprint: value.clone(),
                    immediate_fingerprint: value.clone(),
                    reopen_fingerprint: value.clone(),
                    final_fingerprint: value,
                    root: id.descriptor().expected_root,
                    terminal: super::super::supervisor::structural_terminal(id),
                    ready_validated: true,
                    reaped: true,
                    cleanup_attempts: 1,
                }
            })
            .collect()
    }

    #[test]
    fn deterministic_nonempty_structural_bundle_is_exact_and_bounded() {
        let context = ReportContext::structural();
        let witnesses = witnesses();
        let first = build_bundle(&context, &witnesses).expect("first bundle");
        let second = build_bundle(&context, &witnesses).expect("second bundle");
        assert_eq!(first, second);
        validate_bundle_bytes(&first, &context, &witnesses).expect("validate bundle");
        let summary = summary(&first).expect("bundle summary");
        assert_eq!(summary.bundle_files, 8);
        assert_eq!(summary.timing_rows, 173);
        assert_eq!(summary.registry_rows, 173);
        assert_eq!(summary.fault_result_rows, 487);
        assert_eq!(summary.matrix_rows, 639);
        assert!(summary.bundle_bytes < MAX_BUNDLE_BYTES);
    }

    #[test]
    fn hostile_schema_checksum_source_and_bound_variants_refuse() {
        let context = ReportContext::structural();
        let witnesses = witnesses();
        let baseline = build_bundle(&context, &witnesses).expect("baseline bundle");
        for mutation in 0..8 {
            let mut hostile = baseline.clone();
            match mutation {
                0 => {
                    hostile.insert("unknown.tsv".to_owned(), b"x\n".to_vec());
                }
                1 => {
                    hostile.remove("matrix.tsv");
                }
                2 => hostile.get_mut("matrix.tsv").expect("matrix")[0] ^= 1,
                3 => hostile
                    .get_mut("run.kv")
                    .expect("run")
                    .extend_from_slice(b"hostname=forbidden\n"),
                4 => hostile
                    .get_mut("run.kv")
                    .expect("run")
                    .extend_from_slice(b"path=/absolute\n"),
                5 => hostile
                    .get_mut("run.kv")
                    .expect("run")
                    .extend_from_slice(b"path=one..two\n"),
                6 => {
                    hostile.insert("matrix.tsv".to_owned(), vec![b'x'; MAX_FILE_BYTES + 1]);
                }
                7 => hostile
                    .get_mut("run.kv")
                    .expect("run")
                    .extend_from_slice(&vec![b'x'; MAX_LINE_BYTES + 1]),
                _ => unreachable!(),
            }
            assert!(
                validate_bundle_bytes(&hostile, &context, &witnesses).is_err(),
                "mutation {mutation}"
            );
        }
        assert!(validate_scalar(&"x".repeat(MAX_SCALAR_BYTES + 1)).is_err());
        assert_eq!(
            checked_bundle_total([MAX_BUNDLE_BYTES].into_iter()).expect("exact bundle cap"),
            MAX_BUNDLE_BYTES
        );
        assert_eq!(
            checked_bundle_total([MAX_BUNDLE_BYTES, 1].into_iter()),
            Err(EvidenceError::Replan)
        );
    }

    #[test]
    fn acceptance_classification_cannot_enter_the_structural_bundle_builder() {
        let mut context = ReportContext::structural();
        context.classification = ReportClassification::AcceptanceCandidate;
        assert!(matches!(preflight(&context), Err(EvidenceError::Replan)));

        let source = include_str!("report.rs");
        assert!(!source.contains(&["measured_native_evidence", ": true"].concat()));
        assert!(!source.contains(&["collection_authorized", ": true"].concat()));
    }
}
