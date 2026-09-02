//! Private capability and executor subtree for disposable V2 store children.

use crate::error::{EvidenceError, Result};
use crate::root::{EvidenceRoot, FoundationSummary, HarnessSummary};
use crate::sha256::{Sha256, hex};
use fault::{Artifact, FaultDescriptor, FaultId, FaultSelection, Operation};
use inventory::{ArtifactFingerprint, InventoryFingerprint};
use schema::{FaultMode, PhaseId, PressureKind, RootClassification};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

mod fault;
mod inventory;
mod matrix;
mod oracle;
mod report;
mod schema;
mod supervisor;
mod transaction;

const MAX_SITE_BYTES: usize = 64 * 1_024;
const SITE_PAYLOAD: &[u8] = b"M03-PR03g1 disposable source-site fixture\n";
const SHORT_WRITE_BYTES: usize = 7;
const SHORT_WRITE_BASELINE: &[u8] = b"before-short-write\n";
const V2_CHILD_NAME: &str = "v2-executor-foundation";

struct V2StoreChild {
    cases: PathBuf,
    path: PathBuf,
    cleanup_attempted: bool,
}

impl V2StoreChild {
    fn acquire(root: &EvidenceRoot) -> Result<Self> {
        let cases = fs::canonicalize(root.path.join("cases")).map_err(|_| EvidenceError::Io)?;
        let path = cases.join(V2_CHILD_NAME);
        if path.exists() {
            return Err(EvidenceError::UnsafeInventory);
        }
        fs::create_dir(&path).map_err(|_| EvidenceError::Io)?;
        Ok(Self {
            cases,
            path,
            cleanup_attempted: false,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn cleanup(&mut self) -> Result<()> {
        if self.cleanup_attempted {
            return Err(EvidenceError::InvalidHarness);
        }
        self.cleanup_attempted = true;
        let canonical = fs::canonicalize(&self.path).map_err(|_| EvidenceError::Io)?;
        if canonical.parent() != Some(self.cases.as_path()) {
            return Err(EvidenceError::UnsafeInventory);
        }
        fs::remove_dir_all(canonical).map_err(|_| EvidenceError::Io)
    }
}

impl Drop for V2StoreChild {
    fn drop(&mut self) {
        if !self.cleanup_attempted {
            let _ = self.cleanup();
        }
    }
}

pub(super) fn run_foundation(root: &EvidenceRoot) -> Result<FoundationSummary> {
    schema::validate_closed_schema()?;
    fault::validate_registry()?;
    validate_compiled_registry_bijection()?;
    inventory::canonical_inventory_names()?;
    oracle::validate_foundation_oracles()?;
    let _ = crate::sha256::digest(b"M03-PR03g1")?;

    run_child(root, transaction::run_foundation)
}

pub(super) fn hidden_child_command(arguments: &[String]) -> Result<()> {
    supervisor::hidden_child_command(arguments)
}

pub(super) fn run_harness(root: &EvidenceRoot) -> Result<HarnessSummary> {
    run_complete(root, &report::ReportContext::structural())
}

fn run_complete(root: &EvidenceRoot, context: &report::ReportContext) -> Result<HarnessSummary> {
    schema::validate_closed_schema()?;
    fault::validate_registry()?;
    validate_compiled_registry_bijection()?;
    let matrix = matrix::structural_matrix()?;
    if matrix.len() != matrix::MATRIX_ROW_COUNT {
        return Err(EvidenceError::InvalidHarness);
    }
    report::preflight(context)?;
    let foundation = run_foundation(root)?;
    let witnesses = supervisor::run_campaign(root)?;
    let summary = report::write_and_validate(root, context, &witnesses)?;
    if witnesses.len() != FaultId::ALL.len()
        || summary.bundle_files != report::ALL_FILES.len()
        || summary.timing_rows != matrix::TIMING_SAMPLE_ROW_COUNT
        || summary.timing_summary_rows != matrix::TIMING_SUMMARY_ROW_COUNT
        || summary.resource_rows != matrix::RESOURCE_ROW_COUNT
        || summary.registry_rows != FaultId::ALL.len()
        || summary.fault_result_rows != fault::applicability_rows()?.len()
        || summary.matrix_rows != matrix::MATRIX_ROW_COUNT
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(HarnessSummary {
        schema: schema::REPORT_SCHEMA,
        classification: context.classification.as_str(),
        descriptor_count: foundation.descriptor_count,
        crash_target_count: witnesses.len(),
        matrix_rows: summary.matrix_rows,
        timing_rows: summary.timing_rows,
        timing_summary_rows: summary.timing_summary_rows,
        resource_rows: summary.resource_rows,
        registry_rows: summary.registry_rows,
        fault_result_rows: summary.fault_result_rows,
        bundle_files: summary.bundle_files,
        bundle_bytes: summary.bundle_bytes,
        collection_authorized: context.collection_authorized,
        measured_native_evidence: context.measured_native_evidence,
    })
}

fn run_child<T>(
    root: &EvidenceRoot,
    operation: impl FnOnce(&V2StoreChild) -> Result<T>,
) -> Result<T> {
    let mut child = V2StoreChild::acquire(root)?;
    let operation_result = operation(&child);
    let cleanup_result = child.cleanup();
    match operation_result {
        Err(error) => Err(error),
        Ok(value) => cleanup_result.map(|()| value),
    }
}

type SiteInvoke = fn(&mut V2Io<'_>, Option<FaultSelection>) -> Result<SiteResult>;

#[derive(Clone, Copy, Debug)]
struct SourceSite {
    id: FaultId,
    name: &'static str,
    descriptor: FaultDescriptor,
    crash_registered_for_g2: bool,
    invoke: SiteInvoke,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SiteResult {
    Success,
    Injected {
        mode: FaultMode,
        pressure: PressureKind,
        mutation: MutationWitness,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MutationWitness {
    Unchanged,
    Immediate {
        before: LogicalFileState,
        after: LogicalFileState,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LogicalFileState {
    logical_length: u64,
    sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FlowKind {
    P0P7Present,
    P0P7Absent,
    Rollback,
    EagerOpenClean,
    EagerOpenConvergence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FlowWitness {
    kind: FlowKind,
    trace: Vec<FaultId>,
    root: RootClassification,
}

struct V2Io<'a> {
    path: &'a Path,
    crash_ready: Option<&'a supervisor::WorkerReady>,
    occurrences: BTreeMap<FaultId, u32>,
    adopted: bool,
    inspection_published: bool,
    opened_lock: Option<File>,
}

impl<'a> V2Io<'a> {
    fn new(child: &'a V2StoreChild) -> Result<Self> {
        if !child.path().is_dir() {
            return Err(EvidenceError::UnsafeInventory);
        }
        Ok(Self {
            path: child.path(),
            crash_ready: None,
            occurrences: BTreeMap::new(),
            adopted: false,
            inspection_published: false,
            opened_lock: None,
        })
    }

    fn new_worker(
        target: &'a supervisor::ChildWorkerTarget,
        ready: &'a supervisor::WorkerReady,
    ) -> Result<Self> {
        if !target.path().is_dir() {
            return Err(EvidenceError::UnsafeInventory);
        }
        Ok(Self {
            path: target.path(),
            crash_ready: Some(ready),
            occurrences: BTreeMap::new(),
            adopted: false,
            inspection_published: false,
            opened_lock: None,
        })
    }

    fn execute(
        &mut self,
        site: SourceSite,
        selection: Option<FaultSelection>,
    ) -> Result<SiteResult> {
        validate_site(site)?;
        (site.invoke)(self, selection)
    }

    /// Executes the operation behind one macro-generated source function.
    ///
    /// The caller in `fault.rs` supplies a literal `FaultId` generated by the
    /// same declaration as the descriptor registry. No runtime-selected ID can
    /// enter this low-level operation boundary directly.
    fn execute_literal(
        &mut self,
        id: FaultId,
        selection: Option<FaultSelection>,
    ) -> Result<SiteResult> {
        let site = source_site_unchecked(id);
        let occurrence = self.occurrences.entry(site.id).or_insert(0);
        *occurrence = occurrence.checked_add(1).ok_or(EvidenceError::Bounds)?;
        if *occurrence > site.descriptor.maximum_occurrence {
            return Err(EvidenceError::Bounds);
        }
        let selected = selection
            .filter(|selected| selected.id == site.id && selected.occurrence.get() == *occurrence);
        if let Some(selected) = selected {
            if selected.mode == FaultMode::ChildCrashAfterSuccess {
                let ready = self.crash_ready.ok_or(EvidenceError::Replan)?;
                self.execute_success(site)?;
                return ready.publish_and_block(site.id);
            }
            if selected.mode == FaultMode::PreOperationError {
                return Ok(SiteResult::Injected {
                    mode: selected.mode,
                    pressure: selected.pressure,
                    mutation: MutationWitness::Unchanged,
                });
            }
            if selected.mode == FaultMode::ShortPartialWrite {
                return self.short_partial_write(site, selected.pressure);
            }
        }
        self.execute_success(site)?;
        Ok(SiteResult::Success)
    }

    fn execute_success(&mut self, site: SourceSite) -> Result<()> {
        let occurrence = self.occurrences[&site.id];
        let path = self.site_path(site, occurrence);
        match site.descriptor.operation {
            Operation::DirectoryOpen => {
                File::open(self.path).map_err(|_| EvidenceError::Io)?;
            }
            Operation::DirectoryRead => {
                let mut count = 0_usize;
                for entry in fs::read_dir(self.path).map_err(|_| EvidenceError::Io)? {
                    entry.map_err(|_| EvidenceError::Io)?;
                    count = count.checked_add(1).ok_or(EvidenceError::Bounds)?;
                    if count > inventory::MAX_V2_INVENTORY_ENTRIES {
                        return Err(EvidenceError::Bounds);
                    }
                }
            }
            Operation::FileOpen => {
                File::open(path).map_err(|_| EvidenceError::Io)?;
            }
            Operation::MetadataRead => {
                fs::metadata(path).map_err(|_| EvidenceError::Io)?;
            }
            Operation::BoundedRead => {
                bounded_read(&path)?;
            }
            Operation::CompleteValidation => {
                self.complete_validation(site, &path)?;
            }
            Operation::RelationValidation => {
                self.relation_validation(site)?;
            }
            Operation::CreateNew | Operation::LockCreate => {
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(path)
                    .map_err(|_| EvidenceError::Io)?;
            }
            Operation::Write => {
                let mut file = OpenOptions::new()
                    .write(true)
                    .open(path)
                    .map_err(|_| EvidenceError::Io)?;
                file.write_all(SITE_PAYLOAD)
                    .map_err(|_| EvidenceError::Io)?;
            }
            Operation::Synchronize => {
                let sync_path = if site.descriptor.artifact == Artifact::RootInventory {
                    self.path.to_path_buf()
                } else {
                    path
                };
                File::open(sync_path)
                    .and_then(|file| file.sync_all())
                    .map_err(|_| EvidenceError::Io)?;
            }
            Operation::Rename => {
                let source = self.rename_source(site)?;
                fs::rename(source, path).map_err(|_| EvidenceError::Io)?;
            }
            Operation::Remove => {
                fs::remove_file(path).map_err(|_| EvidenceError::Io)?;
            }
            Operation::Adopt => {
                self.adopted = true;
            }
            Operation::InspectionPublish => {
                if !self.adopted {
                    require_exact_fixture(&self.path.join("manifest-v2-slot-1.och"))?;
                    self.adopted = true;
                }
                self.inspection_published = true;
            }
            Operation::LockOpen => {
                self.opened_lock = Some(File::open(path).map_err(|_| EvidenceError::Io)?);
            }
            Operation::LockAcquire => {
                if self.opened_lock.is_none() {
                    self.opened_lock = Some(File::open(path).map_err(|_| EvidenceError::Io)?);
                }
                let file = self
                    .opened_lock
                    .as_ref()
                    .ok_or(EvidenceError::InvalidHarness)?;
                file.try_lock().map_err(|_| EvidenceError::Io)?;
            }
        }
        Ok(())
    }

    fn short_partial_write(
        &mut self,
        site: SourceSite,
        pressure: PressureKind,
    ) -> Result<SiteResult> {
        if site.descriptor.operation != Operation::Write || !site.descriptor.short_write {
            return Err(EvidenceError::InvalidHarness);
        }
        let occurrence = self.occurrences[&site.id];
        let path = self.site_path(site, occurrence);
        let before_bytes = bounded_read(&path)?;
        if before_bytes == SITE_PAYLOAD
            || before_bytes == SITE_PAYLOAD[..SHORT_WRITE_BYTES]
            || before_bytes.starts_with(&SITE_PAYLOAD[..SHORT_WRITE_BYTES])
        {
            return Err(EvidenceError::InvalidHarness);
        }
        let before = logical_file_state(&path)?;
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|_| EvidenceError::Io)?;
        file.write_all(&SITE_PAYLOAD[..SHORT_WRITE_BYTES])
            .map_err(|_| EvidenceError::Io)?;
        file.flush().map_err(|_| EvidenceError::Io)?;
        drop(file);
        let after_bytes = bounded_read(&path)?;
        let after = logical_file_state(&path)?;
        if SHORT_WRITE_BYTES == 0
            || SHORT_WRITE_BYTES >= SITE_PAYLOAD.len()
            || before == after
            || before_bytes == after_bytes
            || after_bytes == SITE_PAYLOAD
            || !after_bytes.starts_with(&SITE_PAYLOAD[..SHORT_WRITE_BYTES])
        {
            return Err(EvidenceError::InvalidHarness);
        }
        Ok(SiteResult::Injected {
            mode: FaultMode::ShortPartialWrite,
            pressure,
            mutation: MutationWitness::Immediate { before, after },
        })
    }

    fn complete_validation(&self, site: SourceSite, path: &Path) -> Result<()> {
        if site.descriptor.artifact == Artifact::RootInventory {
            let _ = fingerprint_path(self.path)?;
            return Ok(());
        }
        if is_optional_probe(site.id) && !path.exists() {
            return Ok(());
        }
        let bytes = bounded_read(path)?;
        if bytes.is_empty() || bytes.len() > MAX_SITE_BYTES {
            return Err(EvidenceError::InvalidHarness);
        }
        if site.descriptor.artifact == Artifact::Marker {
            let store_id: [u8; 16] = bytes
                .get(12..28)
                .ok_or(EvidenceError::InvalidHarness)?
                .try_into()
                .map_err(|_| EvidenceError::InvalidHarness)?;
            oracle::validate_marker(&bytes, store_id)?;
        } else if bytes != SITE_PAYLOAD {
            return Err(EvidenceError::InvalidHarness);
        }
        Ok(())
    }

    fn relation_validation(&self, site: SourceSite) -> Result<()> {
        if !self.path.join("store-format-v2.och").is_file() {
            return Err(EvidenceError::InvalidHarness);
        }
        match site.descriptor.artifact {
            Artifact::CatalogFinal => {
                require_exact_fixture(&self.path.join("generation-catalog-v2-slot-1.och"))
            }
            Artifact::ManifestFinal => {
                require_exact_fixture(&self.path.join("manifest-v2-slot-1.och"))
            }
            Artifact::StoreAuthority if self.inspection_published || self.adopted => {
                require_exact_fixture(&self.path.join("manifest-v2-slot-1.och"))
            }
            Artifact::StoreAuthority => Ok(()),
            _ => require_exact_fixture(&self.site_path(site, 1)),
        }
    }

    fn rename_source(&self, site: SourceSite) -> Result<PathBuf> {
        match site.descriptor.artifact {
            Artifact::RawFinal => Ok(self.path.join("sealed-journal-v1.staging")),
            Artifact::SegmentFinal => Ok(self.path.join("native-segment-v1.staging")),
            Artifact::CatalogFinal => Ok(self.path.join("generation-catalog-v2.staging")),
            Artifact::ManifestFinal => Ok(self.path.join("manifest-v2.staging")),
            _ => Err(EvidenceError::InvalidHarness),
        }
    }

    fn site_path(&self, site: SourceSite, occurrence: u32) -> PathBuf {
        artifact_path(self.path, site.id, site.descriptor.artifact, occurrence)
    }
}

fn source_sites() -> Vec<SourceSite> {
    FaultId::ALL
        .iter()
        .copied()
        .map(|id| SourceSite {
            id,
            name: id.source_symbol(),
            descriptor: id.descriptor(),
            crash_registered_for_g2: true,
            invoke: id.source_invoke(),
        })
        .collect()
}

fn classify(child: &V2StoreChild) -> Result<inventory::InventoryClass> {
    let directory = child.path();
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(inventory::MAX_V2_INVENTORY_ENTRIES + 1)
        .map_err(|_| EvidenceError::Bounds)?;
    for entry in fs::read_dir(directory).map_err(|_| EvidenceError::Io)? {
        let entry = entry.map_err(|_| EvidenceError::Io)?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| EvidenceError::UnsafeInventory)?;
        entries.push((
            name,
            entry.file_type().map_err(|_| EvidenceError::Io)?.is_file(),
        ));
        if entries.len() > inventory::MAX_V2_INVENTORY_ENTRIES {
            break;
        }
    }
    inventory::classify_entries(entries)
}

fn fingerprint(child: &V2StoreChild) -> Result<InventoryFingerprint> {
    fingerprint_path(child.path())
}

fn fingerprint_path(directory: &Path) -> Result<InventoryFingerprint> {
    let mut artifacts = Vec::new();
    artifacts
        .try_reserve_exact(inventory::MAX_V2_INVENTORY_ENTRIES)
        .map_err(|_| EvidenceError::Bounds)?;
    for entry in fs::read_dir(directory).map_err(|_| EvidenceError::Io)? {
        let entry = entry.map_err(|_| EvidenceError::Io)?;
        if artifacts.len() == inventory::MAX_V2_INVENTORY_ENTRIES {
            return Err(EvidenceError::Bounds);
        }
        if !entry.file_type().map_err(|_| EvidenceError::Io)?.is_file() {
            return Err(EvidenceError::UnsafeInventory);
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| EvidenceError::UnsafeInventory)?;
        let (digest, logical_length) = digest_file(&entry.path(), inventory::MAX_ARTIFACT_BYTES)?;
        artifacts.push(ArtifactFingerprint {
            name,
            kind: "FILE",
            logical_length,
            sha256: hex(&digest),
        });
    }
    inventory::finish_fingerprint(artifacts)
}

fn validate_compiled_registry_bijection() -> Result<()> {
    fault::validate_registry()?;
    let sites = source_sites();
    validate_compiled_site_inventory(&sites)
}

fn validate_compiled_site_inventory(sites: &[SourceSite]) -> Result<()> {
    let applicability = fault::applicability_rows()?;
    let registry = FaultId::ALL.iter().copied().collect::<BTreeSet<_>>();
    let source = sites.iter().map(|site| site.id).collect::<BTreeSet<_>>();
    if sites.len() != FaultId::ALL.len()
        || source.len() != sites.len()
        || source != registry
        || sites.iter().enumerate().any(|(index, site)| {
            sites[index + 1..]
                .iter()
                .any(|other| std::ptr::fn_addr_eq(site.invoke, other.invoke))
        })
        || applicability.iter().any(|row| !registry.contains(&row.id))
    {
        return Err(EvidenceError::InvalidHarness);
    }
    for site in sites {
        validate_site(*site)?;
    }
    Ok(())
}

fn validate_site(site: SourceSite) -> Result<()> {
    let descriptor = site.id.descriptor();
    if site.name != site.id.source_symbol()
        || site.descriptor != descriptor
        || !site.crash_registered_for_g2
        || !std::ptr::fn_addr_eq(site.invoke, site.id.source_invoke())
        || descriptor.id != site.id
        || descriptor.maximum_occurrence == 0
        || descriptor.phase.as_str().is_empty()
        || descriptor.artifact.as_str().is_empty()
        || descriptor.operation.as_str().is_empty()
        || descriptor.commit_side.as_str().is_empty()
        || descriptor.expected_root.as_str().is_empty()
        || descriptor
            .terminals
            .iter()
            .any(|terminal| terminal.as_str().is_empty())
        || (descriptor.short_write && descriptor.operation != Operation::Write)
        || (descriptor.pressure && !descriptor.mutation)
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

fn prepare_site(child: &V2StoreChild, site: SourceSite) -> Result<InventoryFingerprint> {
    clear_root(child)?;
    write_baseline(child.path(), site, true)?;
    let target = artifact_path(child.path(), site.id, site.descriptor.artifact, 1);
    match site.descriptor.operation {
        Operation::CreateNew | Operation::LockCreate => remove_if_present(&target)?,
        Operation::Rename => {
            remove_if_present(&target)?;
            let source = rename_source_path(child.path(), site.descriptor.artifact)?;
            write_file(&source, SITE_PAYLOAD)?;
        }
        _ => {}
    }
    fingerprint(child)
}

fn restore_site(child: &V2StoreChild, site: SourceSite) -> Result<InventoryFingerprint> {
    prepare_site(child, site)
}

fn run_flow(child: &V2StoreChild, kind: FlowKind) -> Result<FlowWitness> {
    clear_root(child)?;
    match kind {
        FlowKind::P0P7Present | FlowKind::P0P7Absent => prepare_prior(child.path(), false)?,
        FlowKind::Rollback => prepare_prior(child.path(), true)?,
        FlowKind::EagerOpenClean => prepare_eager(child.path(), false)?,
        FlowKind::EagerOpenConvergence => prepare_eager(child.path(), true)?,
    }
    if classify(child)? != inventory::InventoryClass::ReviewedV2 {
        return Err(EvidenceError::InvalidHarness);
    }
    let ids = flow_ids(kind);
    let mut io = V2Io::new(child)?;
    let mut trace = Vec::new();
    for id in ids {
        if kind == FlowKind::P0P7Present && id == FaultId::P7RawStagingOpen {
            prepare_optional_cleanup(child.path())?;
        }
        let site = source_site(id)?;
        let result = io.execute(site, None)?;
        if result != SiteResult::Success {
            return Err(EvidenceError::InvalidHarness);
        }
        push_legal(&mut trace, id)?;
    }
    let root_class = match kind {
        FlowKind::Rollback => RootClassification::Prior,
        FlowKind::P0P7Present
        | FlowKind::P0P7Absent
        | FlowKind::EagerOpenClean
        | FlowKind::EagerOpenConvergence => RootClassification::Committed,
    };
    validate_terminal(&trace, root_class)?;
    Ok(FlowWitness {
        kind,
        trace,
        root: root_class,
    })
}

fn exercise_all_sites(child: &V2StoreChild) -> Result<usize> {
    let mut executions = 0_usize;
    for site in source_sites() {
        let before = prepare_site(child, site)?;
        let mut io = V2Io::new(child)?;
        if io.execute(site, None)? != SiteResult::Success {
            return Err(EvidenceError::InvalidHarness);
        }
        executions = executions.checked_add(1).ok_or(EvidenceError::Bounds)?;

        let restored = restore_site(child, site)?;
        if before != restored {
            return Err(EvidenceError::InvalidHarness);
        }
        let selection = FaultSelection::new(
            site.id,
            NonZeroU32::MIN,
            FaultMode::PreOperationError,
            PressureKind::None,
        )?;
        let mut io = V2Io::new(child)?;
        if !matches!(
            io.execute(site, Some(selection))?,
            SiteResult::Injected {
                mutation: MutationWitness::Unchanged,
                ..
            }
        ) || fingerprint(child)? != restored
        {
            return Err(EvidenceError::InvalidHarness);
        }
        executions = executions.checked_add(1).ok_or(EvidenceError::Bounds)?;

        if site.descriptor.short_write {
            exercise_short_write(child, site, PressureKind::None)?;
            executions = executions.checked_add(1).ok_or(EvidenceError::Bounds)?;
        }
        if site.descriptor.pressure {
            for pressure in [PressureKind::StorageFull, PressureKind::QuotaExceeded] {
                let baseline = restore_site(child, site)?;
                let selection = FaultSelection::new(
                    site.id,
                    NonZeroU32::MIN,
                    FaultMode::PreOperationError,
                    pressure,
                )?;
                let mut io = V2Io::new(child)?;
                if !matches!(
                    io.execute(site, Some(selection))?,
                    SiteResult::Injected {
                        pressure: actual,
                        mutation: MutationWitness::Unchanged,
                        ..
                    } if actual == pressure
                ) || fingerprint(child)? != baseline
                {
                    return Err(EvidenceError::InvalidHarness);
                }
                executions = executions.checked_add(1).ok_or(EvidenceError::Bounds)?;
                if site.descriptor.short_write {
                    exercise_short_write(child, site, pressure)?;
                    executions = executions.checked_add(1).ok_or(EvidenceError::Bounds)?;
                }
            }
        }
    }
    clear_root(child)?;
    Ok(executions)
}

fn exercise_short_write(
    child: &V2StoreChild,
    site: SourceSite,
    pressure: PressureKind,
) -> Result<()> {
    let baseline = restore_site(child, site)?;
    let selection = FaultSelection::new(
        site.id,
        NonZeroU32::MIN,
        FaultMode::ShortPartialWrite,
        pressure,
    )?;
    let mut io = V2Io::new(child)?;
    let result = io.execute(site, Some(selection))?;
    let immediate = fingerprint(child)?;
    if !matches!(
        result,
        SiteResult::Injected {
            pressure: actual,
            mutation: MutationWitness::Immediate { before, after },
            ..
        } if actual == pressure && before != after
    ) || immediate == baseline
    {
        return Err(EvidenceError::InvalidHarness);
    }
    let restored = restore_site(child, site)?;
    if restored != baseline {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

fn clear_root(child: &V2StoreChild) -> Result<()> {
    let root = child.path();
    for entry in fs::read_dir(root).map_err(|_| EvidenceError::Io)? {
        let entry = entry.map_err(|_| EvidenceError::Io)?;
        if !entry.file_type().map_err(|_| EvidenceError::Io)?.is_file() {
            return Err(EvidenceError::UnsafeInventory);
        }
        fs::remove_file(entry.path()).map_err(|_| EvidenceError::Io)?;
    }
    File::open(root)
        .and_then(|file| file.sync_all())
        .map_err(|_| EvidenceError::Io)
}

fn source_site(id: FaultId) -> Result<SourceSite> {
    let site = source_site_unchecked(id);
    validate_site(site)?;
    Ok(site)
}

fn source_site_unchecked(id: FaultId) -> SourceSite {
    SourceSite {
        id,
        name: id.source_symbol(),
        descriptor: id.descriptor(),
        crash_registered_for_g2: true,
        invoke: id.source_invoke(),
    }
}

fn prepare_prior(root: &Path, rollback_derivatives: bool) -> Result<()> {
    write_file(&root.join("store-format-v2.och"), &oracle::marker([7; 16]))?;
    for name in [
        "active-journal-v1.och",
        "active-journal-v1.checkpoint",
        "series-registry-v1-slot-0.och",
        "retry-state-v1-slot-0.och",
        "recovery-state-v1-slot-0.och",
        "generation-catalog-v2-slot-0.och",
        "manifest-v2-slot-0.och",
        "sealed-journal-v1-g00000000000000000001.och",
        "native-segment-v1-g00000000000000000001.och",
    ] {
        write_file(&root.join(name), SITE_PAYLOAD)?;
    }
    if rollback_derivatives {
        prepare_rollback_derivatives(root)?;
    }
    Ok(())
}

fn prepare_rollback_derivatives(root: &Path) -> Result<()> {
    for name in [
        "journal-rotation-v2.intent",
        "sealed-journal-v1.staging",
        "sealed-journal-v1-g00000000000000000002.och",
        "native-segment-v1.staging",
        "native-segment-v1-g00000000000000000002.och",
        "active-journal-v1-g00000000000000000002.och",
        "active-journal-v1-g00000000000000000002.checkpoint",
        "generation-catalog-v2.staging",
        "generation-catalog-v2-slot-1.och",
        "manifest-v2.staging",
        "manifest-v2-slot-1.och",
    ] {
        write_file(&root.join(name), SITE_PAYLOAD)?;
    }
    Ok(())
}

fn prepare_eager(root: &Path, convergence: bool) -> Result<()> {
    write_file(&root.join("store-format-v2.och"), &oracle::marker([9; 16]))?;
    for name in [
        "active-journal-v1.och",
        "active-journal-v1.checkpoint",
        "active-journal-v1-g00000000000000000002.och",
        "active-journal-v1-g00000000000000000002.checkpoint",
        "series-registry-v1-slot-0.och",
        "retry-state-v1-slot-0.och",
        "recovery-state-v1-slot-0.och",
        "generation-catalog-v2-slot-0.och",
        "manifest-v2-slot-0.och",
        "manifest-v2-slot-1.och",
    ] {
        write_file(&root.join(name), SITE_PAYLOAD)?;
    }
    for generation in 1..=64_u64 {
        write_file(
            &root.join(format!("sealed-journal-v1-g{generation:020}.och")),
            SITE_PAYLOAD,
        )?;
        write_file(
            &root.join(format!("native-segment-v1-g{generation:020}.och")),
            SITE_PAYLOAD,
        )?;
    }
    if convergence {
        write_file(&root.join("journal-rotation-v2.intent"), SITE_PAYLOAD)?;
    }
    Ok(())
}

fn write_baseline(root: &Path, site: SourceSite, include_optional: bool) -> Result<()> {
    write_file(&root.join("store-format-v2.och"), &oracle::marker([5; 16]))?;
    let target = artifact_path(root, site.id, site.descriptor.artifact, 1);
    if target != root
        && target != root.join("store-format-v2.och")
        && !matches!(
            site.descriptor.operation,
            Operation::CreateNew | Operation::LockCreate
        )
        && (include_optional || !is_optional_probe(site.id))
    {
        let bytes = if site.descriptor.short_write {
            SHORT_WRITE_BASELINE
        } else {
            SITE_PAYLOAD
        };
        write_file(&target, bytes)?;
    }
    if matches!(
        site.descriptor.operation,
        Operation::RelationValidation | Operation::InspectionPublish
    ) && !root.join("manifest-v2-slot-1.och").exists()
    {
        write_file(&root.join("manifest-v2-slot-1.och"), SITE_PAYLOAD)?;
    }
    if site.descriptor.artifact == Artifact::CatalogFinal
        && !root.join("generation-catalog-v2-slot-1.och").exists()
    {
        write_file(&root.join("generation-catalog-v2-slot-1.och"), SITE_PAYLOAD)?;
    }
    Ok(())
}

fn prepare_optional_cleanup(root: &Path) -> Result<()> {
    for name in [
        "sealed-journal-v1.staging",
        "native-segment-v1.staging",
        "generation-catalog-v2.staging",
        "manifest-v2.staging",
    ] {
        write_file(&root.join(name), SITE_PAYLOAD)?;
    }
    Ok(())
}

fn flow_ids(kind: FlowKind) -> Vec<FaultId> {
    match kind {
        FlowKind::P0P7Present | FlowKind::P0P7Absent => FaultId::ALL
            .iter()
            .copied()
            .filter(|id| {
                matches!(
                    id.descriptor().phase,
                    PhaseId::Preflight
                        | PhaseId::Intent
                        | PhaseId::Raw
                        | PhaseId::Segment
                        | PhaseId::Successor
                        | PhaseId::Catalog
                        | PhaseId::Manifest
                        | PhaseId::AdoptClean
                )
            })
            .filter(|id| kind == FlowKind::P0P7Present || !is_optional_remove_or_sync(*id))
            .collect(),
        FlowKind::Rollback => rollback_ids(),
        FlowKind::EagerOpenClean => eager_ids(false),
        FlowKind::EagerOpenConvergence => eager_ids(true),
    }
}

fn rollback_ids() -> Vec<FaultId> {
    use FaultId as F;
    let mut ids = Vec::new();
    for group in [
        [F::RbRawValidate, F::RbRawRemove, F::RbRawSync],
        [F::RbSegmentValidate, F::RbSegmentRemove, F::RbSegmentSync],
        [
            F::RbSuccessorValidate,
            F::RbSuccessorRemove,
            F::RbSuccessorSync,
        ],
        [F::RbCatalogValidate, F::RbCatalogRemove, F::RbCatalogSync],
        [
            F::RbManifestValidate,
            F::RbManifestRemove,
            F::RbManifestSync,
        ],
    ] {
        ids.extend(group);
        ids.extend(group);
    }
    ids.extend([
        F::RbInventoryRead,
        F::RbInventoryValidate,
        F::RbIntentRemove,
        F::RbFinalSync,
    ]);
    ids
}

fn eager_ids(convergence: bool) -> Vec<FaultId> {
    use FaultId as F;
    let mut ids = vec![
        F::OpenDirectoryOpen,
        F::OpenDirectoryRead,
        F::OpenMarkerOpen,
        F::OpenMarkerMetadata,
        F::OpenMarkerRead,
        F::OpenMarkerValidate,
        F::OpenManifestOpen,
        F::OpenManifestRead,
        F::OpenManifestValidate,
        F::OpenAuthorityFamilies,
        F::OpenActiveValidate,
        F::OpenCatalogValidate,
        F::OpenIntentStagingValidate,
    ];
    if convergence {
        ids.extend([
            F::OpenConvergenceRemove,
            F::OpenConvergenceSync,
            F::OpenIntentStagingValidate,
        ]);
    }
    for _ in 0..64 {
        ids.extend([
            F::OpenPairRawOpen,
            F::OpenPairRawMetadata,
            F::OpenPairRawRead,
            F::OpenPairRawValidate,
            F::OpenPairSegmentOpen,
            F::OpenPairSegmentMetadata,
            F::OpenPairSegmentRead,
            F::OpenPairSegmentValidate,
            F::OpenPairRelation,
        ]);
    }
    ids.extend([
        F::OpenLockCreate,
        F::OpenLockOpen,
        F::OpenLockAcquire,
        F::OpenFinalRelation,
        F::OpenAdopt,
    ]);
    ids
}

fn push_legal(trace: &mut Vec<FaultId>, id: FaultId) -> Result<()> {
    if let Some(previous) = trace.last().copied()
        && previous != id
        && !previous.descriptor().successors.contains(&id)
    {
        return Err(EvidenceError::InvalidHarness);
    }
    trace.push(id);
    Ok(())
}

fn validate_terminal(trace: &[FaultId], root: RootClassification) -> Result<()> {
    let last = trace.last().ok_or(EvidenceError::InvalidHarness)?;
    let terminal_ok = match root {
        RootClassification::Prior => last
            .descriptor()
            .terminals
            .contains(&fault::TerminalState::PriorRollback),
        RootClassification::Committed => last
            .descriptor()
            .terminals
            .contains(&fault::TerminalState::CompleteSuccess),
        RootClassification::UnchangedRefusal => false,
    };
    if terminal_ok {
        Ok(())
    } else {
        Err(EvidenceError::InvalidHarness)
    }
}

fn artifact_path(root: &Path, id: FaultId, artifact: Artifact, occurrence: u32) -> PathBuf {
    let slot = usize::try_from((occurrence - 1) % 3).expect("bounded slot");
    let generation = u64::from(((occurrence - 1) % 64) + 1);
    match artifact {
        Artifact::RootInventory => root.to_path_buf(),
        Artifact::Marker => root.join("store-format-v2.och"),
        Artifact::Manifest => root.join(format!("manifest-v2-slot-{}.och", slot % 2)),
        Artifact::ActiveJournal => root.join("active-journal-v1.och"),
        Artifact::Checkpoint => root.join("active-journal-v1.checkpoint"),
        Artifact::Registry => root.join(format!("series-registry-v1-slot-{slot}.och")),
        Artifact::Retry => root.join(format!("retry-state-v1-slot-{slot}.och")),
        Artifact::Recovery => root.join(format!("recovery-state-v1-slot-{slot}.och")),
        Artifact::Catalog => root.join(format!("generation-catalog-v2-slot-{slot}.och")),
        Artifact::RawPair => root.join(format!("sealed-journal-v1-g{generation:020}.och")),
        Artifact::SegmentPair => root.join(format!("native-segment-v1-g{generation:020}.och")),
        Artifact::Intent => root.join("journal-rotation-v2.intent"),
        Artifact::RawStaging => {
            if id.descriptor().phase == PhaseId::Rollback && occurrence == 2 {
                root.join("sealed-journal-v1-g00000000000000000002.och")
            } else {
                root.join("sealed-journal-v1.staging")
            }
        }
        Artifact::RawFinal => root.join("sealed-journal-v1-g00000000000000000002.och"),
        Artifact::SegmentStaging => {
            if id.descriptor().phase == PhaseId::Rollback && occurrence == 2 {
                root.join("native-segment-v1-g00000000000000000002.och")
            } else {
                root.join("native-segment-v1.staging")
            }
        }
        Artifact::SegmentFinal => root.join("native-segment-v1-g00000000000000000002.och"),
        Artifact::SuccessorJournal => {
            if id.descriptor().phase == PhaseId::Rollback && occurrence == 2 {
                root.join("active-journal-v1-g00000000000000000002.checkpoint")
            } else {
                root.join("active-journal-v1-g00000000000000000002.och")
            }
        }
        Artifact::SuccessorCheckpoint => {
            root.join("active-journal-v1-g00000000000000000002.checkpoint")
        }
        Artifact::CatalogStaging => {
            if id.descriptor().phase == PhaseId::Rollback && occurrence == 2 {
                root.join("generation-catalog-v2-slot-1.och")
            } else {
                root.join("generation-catalog-v2.staging")
            }
        }
        Artifact::CatalogFinal => root.join("generation-catalog-v2-slot-1.och"),
        Artifact::ManifestStaging => {
            if id.descriptor().phase == PhaseId::Rollback && occurrence == 2 {
                root.join("manifest-v2-slot-1.och")
            } else {
                root.join("manifest-v2.staging")
            }
        }
        Artifact::ManifestFinal | Artifact::StoreAuthority => root.join("manifest-v2-slot-1.och"),
        Artifact::StableLock => root.join("store-v1.lock"),
    }
}

fn rename_source_path(root: &Path, artifact: Artifact) -> Result<PathBuf> {
    match artifact {
        Artifact::RawFinal => Ok(root.join("sealed-journal-v1.staging")),
        Artifact::SegmentFinal => Ok(root.join("native-segment-v1.staging")),
        Artifact::CatalogFinal => Ok(root.join("generation-catalog-v2.staging")),
        Artifact::ManifestFinal => Ok(root.join("manifest-v2.staging")),
        _ => Err(EvidenceError::InvalidHarness),
    }
}

fn is_optional_probe(id: FaultId) -> bool {
    matches!(
        id,
        FaultId::P7RawStagingOpen
            | FaultId::P7SegmentStagingOpen
            | FaultId::P7CatalogStagingOpen
            | FaultId::P7ManifestStagingOpen
            | FaultId::OpenIntentStagingValidate
    )
}

fn is_optional_remove_or_sync(id: FaultId) -> bool {
    matches!(
        id,
        FaultId::P7RawStagingRemove
            | FaultId::P7RawStagingSync
            | FaultId::P7SegmentStagingRemove
            | FaultId::P7SegmentStagingSync
            | FaultId::P7CatalogStagingRemove
            | FaultId::P7CatalogStagingSync
            | FaultId::P7ManifestStagingRemove
            | FaultId::P7ManifestStagingSync
    )
}

fn bounded_read(path: &Path) -> Result<Vec<u8>> {
    let mut file = File::open(path).map_err(|_| EvidenceError::Io)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(MAX_SITE_BYTES)
        .map_err(|_| EvidenceError::Bounds)?;
    let mut scratch = [0_u8; 1_024];
    loop {
        let read = file.read(&mut scratch).map_err(|_| EvidenceError::Io)?;
        if read == 0 {
            break;
        }
        if bytes.len().checked_add(read).ok_or(EvidenceError::Bounds)? > MAX_SITE_BYTES {
            return Err(EvidenceError::Bounds);
        }
        bytes.extend_from_slice(&scratch[..read]);
    }
    Ok(bytes)
}

fn digest_file(path: &Path, maximum: u64) -> Result<([u8; 32], u64)> {
    let mut file = File::open(path).map_err(|_| EvidenceError::Io)?;
    let metadata = file.metadata().map_err(|_| EvidenceError::Io)?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(EvidenceError::Bounds);
    }
    let mut hash = Sha256::new();
    let mut total = 0_u64;
    let mut scratch = vec![0_u8; 64 * 1_024].into_boxed_slice();
    loop {
        let read = file.read(&mut scratch).map_err(|_| EvidenceError::Io)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(|_| EvidenceError::Bounds)?)
            .ok_or(EvidenceError::Bounds)?;
        if total > maximum {
            return Err(EvidenceError::Bounds);
        }
        hash.update(&scratch[..read])?;
    }
    if total != metadata.len() {
        return Err(EvidenceError::Io);
    }
    Ok((hash.finish()?, total))
}

fn logical_file_state(path: &Path) -> Result<LogicalFileState> {
    let (sha256, logical_length) = digest_file(path, MAX_SITE_BYTES as u64)?;
    Ok(LogicalFileState {
        logical_length,
        sha256,
    })
}

fn require_exact_fixture(path: &Path) -> Result<()> {
    if bounded_read(path)? == SITE_PAYLOAD {
        Ok(())
    } else {
        Err(EvidenceError::InvalidHarness)
    }
}

fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|_| EvidenceError::Io)?;
    file.write_all(bytes).map_err(|_| EvidenceError::Io)?;
    file.sync_all().map_err(|_| EvidenceError::Io)
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(EvidenceError::Io),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    struct Temp {
        parent: PathBuf,
        root: EvidenceRoot,
    }

    impl Temp {
        fn new(label: &str) -> Self {
            let parent = std::env::temp_dir().join(format!(
                "och-v2-g1-io-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&parent);
            fs::create_dir(&parent).expect("create g1 I/O parent");
            let root =
                EvidenceRoot::prepare(&parent.join("evidence")).expect("prepare g1 evidence root");
            root.foundation_layout().expect("prepare g1 root layout");
            Self { parent, root }
        }

        fn run<T>(&self, operation: impl FnOnce(&V2StoreChild) -> Result<T>) -> Result<T> {
            run_child(&self.root, operation)
        }

        fn child_exists(&self) -> bool {
            self.parent
                .join("evidence/cases")
                .join(V2_CHILD_NAME)
                .exists()
        }
    }

    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.parent);
        }
    }

    #[test]
    fn descriptor_source_bijection_and_metadata_are_exact() {
        validate_compiled_registry_bijection().expect("compiled registry bijection");
        let sites = source_sites();
        assert_eq!(sites.len(), 173);
        assert_eq!(sites.len(), FaultId::ALL.len());
        for site in sites {
            assert_eq!(site.descriptor, site.id.descriptor());
            assert!(site.crash_registered_for_g2);
        }
    }

    #[test]
    fn duplicate_missing_extra_and_wrong_metadata_source_rows_refuse() {
        let sites = source_sites();
        let mut missing = sites.clone();
        missing.pop();
        assert!(validate_compiled_site_inventory(&missing).is_err());

        let mut duplicate = sites.clone();
        duplicate[1] = duplicate[0];
        assert!(validate_compiled_site_inventory(&duplicate).is_err());

        let mut extra = sites.clone();
        extra.push(sites[0]);
        assert!(validate_compiled_site_inventory(&extra).is_err());

        let mut wrong = sites;
        wrong[0].descriptor.operation = Operation::Write;
        assert!(validate_compiled_site_inventory(&wrong).is_err());

        let mut wrong_invoke = source_sites();
        wrong_invoke[0].invoke = wrong_invoke[1].invoke;
        assert!(validate_compiled_site_inventory(&wrong_invoke).is_err());
    }

    #[test]
    fn every_site_executes_success_and_every_g1_injection_mode() {
        let temp = Temp::new("all-sites");
        let executions = temp
            .run(exercise_all_sites)
            .expect("exercise all source sites");
        assert!(executions > FaultId::ALL.len() * 2);
        assert!(!temp.child_exists());
    }

    #[test]
    fn every_short_write_site_proves_immediate_change_then_exact_restoration() {
        let temp = Temp::new("short-writes");
        let sites = source_sites()
            .into_iter()
            .filter(|site| site.descriptor.short_write)
            .collect::<Vec<_>>();
        assert_eq!(sites.len(), 7);
        temp.run(|child| {
            for site in sites {
                for pressure in [
                    PressureKind::None,
                    PressureKind::StorageFull,
                    PressureKind::QuotaExceeded,
                ] {
                    exercise_short_write(child, site, pressure)?;
                }
            }
            clear_root(child)
        })
        .expect("immediate mutation and restoration");
    }

    #[test]
    fn p0_p7_present_absent_rollback_and_64_pair_eager_flows_are_legal() {
        let temp = Temp::new("flows");
        temp.run(|child| {
            for kind in [
                FlowKind::P0P7Present,
                FlowKind::P0P7Absent,
                FlowKind::Rollback,
                FlowKind::EagerOpenClean,
                FlowKind::EagerOpenConvergence,
            ] {
                let witness = run_flow(child, kind)?;
                assert_eq!(witness.kind, kind);
                assert!(!witness.trace.is_empty());
            }
            clear_root(child)
        })
        .expect("execute legal flows");
    }

    #[test]
    fn capability_cleanup_precedence_and_same_name_retry_are_exact() {
        let temp = Temp::new("capability-cleanup");
        assert!(std::mem::needs_drop::<V2StoreChild>());

        assert!(matches!(
            temp.run(|_| Err::<(), _>(EvidenceError::InvalidHarness)),
            Err(EvidenceError::InvalidHarness)
        ));
        assert!(!temp.child_exists());
        assert_eq!(temp.run(|_| Ok(7_u8)).expect("same-name retry"), 7);
        assert!(!temp.child_exists());

        let cleanup_error = temp.run(|child| {
            fs::remove_dir(child.path()).expect("remove empty child before cleanup");
            Ok(())
        });
        assert!(matches!(cleanup_error, Err(EvidenceError::Io)));

        let combined_error = temp.run(|child| {
            fs::remove_dir(child.path()).expect("remove retry child before cleanup");
            Err::<(), _>(EvidenceError::Replan)
        });
        assert!(matches!(combined_error, Err(EvidenceError::Replan)));
        assert!(!temp.child_exists());
    }
}
