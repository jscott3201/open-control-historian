use super::fault::{CommitSide, FaultId, FaultSelection, TerminalState};
use super::inventory::{InventoryClass, InventoryFingerprint};
use super::schema::{FaultMode, PhaseId, PressureKind, RootClassification};
use super::{
    FlowKind, SITE_PAYLOAD, SiteResult, V2_CHILD_NAME, V2Io, V2StoreChild, bounded_read, classify,
    clear_root, fingerprint, flow_ids, prepare_eager, prepare_optional_cleanup, prepare_prior,
    prepare_rollback_derivatives, source_site, validate_direct_child_directory,
};
use crate::error::{EvidenceError, Result};
use crate::root::EvidenceRoot;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::Duration;

const CHILD_SCHEMA: &str = "m03-pr03g2-child-v1";
const READY_WAIT_POLLS: usize = 10_000;
const READY_WAIT_INTERVAL: Duration = Duration::from_millis(1);
const REAP_WAIT_POLLS: usize = 10_000;
const WAIT_RETRIES: usize = 3;
const MAX_CONTROL_BYTES: usize = 4_096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CrashWitness {
    pub(super) id: FaultId,
    pub(super) pre_fingerprint: String,
    pub(super) immediate_fingerprint: String,
    pub(super) reopen_fingerprint: String,
    pub(super) final_fingerprint: String,
    pub(super) root: RootClassification,
    pub(super) terminal: TerminalState,
    pub(super) ready_validated: bool,
    pub(super) reaped: bool,
    pub(super) cleanup_attempts: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashFlow {
    Transaction,
    Rollback,
    EagerOpen,
}

struct CrashPlan {
    kind: CrashFlow,
    flow: Vec<FaultId>,
    selected_index: usize,
    prior: InventoryFingerprint,
    selected_pre: InventoryFingerprint,
}

pub(super) struct ChildWorkerTarget {
    path: PathBuf,
}

impl ChildWorkerTarget {
    fn open(root: &EvidenceRoot) -> Result<Self> {
        let cases = root.direct_directory("cases")?;
        let path = validate_direct_child_directory(&cases, V2_CHILD_NAME)?;
        if root.direct_directory("cases")? != cases {
            return Err(EvidenceError::UnsafeInventory);
        }
        Ok(Self { path })
    }

    fn revalidate(&self, root: &EvidenceRoot) -> Result<()> {
        let cases = root.direct_directory("cases")?;
        if validate_direct_child_directory(&cases, V2_CHILD_NAME)? != self.path {
            return Err(EvidenceError::UnsafeInventory);
        }
        Ok(())
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

pub(super) struct WorkerReady {
    path: PathBuf,
    staging: PathBuf,
    control: PathBuf,
    token: String,
    parent_pid: u32,
}

impl WorkerReady {
    pub(super) fn publish_and_block<T>(&self, id: FaultId) -> Result<T> {
        validate_control_directory(&self.control)?;
        ensure_direct_path_absent(&self.staging, &self.control)?;
        ensure_direct_path_absent(&self.path, &self.control)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&self.staging)
            .map_err(|_| EvidenceError::Io)?;
        let bytes = ready_bytes(&self.token, self.parent_pid, std::process::id(), id);
        file.write_all(bytes.as_bytes())
            .and_then(|()| file.sync_all())
            .map_err(|_| EvidenceError::Io)?;
        validate_direct_file(&self.staging, &self.control)?;
        ensure_direct_path_absent(&self.path, &self.control)?;
        fs::rename(&self.staging, &self.path).map_err(|_| EvidenceError::Io)?;
        validate_direct_file(&self.path, &self.control)?;
        validate_control_directory(&self.control)?;
        File::open(&self.control)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| EvidenceError::Io)?;
        loop {
            thread::park_timeout(Duration::from_secs(3_600));
        }
    }
}

pub(super) fn run_campaign(root: &EvidenceRoot) -> Result<Vec<CrashWitness>> {
    let control = prepare_control(root)?;
    let mut witnesses = Vec::new();
    witnesses
        .try_reserve_exact(FaultId::ALL.len())
        .map_err(|_| EvidenceError::Bounds)?;
    for (index, id) in FaultId::ALL.iter().copied().enumerate() {
        witnesses.push(run_target(root, &control, index, id)?);
    }
    if witnesses.len() != FaultId::ALL.len()
        || witnesses.iter().any(|witness| {
            !witness.ready_validated || !witness.reaped || witness.cleanup_attempts != 1
        })
        || fs::read_dir(&control)
            .map_err(|_| EvidenceError::Io)?
            .next()
            .transpose()
            .map_err(|_| EvidenceError::Io)?
            .is_some()
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(witnesses)
}

fn run_target(
    root: &EvidenceRoot,
    control: &Path,
    index: usize,
    id: FaultId,
) -> Result<CrashWitness> {
    let token = format!("CRASH-{index:03}-{}", std::process::id());
    let request = control.join(format!("{token}.request"));
    let ready = control.join(format!("{token}.ready"));
    let ready_staging = control.join(format!("{token}.ready.staging"));
    let mut child = V2StoreChild::acquire(root)?;
    let mut spawned = false;
    let mut reaped = true;
    let operation_result = (|| {
        let site = source_site(id)?;
        child.revalidate()?;
        let plan = prepare_crash_plan(&child, id)?;
        write_request(&request, control, &token, id)?;
        let mut process = spawn_worker(root, &token, id)?;
        spawned = true;
        reaped = false;
        let supervision = supervise_worker(&mut process, &ready, &token, id)?;
        reaped = true;
        if supervision.termination_errors > REAP_WAIT_POLLS + WAIT_RETRIES {
            return Err(EvidenceError::InvalidHarness);
        }
        supervision.observation?;
        child.revalidate()?;
        let immediate = fingerprint(&child)?;
        if classify(&child)? != InventoryClass::ReviewedV2 {
            return Err(EvidenceError::InvalidHarness);
        }
        let protected = match plan.kind {
            CrashFlow::Transaction if site.descriptor.commit_side == CommitSide::Precommit => {
                protected_committed_finals(&plan.prior)
            }
            CrashFlow::Rollback => protected_committed_finals(&plan.prior),
            CrashFlow::Transaction | CrashFlow::EagerOpen => protected_committed_finals(&immediate),
        };
        child.revalidate()?;
        let terminal = reopen_and_converge(&child, &plan, site.descriptor.commit_side)?;
        let reopened = fingerprint(&child)?;
        let final_fingerprint = fingerprint(&child)?;
        validate_protected_finals(&protected, &final_fingerprint)?;
        let witness = CrashWitness {
            id,
            pre_fingerprint: plan.selected_pre.aggregate_sha256,
            immediate_fingerprint: immediate.aggregate_sha256,
            reopen_fingerprint: reopened.aggregate_sha256,
            final_fingerprint: final_fingerprint.aggregate_sha256,
            root: site.descriptor.expected_root,
            terminal,
            ready_validated: true,
            reaped: true,
            cleanup_attempts: 1,
        };
        validate_witness(&witness, site.descriptor.commit_side)?;
        Ok(witness)
    })();
    if spawned && !reaped {
        child.retain_after_unreaped_child();
        return Err(EvidenceError::Replan);
    }
    let cleanup_result = child.cleanup();
    let control_cleanup = cleanup_control(control, [&request, &ready, &ready_staging]);
    match operation_result {
        Err(error) => Err(error),
        Ok(witness) => cleanup_result.and(control_cleanup).map(|()| witness),
    }
}

fn validate_witness(witness: &CrashWitness, side: CommitSide) -> Result<()> {
    let expected_root = witness.id.descriptor().expected_root;
    if witness.pre_fingerprint.len() != 64
        || witness.immediate_fingerprint.len() != 64
        || witness.reopen_fingerprint.len() != 64
        || witness.final_fingerprint.len() != 64
        || witness.root != expected_root
        || witness.terminal != structural_terminal(witness.id)
        || !terminal_reachable(witness.id, witness.terminal)
        || witness.cleanup_attempts != 1
        || !witness.ready_validated
        || !witness.reaped
        || (side == CommitSide::Postcommit && witness.root != RootClassification::Committed)
        || (side == CommitSide::RenameBoundary && witness.root != RootClassification::Committed)
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

fn prepare_control(root: &EvidenceRoot) -> Result<PathBuf> {
    let control = root.direct_directory("control")?;
    if fs::read_dir(&control)
        .map_err(|_| EvidenceError::Io)?
        .next()
        .transpose()
        .map_err(|_| EvidenceError::Io)?
        .is_some()
    {
        return Err(EvidenceError::UnsafeInventory);
    }
    if root.direct_directory("control")? != control {
        return Err(EvidenceError::UnsafeInventory);
    }
    Ok(control)
}

fn validate_control_directory(control: &Path) -> Result<()> {
    let root = control.parent().ok_or(EvidenceError::UnsafeInventory)?;
    for directory in [root, control] {
        let metadata = fs::symlink_metadata(directory).map_err(|_| EvidenceError::Io)?;
        if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
            return Err(EvidenceError::UnsafeInventory);
        }
        if fs::canonicalize(directory).map_err(|_| EvidenceError::Io)? != directory {
            return Err(EvidenceError::UnsafeInventory);
        }
    }
    if control.parent() != Some(root) {
        return Err(EvidenceError::UnsafeInventory);
    }
    Ok(())
}

fn ensure_direct_path_absent(path: &Path, parent: &Path) -> Result<()> {
    validate_control_directory(parent)?;
    if path.parent() != Some(parent) {
        return Err(EvidenceError::UnsafeInventory);
    }
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(EvidenceError::Io),
        Ok(_) => Err(EvidenceError::UnsafeInventory),
    }
}

fn validate_direct_file(path: &Path, parent: &Path) -> Result<()> {
    validate_control_directory(parent)?;
    if path.parent() != Some(parent) {
        return Err(EvidenceError::UnsafeInventory);
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| EvidenceError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(EvidenceError::UnsafeInventory);
    }
    let canonical = fs::canonicalize(path).map_err(|_| EvidenceError::Io)?;
    if canonical != path || canonical.parent() != Some(parent) {
        return Err(EvidenceError::UnsafeInventory);
    }
    Ok(())
}

fn parse_direct_control(path: &Path, control: &Path) -> Result<BTreeMap<String, String>> {
    validate_direct_file(path, control)?;
    let values = parse_control(path)?;
    validate_direct_file(path, control)?;
    Ok(values)
}

fn write_request(path: &Path, control: &Path, token: &str, id: FaultId) -> Result<()> {
    validate_control_directory(control)?;
    ensure_direct_path_absent(path, control)?;
    let bytes = request_bytes(token, std::process::id(), id);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|_| EvidenceError::Io)?;
    file.write_all(bytes.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|_| EvidenceError::Io)?;
    validate_direct_file(path, control)?;
    validate_control_directory(control)?;
    File::open(control)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| EvidenceError::Io)
}

fn spawn_worker(root: &EvidenceRoot, token: &str, id: FaultId) -> Result<Child> {
    root.direct_directory("control")?;
    let target = ChildWorkerTarget::open(root)?;
    target.revalidate(root)?;
    Command::new(std::env::current_exe().map_err(|_| EvidenceError::Io)?)
        .arg("__native-child")
        .arg("--root")
        .arg(&root.path)
        .arg("--token")
        .arg(token)
        .arg("--fault-id")
        .arg(id.as_str())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| EvidenceError::Io)
}

struct SupervisionOutcome {
    observation: Result<()>,
    termination_errors: usize,
}

fn supervise_worker(
    child: &mut Child,
    ready: &Path,
    token: &str,
    id: FaultId,
) -> Result<SupervisionOutcome> {
    let observation = wait_ready(child, ready, token, id);
    finish_supervision(child, observation)
}

fn finish_supervision(
    child: &mut impl ChildLifecycle,
    observation: Result<()>,
) -> Result<SupervisionOutcome> {
    let termination = terminate_and_reap(child)?;
    let observation = match observation {
        Ok(()) if termination.status.success => Err(EvidenceError::InvalidHarness),
        other => other,
    };
    Ok(SupervisionOutcome {
        observation,
        termination_errors: termination.error_count,
    })
}

fn wait_ready(child: &mut Child, ready: &Path, token: &str, id: FaultId) -> Result<()> {
    let control = ready.parent().ok_or(EvidenceError::UnsafeInventory)?;
    for _ in 0..READY_WAIT_POLLS {
        match fs::symlink_metadata(ready) {
            Ok(_) => {
                validate_control_directory(control)?;
                let values = parse_direct_control(ready, control)?;
                if values.get("schema").map(String::as_str) != Some(CHILD_SCHEMA)
                    || values.get("token").map(String::as_str) != Some(token)
                    || values.get("fault_id").map(String::as_str) != Some(id.as_str())
                    || values
                        .get("parent_pid")
                        .and_then(|value| value.parse::<u32>().ok())
                        != Some(std::process::id())
                    || values
                        .get("worker_pid")
                        .and_then(|value| value.parse::<u32>().ok())
                        != Some(child.id())
                    || values.get("state").map(String::as_str)
                        != Some("READY_BLOCKED_BEFORE_RETURN")
                    || values.get("last_successful_boundary").map(String::as_str)
                        != Some(id.as_str())
                    || values.len() != 7
                {
                    return Err(EvidenceError::InvalidHarness);
                }
                validate_direct_file(ready, control)?;
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(EvidenceError::Io),
        }
        if child.try_wait().map_err(|_| EvidenceError::Io)?.is_some() {
            return Err(EvidenceError::InvalidHarness);
        }
        thread::sleep(READY_WAIT_INTERVAL);
    }
    Err(EvidenceError::Bounds)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProcessStatus {
    success: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReapOutcome {
    status: ProcessStatus,
    error_count: usize,
}

trait ChildLifecycle {
    fn try_wait_status(&mut self) -> Result<Option<ProcessStatus>>;
    fn kill_process(&mut self) -> Result<()>;
    fn wait_status(&mut self) -> Result<ProcessStatus>;
    fn pause(&mut self);
}

impl ChildLifecycle for Child {
    fn try_wait_status(&mut self) -> Result<Option<ProcessStatus>> {
        self.try_wait()
            .map(|status| status.map(process_status))
            .map_err(|_| EvidenceError::Io)
    }

    fn kill_process(&mut self) -> Result<()> {
        self.kill().map_err(|_| EvidenceError::Io)
    }

    fn wait_status(&mut self) -> Result<ProcessStatus> {
        self.wait()
            .map(process_status)
            .map_err(|_| EvidenceError::Io)
    }

    fn pause(&mut self) {
        thread::sleep(READY_WAIT_INTERVAL);
    }
}

fn process_status(status: ExitStatus) -> ProcessStatus {
    ProcessStatus {
        success: status.success(),
    }
}

fn terminate_and_reap(child: &mut impl ChildLifecycle) -> Result<ReapOutcome> {
    let mut error_count = 0_usize;
    for _ in 0..REAP_WAIT_POLLS {
        match child.try_wait_status() {
            Ok(Some(_)) => return wait_with_retry(child, error_count),
            Ok(None) => {
                if child.kill_process().is_err() {
                    error_count = error_count.checked_add(1).ok_or(EvidenceError::Bounds)?;
                }
            }
            Err(_) => {
                error_count = error_count.checked_add(1).ok_or(EvidenceError::Bounds)?;
                if child.kill_process().is_err() {
                    error_count = error_count.checked_add(1).ok_or(EvidenceError::Bounds)?;
                }
            }
        }
        child.pause();
    }
    Err(EvidenceError::Replan)
}

fn wait_with_retry(child: &mut impl ChildLifecycle, mut error_count: usize) -> Result<ReapOutcome> {
    for _ in 0..WAIT_RETRIES {
        if let Ok(status) = child.wait_status() {
            return Ok(ReapOutcome {
                status,
                error_count,
            });
        }
        error_count = error_count.checked_add(1).ok_or(EvidenceError::Bounds)?;
        child.pause();
    }
    Err(EvidenceError::Replan)
}

fn cleanup_control<const N: usize>(control: &Path, paths: [&Path; N]) -> Result<()> {
    validate_control_directory(control)?;
    for path in paths {
        if path.parent() != Some(control) {
            return Err(EvidenceError::UnsafeInventory);
        }
        match fs::symlink_metadata(path) {
            Ok(metadata)
                if !metadata.file_type().is_symlink() && metadata.file_type().is_file() => {}
            Ok(_) => return Err(EvidenceError::UnsafeInventory),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(EvidenceError::Io),
        }
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(EvidenceError::Io),
        }
        validate_control_directory(control)?;
    }
    File::open(control)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| EvidenceError::Io)
}

fn prepare_crash_plan(child: &V2StoreChild, selected: FaultId) -> Result<CrashPlan> {
    clear_root(child)?;
    let kind = match selected.descriptor().phase {
        PhaseId::Rollback => CrashFlow::Rollback,
        PhaseId::EagerOpen => CrashFlow::EagerOpen,
        _ => CrashFlow::Transaction,
    };
    let (flow, prior) = match kind {
        CrashFlow::Transaction => {
            prepare_prior(child.path(), false)?;
            (flow_ids(FlowKind::P0P7Present), fingerprint(child)?)
        }
        CrashFlow::Rollback => {
            prepare_prior(child.path(), false)?;
            let prior = fingerprint(child)?;
            prepare_rollback_derivatives(child.path())?;
            (flow_ids(FlowKind::Rollback), prior)
        }
        CrashFlow::EagerOpen => {
            prepare_eager(child.path(), true)?;
            (
                flow_ids(FlowKind::EagerOpenConvergence),
                fingerprint(child)?,
            )
        }
    };
    validate_flow(&flow)?;
    let selected_index = flow
        .iter()
        .position(|id| *id == selected)
        .ok_or(EvidenceError::InvalidHarness)?;
    let mut io = V2Io::new(child)?;
    for id in flow.iter().take(selected_index).copied() {
        prepare_optional_before(child, id)?;
        if io.execute(source_site(id)?, None)? != SiteResult::Success {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    prepare_optional_before(child, selected)?;
    if classify(child)? != InventoryClass::ReviewedV2 {
        return Err(EvidenceError::InvalidHarness);
    }
    let selected_pre = fingerprint(child)?;
    Ok(CrashPlan {
        kind,
        flow,
        selected_index,
        prior,
        selected_pre,
    })
}

fn prepare_optional_before(child: &V2StoreChild, id: FaultId) -> Result<()> {
    if id == FaultId::P7RawStagingOpen && !child.path().join("sealed-journal-v1.staging").exists() {
        prepare_optional_cleanup(child.path())?;
    }
    Ok(())
}

fn validate_flow(flow: &[FaultId]) -> Result<()> {
    if flow.is_empty() {
        return Err(EvidenceError::InvalidHarness);
    }
    for pair in flow.windows(2) {
        if pair[0] != pair[1] && !pair[0].descriptor().successors.contains(&pair[1]) {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    Ok(())
}

fn reopen_and_converge(
    child: &V2StoreChild,
    plan: &CrashPlan,
    side: CommitSide,
) -> Result<TerminalState> {
    let selected = plan.flow[plan.selected_index];
    let terminal = match plan.kind {
        CrashFlow::Transaction if side == CommitSide::Precommit => {
            if selected.descriptor().phase == PhaseId::Preflight {
                if fingerprint(child)? != plan.selected_pre || plan.selected_pre != plan.prior {
                    return Err(EvidenceError::InvalidHarness);
                }
                TerminalState::UnchangedRefusal
            } else {
                rollback_uncommitted_transaction(child)?;
                if fingerprint(child)? != plan.prior {
                    return Err(EvidenceError::InvalidHarness);
                }
                TerminalState::PriorRollback
            }
        }
        CrashFlow::Transaction => {
            validate_committed_immediate(child)?;
            continue_flow(child, plan)?;
            if fingerprint(child)? == plan.prior {
                return Err(EvidenceError::InvalidHarness);
            }
            TerminalState::CompleteSuccess
        }
        CrashFlow::Rollback => {
            continue_flow(child, plan)?;
            if fingerprint(child)? != plan.prior {
                return Err(EvidenceError::InvalidHarness);
            }
            TerminalState::PriorRollback
        }
        CrashFlow::EagerOpen => {
            continue_flow(child, plan)?;
            if child.path().join("journal-rotation-v2.intent").exists()
                || !child.path().join("store-v1.lock").is_file()
            {
                return Err(EvidenceError::InvalidHarness);
            }
            TerminalState::CompleteSuccess
        }
    };
    if classify(child)? != InventoryClass::ReviewedV2 || !terminal_reachable(selected, terminal) {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(terminal)
}

fn continue_flow(child: &V2StoreChild, plan: &CrashPlan) -> Result<()> {
    let mut io = V2Io::new(child)?;
    for id in plan.flow.iter().take(plan.selected_index + 1).copied() {
        let occurrence = io.occurrences.entry(id).or_insert(0);
        *occurrence = occurrence.checked_add(1).ok_or(EvidenceError::Bounds)?;
    }
    let prefix = &plan.flow[..=plan.selected_index];
    io.adopted = prefix
        .iter()
        .any(|id| matches!(id, FaultId::P7Adopt | FaultId::OpenAdopt));
    io.inspection_published = prefix.contains(&FaultId::P7Inspection);
    for id in plan.flow.iter().skip(plan.selected_index + 1).copied() {
        prepare_optional_before(child, id)?;
        if io.execute(source_site(id)?, None)? != SiteResult::Success {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    let last = *plan.flow.last().ok_or(EvidenceError::InvalidHarness)?;
    if !last.descriptor().terminals.iter().any(|terminal| {
        matches!(
            terminal,
            TerminalState::CompleteSuccess | TerminalState::PriorRollback
        )
    }) {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

fn rollback_uncommitted_transaction(child: &V2StoreChild) -> Result<()> {
    if child.path().join("manifest-v2-slot-1.och").exists() {
        return Err(EvidenceError::InvalidHarness);
    }
    for name in [
        "sealed-journal-v1.staging",
        "sealed-journal-v1-g00000000000000000002.och",
        "native-segment-v1.staging",
        "native-segment-v1-g00000000000000000002.och",
        "active-journal-v1-g00000000000000000002.och",
        "active-journal-v1-g00000000000000000002.checkpoint",
        "generation-catalog-v2.staging",
        "generation-catalog-v2-slot-1.och",
        "manifest-v2.staging",
    ] {
        remove_exact_candidate(child.path(), name)?;
    }
    remove_exact_candidate(child.path(), "journal-rotation-v2.intent")?;
    Ok(())
}

fn remove_exact_candidate(root: &Path, name: &str) -> Result<()> {
    let path = root.join(name);
    if !path.exists() {
        return Ok(());
    }
    let bytes = bounded_read(&path)?;
    if !bytes.is_empty() && bytes != SITE_PAYLOAD {
        return Err(EvidenceError::InvalidHarness);
    }
    fs::remove_file(path).map_err(|_| EvidenceError::Io)?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| EvidenceError::Io)
}

fn validate_committed_immediate(child: &V2StoreChild) -> Result<()> {
    for name in [
        "sealed-journal-v1-g00000000000000000002.och",
        "native-segment-v1-g00000000000000000002.och",
        "generation-catalog-v2-slot-1.och",
        "manifest-v2-slot-1.och",
    ] {
        if bounded_read(&child.path().join(name))? != SITE_PAYLOAD {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    Ok(())
}

fn protected_committed_finals(
    fingerprint: &InventoryFingerprint,
) -> BTreeMap<String, (u64, String)> {
    fingerprint
        .artifacts
        .iter()
        .filter(|artifact| {
            artifact.name.starts_with("sealed-journal-v1-g")
                || artifact.name.starts_with("native-segment-v1-g")
                || artifact.name.starts_with("generation-catalog-v2-slot-")
                || artifact.name.starts_with("manifest-v2-slot-")
        })
        .map(|artifact| {
            (
                artifact.name.clone(),
                (artifact.logical_length, artifact.sha256.clone()),
            )
        })
        .collect()
}

fn validate_protected_finals(
    protected: &BTreeMap<String, (u64, String)>,
    final_fingerprint: &InventoryFingerprint,
) -> Result<()> {
    let final_map = protected_committed_finals(final_fingerprint);
    if protected
        .iter()
        .any(|(name, expected)| final_map.get(name) != Some(expected))
    {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

fn terminal_reachable(start: FaultId, terminal: TerminalState) -> bool {
    let mut visited = BTreeSet::new();
    let mut frontier = vec![start];
    while let Some(id) = frontier.pop() {
        if !visited.insert(id) {
            continue;
        }
        if id.descriptor().terminals.contains(&terminal) {
            return true;
        }
        frontier.extend_from_slice(id.descriptor().successors);
    }
    false
}

pub(super) fn structural_terminal(id: FaultId) -> TerminalState {
    match id.descriptor().phase {
        PhaseId::Preflight => TerminalState::UnchangedRefusal,
        PhaseId::Rollback => TerminalState::PriorRollback,
        PhaseId::EagerOpen => TerminalState::CompleteSuccess,
        _ if id.descriptor().commit_side == CommitSide::Precommit => TerminalState::PriorRollback,
        _ => TerminalState::CompleteSuccess,
    }
}

pub(super) fn hidden_child_command(arguments: &[String]) -> Result<()> {
    let values = parse_arguments(arguments)?;
    let root = EvidenceRoot::open(Path::new(values["--root"]))?;
    let id = FaultId::parse(values["--fault-id"])?;
    let token = values["--token"];
    if !valid_token(token) {
        return Err(EvidenceError::Usage);
    }
    let cases = root.direct_directory("cases")?;
    let control = root.direct_directory("control")?;
    let request = control.join(format!("{token}.request"));
    let ready_path = control.join(format!("{token}.ready"));
    let ready_staging = control.join(format!("{token}.ready.staging"));
    let request_values = parse_direct_control(&request, &control)?;
    let parent_pid = request_values
        .get("parent_pid")
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or(EvidenceError::InvalidHarness)?;
    if request_values.get("schema").map(String::as_str) != Some(CHILD_SCHEMA)
        || request_values.get("token").map(String::as_str) != Some(token)
        || request_values.get("fault_id").map(String::as_str) != Some(id.as_str())
        || request_values.len() != 4
        || fs::symlink_metadata(&ready_path).is_ok()
        || fs::symlink_metadata(&ready_staging).is_ok()
    {
        return Err(EvidenceError::InvalidHarness);
    }
    let target = ChildWorkerTarget::open(&root)?;
    if root.direct_directory("cases")? != cases || root.direct_directory("control")? != control {
        return Err(EvidenceError::UnsafeInventory);
    }
    target.revalidate(&root)?;
    validate_direct_file(&request, &control)?;
    ensure_direct_path_absent(&ready_path, &control)?;
    ensure_direct_path_absent(&ready_staging, &control)?;
    let ready = WorkerReady {
        path: ready_path,
        staging: ready_staging,
        control,
        token: token.to_owned(),
        parent_pid,
    };
    let selection = FaultSelection::new(
        id,
        NonZeroU32::MIN,
        FaultMode::ChildCrashAfterSuccess,
        PressureKind::None,
    )?;
    let mut io = V2Io::new_worker(&target, &ready)?;
    let _ = io.execute(source_site(id)?, Some(selection))?;
    Err(EvidenceError::InvalidHarness)
}

fn parse_arguments(arguments: &[String]) -> Result<BTreeMap<&str, &str>> {
    let required = ["--root", "--token", "--fault-id"];
    let mut values = BTreeMap::new();
    let (chunks, remainder) = arguments.as_chunks::<2>();
    for pair in chunks {
        if !required.contains(&pair[0].as_str())
            || pair[1].is_empty()
            || pair[1].starts_with("--")
            || values.insert(pair[0].as_str(), pair[1].as_str()).is_some()
        {
            return Err(EvidenceError::Usage);
        }
    }
    if !remainder.is_empty() || values.len() != required.len() {
        return Err(EvidenceError::Usage);
    }
    Ok(values)
}

fn parse_control(path: &Path) -> Result<BTreeMap<String, String>> {
    let file = File::open(path).map_err(|_| EvidenceError::Io)?;
    let metadata = file.metadata().map_err(|_| EvidenceError::Io)?;
    if !metadata.is_file()
        || metadata.len() > u64::try_from(MAX_CONTROL_BYTES).map_err(|_| EvidenceError::Bounds)?
    {
        return Err(EvidenceError::Bounds);
    }
    let mut bytes = Vec::new();
    file.take(u64::try_from(MAX_CONTROL_BYTES + 1).map_err(|_| EvidenceError::Bounds)?)
        .read_to_end(&mut bytes)
        .map_err(|_| EvidenceError::Io)?;
    if bytes.len() > MAX_CONTROL_BYTES {
        return Err(EvidenceError::Bounds);
    }
    let text = String::from_utf8(bytes).map_err(|_| EvidenceError::InvalidHarness)?;
    let mut values = BTreeMap::new();
    for line in text.lines() {
        let (key, value) = line.split_once('=').ok_or(EvidenceError::InvalidHarness)?;
        if key.is_empty()
            || value.is_empty()
            || value.len() > 1_024
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || values.insert(key.to_owned(), value.to_owned()).is_some()
        {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    Ok(values)
}

fn request_bytes(token: &str, parent_pid: u32, id: FaultId) -> String {
    format!(
        "schema={CHILD_SCHEMA}\ntoken={token}\nfault_id={}\nparent_pid={parent_pid}\n",
        id.as_str()
    )
}

fn ready_bytes(token: &str, parent_pid: u32, worker_pid: u32, id: FaultId) -> String {
    format!(
        "schema={CHILD_SCHEMA}\ntoken={token}\nfault_id={}\nparent_pid={parent_pid}\nworker_pid={worker_pid}\nstate=READY_BLOCKED_BEFORE_RETURN\nlast_successful_boundary={}\n",
        id.as_str(),
        id.as_str()
    )
}

fn valid_token(token: &str) -> bool {
    token.len() <= 64
        && token.starts_with("CRASH-")
        && token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte.is_ascii_uppercase() || byte == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_SUPERVISOR_ROOT: AtomicU64 = AtomicU64::new(1);

    struct TempHiddenRoot {
        parent: PathBuf,
        root: EvidenceRoot,
    }

    impl TempHiddenRoot {
        fn new() -> Self {
            let parent = std::env::temp_dir().join(format!(
                "och-v2-hidden-containment-{}-{}",
                std::process::id(),
                NEXT_SUPERVISOR_ROOT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&parent);
            fs::create_dir(&parent).expect("create hidden containment parent");
            let root = EvidenceRoot::prepare(&parent.join("evidence"))
                .expect("prepare hidden containment root");
            root.foundation_layout()
                .expect("prepare hidden containment layout");
            fs::create_dir(root.path.join("cases").join(V2_CHILD_NAME))
                .expect("create hidden worker child");
            Self { parent, root }
        }

        fn token() -> String {
            format!("CRASH-999-{}", std::process::id())
        }

        fn arguments(&self, token: &str, id: FaultId) -> Vec<String> {
            vec![
                "--root".to_owned(),
                self.root.path.to_string_lossy().into_owned(),
                "--token".to_owned(),
                token.to_owned(),
                "--fault-id".to_owned(),
                id.as_str().to_owned(),
            ]
        }

        fn write_request(&self, token: &str, id: FaultId) {
            fs::write(
                self.root
                    .path
                    .join("control")
                    .join(format!("{token}.request")),
                request_bytes(token, std::process::id(), id),
            )
            .expect("write hidden request");
        }
    }

    impl Drop for TempHiddenRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.parent);
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ModelState {
        Running,
        Exited,
    }

    struct LifecycleModel {
        state: ModelState,
        kill_errors: usize,
        wait_errors: usize,
        try_wait_errors: usize,
        exit_during_kill_error: bool,
        reaped: bool,
        pauses: usize,
    }

    impl LifecycleModel {
        const fn running() -> Self {
            Self {
                state: ModelState::Running,
                kill_errors: 0,
                wait_errors: 0,
                try_wait_errors: 0,
                exit_during_kill_error: false,
                reaped: false,
                pauses: 0,
            }
        }

        const fn exited() -> Self {
            Self {
                state: ModelState::Exited,
                kill_errors: 0,
                wait_errors: 0,
                try_wait_errors: 0,
                exit_during_kill_error: false,
                reaped: false,
                pauses: 0,
            }
        }

        fn assert_reaped(&self) {
            assert_eq!(self.state, ModelState::Exited);
            assert!(self.reaped);
        }
    }

    impl ChildLifecycle for LifecycleModel {
        fn try_wait_status(&mut self) -> Result<Option<ProcessStatus>> {
            if self.try_wait_errors > 0 {
                self.try_wait_errors -= 1;
                return Err(EvidenceError::Io);
            }
            Ok((self.state == ModelState::Exited).then_some(ProcessStatus { success: false }))
        }

        fn kill_process(&mut self) -> Result<()> {
            if self.kill_errors > 0 {
                self.kill_errors -= 1;
                if self.exit_during_kill_error {
                    self.state = ModelState::Exited;
                }
                return Err(EvidenceError::Io);
            }
            self.state = ModelState::Exited;
            Ok(())
        }

        fn wait_status(&mut self) -> Result<ProcessStatus> {
            if self.wait_errors > 0 {
                self.wait_errors -= 1;
                return Err(EvidenceError::Io);
            }
            if self.state != ModelState::Exited {
                return Err(EvidenceError::Io);
            }
            self.reaped = true;
            Ok(ProcessStatus { success: false })
        }

        fn pause(&mut self) {
            self.pauses = self.pauses.checked_add(1).expect("bounded model pauses");
        }
    }

    #[test]
    fn malformed_or_direct_hidden_child_arguments_refuse() {
        for arguments in [
            vec![],
            vec!["--root".to_owned(), "one".to_owned()],
            vec![
                "--root".to_owned(),
                "one".to_owned(),
                "--token".to_owned(),
                "hostile".to_owned(),
                "--fault-id".to_owned(),
                "V2IO-*".to_owned(),
            ],
        ] {
            assert!(hidden_child_command(&arguments).is_err());
        }
    }

    #[test]
    fn child_worker_view_has_no_cleanup_owner() {
        let source = include_str!("supervisor.rs");
        let drop_owner = ["impl Drop for", "ChildWorkerTarget"].join(" ");
        let cleanup_owner = ["fn", "cleanup("].join(" ");
        assert!(!source.contains(&drop_owner));
        let worker_impl = source
            .split("impl ChildWorkerTarget")
            .nth(1)
            .and_then(|value| value.split_once("}\n").map(|(body, _)| body))
            .expect("bounded worker implementation");
        assert!(!worker_impl.contains(&cleanup_owner));
        assert!(std::mem::needs_drop::<V2StoreChild>());
    }

    #[test]
    fn reopen_convergence_cannot_clear_or_rebuild_the_killed_inventory() {
        let source = include_str!("supervisor.rs");
        let convergence = source
            .split("fn reopen_and_converge")
            .nth(1)
            .and_then(|value| value.split_once("pub(super) fn hidden_child_command"))
            .map(|(body, _)| body)
            .expect("bounded convergence source region");
        assert!(!convergence.contains("clear_root"));
        assert!(!convergence.contains("write_file"));
        assert!(convergence.contains("rollback_uncommitted_transaction"));
        assert!(convergence.contains("continue_flow"));
        assert!(convergence.contains("validate_committed_immediate"));
        assert!(convergence.contains("validate_protected_finals"));
    }

    #[test]
    fn lifecycle_model_reaps_ready_timeout_parse_and_early_exit_paths() {
        for (observation, expected) in [
            (Ok(()), 0_u8),
            (Err(EvidenceError::Bounds), 1),
            (Err(EvidenceError::InvalidHarness), 2),
        ] {
            let mut child = LifecycleModel::running();
            let outcome = finish_supervision(&mut child, observation)
                .expect("running model must be killed and reaped");
            child.assert_reaped();
            assert_eq!(outcome.termination_errors, 0);
            match expected {
                0 => assert!(outcome.observation.is_ok()),
                1 => assert!(matches!(outcome.observation, Err(EvidenceError::Bounds))),
                2 => assert!(matches!(
                    outcome.observation,
                    Err(EvidenceError::InvalidHarness)
                )),
                _ => unreachable!(),
            }
        }

        let mut exited = LifecycleModel::exited();
        let outcome = finish_supervision(&mut exited, Err(EvidenceError::InvalidHarness))
            .expect("already-exited model must still be waited");
        exited.assert_reaped();
        assert!(matches!(
            outcome.observation,
            Err(EvidenceError::InvalidHarness)
        ));
    }

    #[test]
    fn lifecycle_model_captures_kill_race_and_wait_retry_before_reap() {
        let mut kill_race = LifecycleModel::running();
        kill_race.kill_errors = 1;
        kill_race.exit_during_kill_error = true;
        let outcome =
            finish_supervision(&mut kill_race, Ok(())).expect("kill race must proceed to wait");
        kill_race.assert_reaped();
        assert_eq!(outcome.termination_errors, 1);

        let mut wait_retry = LifecycleModel::running();
        wait_retry.try_wait_errors = 1;
        wait_retry.wait_errors = 1;
        let outcome = finish_supervision(&mut wait_retry, Err(EvidenceError::Bounds))
            .expect("wait error must retry through a proven reap");
        wait_retry.assert_reaped();
        assert_eq!(outcome.termination_errors, 2);
        assert!(matches!(outcome.observation, Err(EvidenceError::Bounds)));
    }

    #[test]
    fn lifecycle_model_fails_replan_when_reap_cannot_be_proven() {
        let mut blocked = LifecycleModel::running();
        blocked.kill_errors = REAP_WAIT_POLLS;
        assert!(matches!(
            finish_supervision(&mut blocked, Err(EvidenceError::Bounds)),
            Err(EvidenceError::Replan)
        ));
        assert_eq!(blocked.state, ModelState::Running);
        assert!(!blocked.reaped);
    }

    #[test]
    fn hidden_child_rejects_replaced_child_and_request_non_files_before_mutation() {
        let id = FaultId::ALL[0];
        let temp = TempHiddenRoot::new();
        let token = TempHiddenRoot::token();
        temp.write_request(&token, id);
        let child = temp.root.path.join("cases").join(V2_CHILD_NAME);
        fs::remove_dir(&child).expect("remove real worker child");
        fs::write(&child, b"hostile replacement").expect("write hostile child replacement");
        assert!(hidden_child_command(&temp.arguments(&token, id)).is_err());
        assert_eq!(
            fs::read(&child).expect("read unchanged child replacement"),
            b"hostile replacement"
        );

        let temp = TempHiddenRoot::new();
        let token = TempHiddenRoot::token();
        let request = temp
            .root
            .path
            .join("control")
            .join(format!("{token}.request"));
        fs::create_dir(&request).expect("create hostile request directory");
        assert!(hidden_child_command(&temp.arguments(&token, id)).is_err());
        assert_eq!(
            fs::read_dir(temp.root.path.join("cases").join(V2_CHILD_NAME))
                .expect("read unmutated worker child")
                .count(),
            0
        );
    }

    #[cfg(unix)]
    #[test]
    fn hidden_child_rejects_symlinked_layouts_and_forged_paths_without_external_mutation() {
        use std::os::unix::fs::symlink;

        let id = FaultId::ALL[0];

        let temp = TempHiddenRoot::new();
        let token = TempHiddenRoot::token();
        let external = temp.parent.join("external-request");
        fs::write(&external, request_bytes(&token, std::process::id(), id))
            .expect("write external request");
        let request = temp
            .root
            .path
            .join("control")
            .join(format!("{token}.request"));
        symlink(&external, &request).expect("symlink hostile request");
        let before = fs::read(&external).expect("read external request before");
        assert!(hidden_child_command(&temp.arguments(&token, id)).is_err());
        assert_eq!(
            fs::read(&external).expect("read external request after"),
            before
        );

        let temp = TempHiddenRoot::new();
        let token = TempHiddenRoot::token();
        temp.write_request(&token, id);
        let external = temp.parent.join("external-child");
        fs::create_dir(&external).expect("create external child");
        fs::write(external.join("sentinel"), b"unchanged").expect("write child sentinel");
        let child = temp.root.path.join("cases").join(V2_CHILD_NAME);
        fs::remove_dir(&child).expect("remove direct worker child");
        symlink(&external, &child).expect("symlink hostile child");
        assert!(hidden_child_command(&temp.arguments(&token, id)).is_err());
        assert_eq!(
            fs::read(external.join("sentinel")).expect("read child sentinel"),
            b"unchanged"
        );

        let temp = TempHiddenRoot::new();
        let token = TempHiddenRoot::token();
        let external = temp.parent.join("external-control");
        fs::create_dir(&external).expect("create external control");
        fs::write(external.join("sentinel"), b"unchanged").expect("write control sentinel");
        let control = temp.root.path.join("control");
        fs::remove_dir(&control).expect("remove direct control");
        symlink(&external, &control).expect("symlink hostile control");
        assert!(hidden_child_command(&temp.arguments(&token, id)).is_err());
        assert_eq!(
            fs::read(external.join("sentinel")).expect("read control sentinel"),
            b"unchanged"
        );
        assert_eq!(
            fs::read_dir(&external)
                .expect("read external control")
                .count(),
            1
        );
    }
}
