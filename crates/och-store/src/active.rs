//! Bounded active-journal ownership and crash-safe durable high-water state.

use crate::codec::{crc32c, frame_len_from_prefix_v1};
use crate::{
    AppendSequenceV1, DecodeLimitsV1, DecodedAdmissionV1, JOURNAL_V1_FRAME_PREFIX_LEN,
    JOURNAL_V1_HEADER_LEN, JournalHeaderV1, JournalV1Error, PreparedFrameV1,
    RecoveryClassification,
};
use och_core::{RetryQualification, StoreId};
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Exact active Journal V1 artifact name.
pub const ACTIVE_JOURNAL_FILE_NAME: &str = "active-journal-v1.och";
/// Exact active durable-high-water artifact name.
pub const ACTIVE_CHECKPOINT_FILE_NAME: &str = "active-journal-v1.checkpoint";
/// Legacy first active journal generation with fixed artifact names.
pub const ACTIVE_JOURNAL_GENERATION: u64 = 1;

pub(crate) fn active_journal_file_name(generation: u64) -> String {
    if generation == ACTIVE_JOURNAL_GENERATION {
        ACTIVE_JOURNAL_FILE_NAME.to_owned()
    } else {
        format!("active-journal-v1-g{generation:020}.och")
    }
}

pub(crate) fn active_checkpoint_file_name(generation: u64) -> String {
    if generation == ACTIVE_JOURNAL_GENERATION {
        ACTIVE_CHECKPOINT_FILE_NAME.to_owned()
    } else {
        format!("active-journal-v1-g{generation:020}.checkpoint")
    }
}
/// Hard maximum configured active journal bytes for this bounded vertical.
pub const MAX_ACTIVE_JOURNAL_BYTES: u64 = 512 * 1_024 * 1_024;
/// Hard maximum active admission frames for this bounded vertical.
pub const MAX_ACTIVE_JOURNAL_RECORDS: usize = 4_096;
/// Maximum retained store-directory path bytes.
pub const MAX_STORE_DIRECTORY_BYTES: usize = 4_096;

const CHECKPOINT_MAGIC: [u8; 8] = *b"OCHCP001";
const CHECKPOINT_VERSION: u16 = 1;
const CHECKPOINT_SLOT_LEN: usize = 64;
const CHECKPOINT_SLOT_LEN_U16: u16 = 64;
const CHECKPOINT_FILE_LEN: usize = CHECKPOINT_SLOT_LEN * 2;

#[cfg(test)]
std::thread_local! {
    static RECOVERY_FAULT: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn set_recovery_fault(code: u8) {
    RECOVERY_FAULT.with(|fault| fault.set(code));
}

#[cfg(test)]
fn take_recovery_fault(code: u8) -> bool {
    RECOVERY_FAULT.with(|fault| {
        if fault.get() == code {
            fault.set(0);
            true
        } else {
            false
        }
    })
}

/// Whether open creates the fixed active artifacts or requires them to exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActiveJournalOpenMode {
    /// Create the exact fixed artifacts in an existing directory.
    CreateNew,
    /// Open and validate the exact fixed artifacts.
    OpenExisting,
}

/// Explicit finite active-journal scan and append limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActiveJournalLimits {
    payload_limit: usize,
    active_bytes: u64,
    record_limit: usize,
}

impl ActiveJournalLimits {
    /// Validates all active-journal limits before filesystem I/O.
    ///
    /// # Errors
    ///
    /// Refuses a payload limit above Journal V1, bytes below the fixed header or
    /// above the hard active bound, and record counts above the hard bound.
    pub const fn new(
        max_payload_len: usize,
        max_active_bytes: u64,
        max_active_records: usize,
    ) -> Result<Self, ActiveJournalError> {
        if max_payload_len > crate::MAX_ADMISSION_PAYLOAD_V1
            || max_active_bytes < JOURNAL_V1_HEADER_LEN as u64
            || max_active_bytes > MAX_ACTIVE_JOURNAL_BYTES
            || max_active_records > MAX_ACTIVE_JOURNAL_RECORDS
        {
            return Err(ActiveJournalError::InvalidOptions);
        }
        Ok(Self {
            payload_limit: max_payload_len,
            active_bytes: max_active_bytes,
            record_limit: max_active_records,
        })
    }

    /// Returns the maximum admission payload accepted during open.
    #[must_use]
    pub const fn max_payload_len(self) -> usize {
        self.payload_limit
    }

    /// Returns the maximum active artifact byte length, including its header.
    #[must_use]
    pub const fn max_active_bytes(self) -> u64 {
        self.active_bytes
    }

    /// Returns the maximum active admission record count.
    #[must_use]
    pub const fn max_active_records(self) -> usize {
        self.record_limit
    }
}

/// Validated blocking active-journal open configuration.
pub struct ActiveJournalConfig {
    directory: PathBuf,
    store_id: StoreId,
    mode: ActiveJournalOpenMode,
    limits: ActiveJournalLimits,
    recovery_policy: RecoveryPolicy,
    generation: u64,
    sequence_floor: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryPolicy {
    Converge,
    ManifestGenesis,
    Strict,
}

impl ActiveJournalConfig {
    /// Validates one existing store directory and fixed-artifact open request.
    ///
    /// # Errors
    ///
    /// Refuses an empty or overlong encoded path before I/O.
    pub fn new(
        directory: PathBuf,
        store_id: StoreId,
        mode: ActiveJournalOpenMode,
        limits: ActiveJournalLimits,
    ) -> Result<Self, ActiveJournalError> {
        let length = directory.as_os_str().as_encoded_bytes().len();
        if length == 0 || length > MAX_STORE_DIRECTORY_BYTES {
            return Err(ActiveJournalError::InvalidOptions);
        }
        Ok(Self {
            directory,
            store_id,
            mode,
            limits,
            recovery_policy: RecoveryPolicy::Converge,
            generation: ACTIVE_JOURNAL_GENERATION,
            sequence_floor: 0,
        })
    }

    /// Returns the configured store identity.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns the configured open mode.
    #[must_use]
    pub const fn mode(&self) -> ActiveJournalOpenMode {
        self.mode
    }

    /// Returns the configured finite limits.
    #[must_use]
    pub const fn limits(&self) -> ActiveJournalLimits {
        self.limits
    }

    pub(crate) fn manifest_create(mut self) -> Self {
        self.recovery_policy = RecoveryPolicy::Strict;
        self
    }

    pub(crate) fn manifest_genesis(mut self) -> Self {
        self.recovery_policy = RecoveryPolicy::ManifestGenesis;
        self
    }

    pub(crate) fn manifest_existing(mut self) -> Self {
        self.recovery_policy = RecoveryPolicy::Strict;
        self
    }

    pub(crate) fn manifest_generation(
        mut self,
        generation: u64,
        sequence_floor: u64,
    ) -> Result<Self, ActiveJournalError> {
        if generation == 0 || (generation == ACTIVE_JOURNAL_GENERATION && sequence_floor != 0) {
            return Err(ActiveJournalError::InvalidOptions);
        }
        self.generation = generation;
        self.sequence_floor = sequence_floor;
        Ok(self)
    }
}

impl fmt::Debug for ActiveJournalConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveJournalConfig")
            .field("store_id", &self.store_id)
            .field("mode", &self.mode)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

/// Filesystem operation attached to sanitized generic I/O evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreIoOperation {
    /// Open or inspect the configured directory.
    OpenDirectory,
    /// Create an active artifact.
    CreateArtifact,
    /// Open an existing active artifact.
    OpenArtifact,
    /// Acquire the retained active-writer file lock.
    LockJournal,
    /// Read exact active bytes.
    Read,
    /// Seek within an active artifact.
    Seek,
    /// Write active journal or checkpoint bytes.
    Write,
    /// Resize a proven invalid unacknowledged suffix.
    Truncate,
    /// Synchronize journal content.
    SyncJournal,
    /// Synchronize durable-high-water content.
    SyncCheckpoint,
    /// Synchronize genesis directory entries.
    SyncDirectory,
    /// Read active artifact metadata.
    Metadata,
}

/// Generic path-free I/O evidence preserving the platform error classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreIoEvidence {
    operation: StoreIoOperation,
    kind: ErrorKind,
    raw_os_error: Option<i32>,
}

impl StoreIoEvidence {
    /// Returns the failed operation.
    #[must_use]
    pub const fn operation(self) -> StoreIoOperation {
        self.operation
    }

    /// Returns the standard-library error kind without platform assumptions.
    #[must_use]
    pub const fn kind(self) -> ErrorKind {
        self.kind
    }

    /// Returns optional platform-native error evidence.
    #[must_use]
    pub const fn raw_os_error(self) -> Option<i32> {
        self.raw_os_error
    }
}

/// Closed active-journal open, append, scan, and synchronization refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActiveJournalError {
    /// Configuration relationships are invalid.
    InvalidOptions,
    /// A fixed artifact already exists during create-new.
    AlreadyExists,
    /// Another process or handle retains the active writer lock.
    AlreadyOpen,
    /// Fixed active artifacts are missing.
    MissingArtifact,
    /// Header, checkpoint, frame, or prefix structure is invalid.
    InvalidLayout,
    /// Header, checkpoint, or frame store scope differs.
    StoreMismatch,
    /// The next frame would exceed the active byte or record limit.
    RotationRequired,
    /// One frame cannot fit an otherwise empty configured active generation.
    FrameTooLarge,
    /// Append sequence is not the exact writer-owned successor.
    SequenceMismatch,
    /// A prior append I/O failure may have changed bytes, so this handle is unusable.
    Faulted,
    /// Journal V1 framing or semantic decode refused the bytes.
    Journal(JournalV1Error),
    /// Generic path-free standard-library I/O evidence.
    Io(StoreIoEvidence),
}

impl fmt::Display for ActiveJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidOptions => "invalid active journal options",
            Self::AlreadyExists => "active journal artifacts already exist",
            Self::AlreadyOpen => "active journal is already open",
            Self::MissingArtifact => "active journal artifact is missing",
            Self::InvalidLayout => "invalid active journal layout",
            Self::StoreMismatch => "active journal store identity mismatch",
            Self::RotationRequired => "active journal rotation is required",
            Self::FrameTooLarge => "Journal V1 frame exceeds empty active generation capacity",
            Self::SequenceMismatch => "active journal append sequence mismatch",
            Self::Faulted => "active journal authority is faulted",
            Self::Journal(_) => "invalid Journal V1 evidence",
            Self::Io(_) => "active journal I/O failed",
        })
    }
}

impl Error for ActiveJournalError {}

impl From<JournalV1Error> for ActiveJournalError {
    fn from(error: JournalV1Error) -> Self {
        Self::Journal(error)
    }
}

/// Stable identity of the one pre-manifest active journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalIdentity {
    store_id: StoreId,
    generation: u64,
}

impl JournalIdentity {
    /// Returns the exact store identity.
    #[must_use]
    pub const fn store_id(self) -> StoreId {
        self.store_id
    }

    /// Returns the fixed active generation.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

/// Crash-safe mechanical durable high-water state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableCutoff {
    journal: JournalIdentity,
    checkpoint_generation: u64,
    append_sequence: u64,
    end_offset: u64,
}

impl DurableCutoff {
    /// Returns the stable active-journal identity.
    #[must_use]
    pub const fn journal(self) -> JournalIdentity {
        self.journal
    }

    /// Returns the mechanical checkpoint slot generation covering this cutoff.
    #[must_use]
    pub const fn checkpoint_generation(self) -> u64 {
        self.checkpoint_generation
    }

    /// Returns the last durable append sequence, or zero at genesis.
    #[must_use]
    pub const fn append_sequence(self) -> u64 {
        self.append_sequence
    }

    /// Returns the exact durable end offset.
    #[must_use]
    pub const fn end_offset(self) -> u64 {
        self.end_offset
    }
}

/// Sanitized bounded inspection of active journal state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActiveJournalInspection {
    journal: JournalIdentity,
    active_bytes: u64,
    active_records: usize,
    last_append_sequence: u64,
    durable_cutoff: DurableCutoff,
    sync_count: u64,
}

impl ActiveJournalInspection {
    /// Returns the stable active-journal identity.
    #[must_use]
    pub const fn journal(self) -> JournalIdentity {
        self.journal
    }

    /// Returns current active artifact bytes including its header.
    #[must_use]
    pub const fn active_bytes(self) -> u64 {
        self.active_bytes
    }

    /// Returns current valid active admission records.
    #[must_use]
    pub const fn active_records(self) -> usize {
        self.active_records
    }

    /// Returns the last assigned append sequence, or zero at genesis.
    #[must_use]
    pub const fn last_append_sequence(self) -> u64 {
        self.last_append_sequence
    }

    /// Returns the crash-safe durable cutoff.
    #[must_use]
    pub const fn durable_cutoff(self) -> DurableCutoff {
        self.durable_cutoff
    }

    /// Returns successful centralized barrier count in this open session.
    #[must_use]
    pub const fn sync_count(self) -> u64 {
        self.sync_count
    }
}

/// One bounded decoded active-journal record exposed for inspection only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredAdmissionV1 {
    end_offset: u64,
    admission: DecodedAdmissionV1,
}

impl RecoveredAdmissionV1 {
    /// Returns the exact record end offset.
    #[must_use]
    pub const fn end_offset(&self) -> u64 {
        self.end_offset
    }

    /// Returns the decoded non-authorizing evidence.
    #[must_use]
    pub const fn admission(&self) -> &DecodedAdmissionV1 {
        &self.admission
    }

    /// Returns the retained retry qualification for bounded inspection.
    #[must_use]
    pub const fn retry(&self) -> &RetryQualification {
        self.admission.retry()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CheckpointState {
    slot_generation: u64,
    append_sequence: u64,
    end_offset: u64,
}

/// Sole blocking owner of one locked active journal and checkpoint pair.
pub struct ActiveJournal {
    journal: File,
    checkpoint: File,
    identity: JournalIdentity,
    limits: ActiveJournalLimits,
    checkpoint_state: CheckpointState,
    active_bytes: u64,
    records: Vec<RecoveredAdmissionV1>,
    sync_count: u64,
    faulted: bool,
    #[cfg(test)]
    faults: Faults,
}

pub(crate) struct ActiveRecoveryPlan {
    identity: JournalIdentity,
    cutoff: DurableCutoff,
    original_length: u64,
    classification: RecoveryClassification,
}

pub(crate) enum ManifestRootOpenError {
    RootMismatch,
    Active(ActiveJournalError),
}

impl ManifestRootOpenError {
    fn into_active(self) -> ActiveJournalError {
        match self {
            Self::RootMismatch => ActiveJournalError::InvalidLayout,
            Self::Active(error) => error,
        }
    }
}

impl From<ActiveJournalError> for ManifestRootOpenError {
    fn from(error: ActiveJournalError) -> Self {
        Self::Active(error)
    }
}

impl ActiveRecoveryPlan {
    pub(crate) const fn original_length(&self) -> u64 {
        self.original_length
    }

    pub(crate) const fn classification(&self) -> RecoveryClassification {
        self.classification
    }
}

impl ActiveJournal {
    /// Creates or opens the active artifacts on the calling blocking thread.
    ///
    /// The retained journal file lock excludes other writers for this value's
    /// lifetime. Open scans only configured bytes/records. Any recovery
    /// truncation is synchronized before this function returns readiness.
    ///
    /// # Errors
    ///
    /// Returns a closed path-free refusal without mutating durable cutoff on
    /// invalid options, lock, identity, layout, scan, or I/O failure.
    pub fn open(config: ActiveJournalConfig) -> Result<Self, ActiveJournalError> {
        let ActiveJournalConfig {
            directory: directory_path,
            store_id,
            mode,
            limits,
            recovery_policy,
            generation,
            sequence_floor,
        } = config;
        let directory = open_directory(&directory_path)?;
        let journal_path = directory_path.join(active_journal_file_name(generation));
        let checkpoint_path = directory_path.join(active_checkpoint_file_name(generation));
        let identity = JournalIdentity {
            store_id,
            generation,
        };
        match mode {
            ActiveJournalOpenMode::CreateNew => Self::create_new(
                &directory,
                &journal_path,
                &checkpoint_path,
                identity,
                sequence_floor,
                limits,
            ),
            ActiveJournalOpenMode::OpenExisting => Self::open_existing(
                &directory,
                &journal_path,
                &checkpoint_path,
                identity,
                sequence_floor,
                limits,
                recovery_policy,
                None,
            )
            .map(|(journal, _)| journal)
            .map_err(ManifestRootOpenError::into_active),
        }
    }

    pub(crate) fn open_manifest_root(
        config: ActiveJournalConfig,
        root: DurableCutoff,
    ) -> Result<(Self, Option<ActiveRecoveryPlan>), ManifestRootOpenError> {
        let ActiveJournalConfig {
            directory: directory_path,
            store_id,
            mode,
            limits,
            recovery_policy,
            generation,
            sequence_floor,
        } = config;
        if mode != ActiveJournalOpenMode::OpenExisting
            || recovery_policy != RecoveryPolicy::Strict
            || root.journal().store_id != store_id
            || root.journal().generation != generation
        {
            return Err(ActiveJournalError::InvalidOptions.into());
        }
        let directory = open_directory(&directory_path)?;
        let journal_path = directory_path.join(active_journal_file_name(generation));
        let checkpoint_path = directory_path.join(active_checkpoint_file_name(generation));
        Self::open_existing(
            &directory,
            &journal_path,
            &checkpoint_path,
            JournalIdentity {
                store_id,
                generation,
            },
            sequence_floor,
            limits,
            recovery_policy,
            Some(root),
        )
    }

    fn create_new(
        directory: &File,
        journal_path: &Path,
        checkpoint_path: &Path,
        identity: JournalIdentity,
        sequence_floor: u64,
        limits: ActiveJournalLimits,
    ) -> Result<Self, ActiveJournalError> {
        let mut journal = create_artifact(journal_path)?;
        lock_journal(&journal)?;
        let header_bytes = JournalHeaderV1::new(identity.store_id).encode();
        journal
            .write_all(&header_bytes)
            .map_err(|error| io_error(StoreIoOperation::Write, error))?;
        journal
            .sync_all()
            .map_err(|error| io_error(StoreIoOperation::SyncJournal, error))?;
        let checkpoint = create_artifact(checkpoint_path)?;
        let checkpoint_state =
            initialize_checkpoint(&checkpoint, directory, identity, sequence_floor)?;
        Ok(Self {
            journal,
            checkpoint,
            identity,
            limits,
            checkpoint_state,
            active_bytes: JOURNAL_V1_HEADER_LEN as u64,
            records: Vec::new(),
            sync_count: 0,
            faulted: false,
            #[cfg(test)]
            faults: Faults::default(),
        })
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    fn open_existing(
        directory: &File,
        journal_path: &Path,
        checkpoint_path: &Path,
        identity: JournalIdentity,
        sequence_floor: u64,
        limits: ActiveJournalLimits,
        recovery_policy: RecoveryPolicy,
        expected_root: Option<DurableCutoff>,
    ) -> Result<(Self, Option<ActiveRecoveryPlan>), ManifestRootOpenError> {
        let mut journal = open_artifact(journal_path)?;
        lock_journal(&journal)?;
        let journal_len = journal
            .metadata()
            .map_err(|error| io_error(StoreIoOperation::Metadata, error))?
            .len();
        if journal_len < JOURNAL_V1_HEADER_LEN as u64 || journal_len > limits.active_bytes {
            return Err(ActiveJournalError::InvalidLayout.into());
        }
        if recovery_policy == RecoveryPolicy::ManifestGenesis
            && journal_len != JOURNAL_V1_HEADER_LEN as u64
        {
            return Err(ActiveJournalError::InvalidLayout.into());
        }
        let mut header_bytes = [0_u8; JOURNAL_V1_HEADER_LEN];
        journal
            .read_exact(&mut header_bytes)
            .map_err(|error| io_error(StoreIoOperation::Read, error))?;
        let header_store_id = JournalHeaderV1::decode(&header_bytes)
            .map_err(ActiveJournalError::from)?
            .store_id();
        if header_store_id != identity.store_id {
            return Err(ActiveJournalError::StoreMismatch.into());
        }
        let (checkpoint, mut initialized_state) = match open_artifact(checkpoint_path) {
            Ok(checkpoint) => (checkpoint, None),
            Err(ActiveJournalError::MissingArtifact)
                if recovery_policy != RecoveryPolicy::Strict
                    && journal_len == JOURNAL_V1_HEADER_LEN as u64 =>
            {
                let checkpoint = create_artifact(checkpoint_path)?;
                let state =
                    initialize_checkpoint(&checkpoint, directory, identity, sequence_floor)?;
                (checkpoint, Some(state))
            }
            Err(error) => return Err(error.into()),
        };
        let checkpoint_len = checkpoint
            .metadata()
            .map_err(|error| io_error(StoreIoOperation::Metadata, error))?
            .len();
        if checkpoint_len == 0
            && recovery_policy != RecoveryPolicy::Strict
            && journal_len == JOURNAL_V1_HEADER_LEN as u64
        {
            if initialized_state.is_none() {
                initialized_state = Some(initialize_checkpoint(
                    &checkpoint,
                    directory,
                    identity,
                    sequence_floor,
                )?);
            }
        } else if checkpoint_len != CHECKPOINT_FILE_LEN as u64 {
            return Err(ActiveJournalError::InvalidLayout.into());
        }
        let checkpoint_state = match initialized_state {
            Some(state) => Some(state),
            None => read_checkpoint(&checkpoint, identity, sequence_floor, journal_len)?,
        };
        if recovery_policy == RecoveryPolicy::ManifestGenesis
            && checkpoint_state != Some(genesis_checkpoint_state(sequence_floor))
        {
            return Err(ActiveJournalError::InvalidLayout.into());
        }
        if checkpoint_state.is_none()
            && (expected_root.is_some() || journal_len != JOURNAL_V1_HEADER_LEN as u64)
        {
            return Err(ActiveJournalError::InvalidLayout.into());
        }
        let mut checkpoint_state = match checkpoint_state {
            Some(state) => state,
            None => initialize_checkpoint(&checkpoint, directory, identity, sequence_floor)?,
        };
        if expected_root
            .is_some_and(|root| root != durable_cutoff_from_state(identity, checkpoint_state))
        {
            return Err(ManifestRootOpenError::RootMismatch);
        }
        let scan = match expected_root {
            Some(root) => scan_manifest_root(
                &mut journal,
                identity,
                limits,
                checkpoint_state,
                sequence_floor,
                journal_len,
                root,
            )?,
            None => scan_journal(
                &mut journal,
                identity,
                limits,
                checkpoint_state,
                sequence_floor,
                journal_len,
            )?,
        };
        if recovery_policy == RecoveryPolicy::Strict
            && (scan.truncate_to.is_some() || scan.valid_end != checkpoint_state.end_offset)
        {
            return Err(ActiveJournalError::InvalidLayout.into());
        }
        if expected_root.is_none()
            && let Some(truncate_to) = scan.truncate_to
        {
            journal
                .set_len(truncate_to)
                .map_err(|error| io_error(StoreIoOperation::Truncate, error))?;
            journal
                .sync_all()
                .map_err(|error| io_error(StoreIoOperation::SyncJournal, error))?;
        }
        let active_bytes = scan.valid_end;
        let last_sequence = scan
            .records
            .last()
            .map_or(sequence_floor, |record| record.admission.append_sequence());
        if expected_root.is_some() {
            // Root-aware open is deliberately read-only until the manifest
            // transaction validates every other authority family.
        } else if active_bytes > checkpoint_state.end_offset {
            journal
                .sync_all()
                .map_err(|error| io_error(StoreIoOperation::SyncJournal, error))?;
            let next = CheckpointState {
                slot_generation: checkpoint_state
                    .slot_generation
                    .checked_add(1)
                    .ok_or(ActiveJournalError::InvalidLayout)?,
                append_sequence: last_sequence,
                end_offset: active_bytes,
            };
            write_checkpoint_slot(&checkpoint, identity, next)?;
            checkpoint
                .sync_all()
                .map_err(|error| io_error(StoreIoOperation::SyncCheckpoint, error))?;
            checkpoint_state = next;
        } else {
            // A readable slot may be the complete result of an interrupted
            // checkpoint publication. Re-synchronize the selected evidence so
            // readiness never relies on an unproven cached checkpoint write.
            checkpoint
                .sync_all()
                .map_err(|error| io_error(StoreIoOperation::SyncCheckpoint, error))?;
        }
        let recovery_plan = scan.recovery_plan;
        Ok((
            Self {
                journal,
                checkpoint,
                identity,
                limits,
                checkpoint_state,
                active_bytes,
                records: scan.records,
                sync_count: 0,
                faulted: false,
                #[cfg(test)]
                faults: Faults::default(),
            },
            recovery_plan,
        ))
    }

    /// Returns current sanitized active-journal inspection.
    #[must_use]
    pub fn inspection(&self) -> ActiveJournalInspection {
        ActiveJournalInspection {
            journal: self.identity,
            active_bytes: self.active_bytes,
            active_records: self.records.len(),
            last_append_sequence: self
                .records
                .last()
                .map_or(self.checkpoint_state.append_sequence, |record| {
                    record.admission.append_sequence()
                }),
            durable_cutoff: self.durable_cutoff(),
            sync_count: self.sync_count,
        }
    }

    /// Returns bounded decoded active evidence without authorizing it.
    #[must_use]
    pub fn recovered_records(&self) -> &[RecoveredAdmissionV1] {
        &self.records
    }

    pub(crate) const fn limits(&self) -> ActiveJournalLimits {
        self.limits
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn apply_recovery(
        &mut self,
        plan: ActiveRecoveryPlan,
    ) -> Result<(), ActiveJournalError> {
        self.ensure_usable()?;
        let actual_length = self
            .journal
            .metadata()
            .map_err(|error| io_error(StoreIoOperation::Metadata, error))?
            .len();
        if plan.identity != self.identity
            || plan.cutoff != self.durable_cutoff()
            || plan.original_length != actual_length
            || self.active_bytes != plan.cutoff.end_offset()
            || plan.original_length <= self.active_bytes
        {
            return Err(ActiveJournalError::InvalidLayout);
        }
        #[cfg(test)]
        if take_recovery_fault(50) {
            return Err(injected_io(StoreIoOperation::Truncate));
        }
        self.journal
            .set_len(self.active_bytes)
            .map_err(|error| io_error(StoreIoOperation::Truncate, error))?;
        #[cfg(test)]
        if take_recovery_fault(51) {
            return Err(injected_io(StoreIoOperation::SyncJournal));
        }
        self.journal
            .sync_all()
            .map_err(|error| io_error(StoreIoOperation::SyncJournal, error))?;
        #[cfg(test)]
        if take_recovery_fault(52) {
            return Err(injected_io(StoreIoOperation::SyncJournal));
        }
        Ok(())
    }

    pub(crate) fn synchronize_recovery_cutoff(
        &self,
        cutoff: DurableCutoff,
    ) -> Result<(), ActiveJournalError> {
        self.ensure_usable()?;
        let actual_length = self
            .journal
            .metadata()
            .map_err(|error| io_error(StoreIoOperation::Metadata, error))?
            .len();
        if cutoff != self.durable_cutoff()
            || self.active_bytes != cutoff.end_offset()
            || actual_length != cutoff.end_offset()
        {
            return Err(ActiveJournalError::InvalidLayout);
        }
        self.journal
            .sync_all()
            .map_err(|error| io_error(StoreIoOperation::SyncJournal, error))
    }

    /// Returns the next exact writer-owned append sequence.
    ///
    /// # Errors
    ///
    /// Refuses sequence exhaustion or a terminally faulted handle.
    pub fn next_append_sequence(&self) -> Result<AppendSequenceV1, ActiveJournalError> {
        self.ensure_usable()?;
        let last = self
            .records
            .last()
            .map_or(self.checkpoint_state.append_sequence, |record| {
                record.admission.append_sequence()
            });
        AppendSequenceV1::new(
            last.checked_add(1)
                .ok_or(ActiveJournalError::SequenceMismatch)?,
        )
        .map_err(ActiveJournalError::Journal)
    }

    /// Appends one complete self-validating frame without synchronizing it.
    ///
    /// Store scope, declaration scope, sequence, byte limit, and record limit
    /// are checked before mutation.
    ///
    /// # Errors
    ///
    /// Refuses invalid scope/sequence/layout, rotation demand, or write failure.
    pub fn append(&mut self, frame: &PreparedFrameV1) -> Result<u64, ActiveJournalError> {
        self.ensure_usable()?;
        if frame.append_sequence() != self.next_append_sequence()? {
            return Err(ActiveJournalError::SequenceMismatch);
        }
        let previous = self
            .records
            .last()
            .map(|record| AppendSequenceV1::new(record.admission.append_sequence()))
            .transpose()
            .map_err(ActiveJournalError::Journal)?;
        let decoded = crate::decode_admission_frame_v1(
            frame.bytes(),
            DecodeLimitsV1::new(self.limits.payload_limit).map_err(ActiveJournalError::Journal)?,
            previous,
        )?;
        if decoded.store_id() != self.identity.store_id
            || decoded.declaration().store_id() != self.identity.store_id
            || frame.admission().store_id() != self.identity.store_id
            || frame.admission().declaration().store_id() != self.identity.store_id
        {
            return Err(ActiveJournalError::StoreMismatch);
        }
        let frame_len =
            u64::try_from(frame.len()).map_err(|_| ActiveJournalError::RotationRequired)?;
        let end_offset = self
            .active_bytes
            .checked_add(frame_len)
            .ok_or(ActiveJournalError::RotationRequired)?;
        if end_offset > self.limits.active_bytes || self.records.len() >= self.limits.record_limit {
            return Err(ActiveJournalError::RotationRequired);
        }
        self.journal
            .seek(SeekFrom::End(0))
            .map_err(|error| io_error(StoreIoOperation::Seek, error))?;
        #[cfg(test)]
        if let Some(length) = self.faults.short_write.take() {
            let partial = length.min(frame.bytes().len());
            if let Err(error) = self.journal.write_all(&frame.bytes()[..partial]) {
                self.faulted = true;
                return Err(io_error(StoreIoOperation::Write, error));
            }
            self.faulted = true;
            return Err(ActiveJournalError::Io(StoreIoEvidence {
                operation: StoreIoOperation::Write,
                kind: ErrorKind::WriteZero,
                raw_os_error: None,
            }));
        }
        if let Err(error) = self.journal.write_all(frame.bytes()) {
            self.faulted = true;
            return Err(io_error(StoreIoOperation::Write, error));
        }
        self.active_bytes = end_offset;
        self.records.push(RecoveredAdmissionV1 {
            end_offset,
            admission: decoded,
        });
        Ok(end_offset)
    }

    /// Returns whether one encoded frame fits the current active generation.
    #[must_use]
    pub fn can_fit(&self, frame_len: usize) -> bool {
        let Ok(frame_len) = u64::try_from(frame_len) else {
            return false;
        };
        self.records.len() < self.limits.record_limit
            && self
                .active_bytes
                .checked_add(frame_len)
                .is_some_and(|end| end <= self.limits.active_bytes)
    }

    /// Synchronizes journal bytes, advances the alternate checkpoint slot, and
    /// synchronizes that mechanical durable cutoff in the required order.
    ///
    /// # Errors
    ///
    /// On failure the in-memory durable cutoff is not advanced. A handle whose
    /// append may have partially changed bytes refuses all synchronization.
    pub fn sync_pending(&mut self) -> Result<DurableCutoff, ActiveJournalError> {
        self.ensure_usable()?;
        let last_sequence = self
            .records
            .last()
            .map_or(self.checkpoint_state.append_sequence, |record| {
                record.admission.append_sequence()
            });
        if last_sequence == self.checkpoint_state.append_sequence
            && self.active_bytes == self.checkpoint_state.end_offset
        {
            return Ok(self.durable_cutoff());
        }
        #[cfg(test)]
        if self.faults.journal_sync {
            return Err(injected_io(StoreIoOperation::SyncJournal));
        }
        self.journal
            .sync_all()
            .map_err(|error| io_error(StoreIoOperation::SyncJournal, error))?;
        let next = CheckpointState {
            slot_generation: self
                .checkpoint_state
                .slot_generation
                .checked_add(1)
                .ok_or(ActiveJournalError::InvalidLayout)?,
            append_sequence: last_sequence,
            end_offset: self.active_bytes,
        };
        #[cfg(test)]
        if self.faults.checkpoint_write {
            return Err(injected_io(StoreIoOperation::Write));
        }
        write_checkpoint_slot(&self.checkpoint, self.identity, next)?;
        #[cfg(test)]
        if self.faults.checkpoint_sync {
            return Err(injected_io(StoreIoOperation::SyncCheckpoint));
        }
        self.checkpoint
            .sync_all()
            .map_err(|error| io_error(StoreIoOperation::SyncCheckpoint, error))?;
        self.checkpoint_state = next;
        self.sync_count = self.sync_count.saturating_add(1);
        Ok(self.durable_cutoff())
    }

    pub(crate) fn durable_cutoff(&self) -> DurableCutoff {
        DurableCutoff {
            journal: self.identity,
            checkpoint_generation: self.checkpoint_state.slot_generation,
            append_sequence: self.checkpoint_state.append_sequence,
            end_offset: self.checkpoint_state.end_offset,
        }
    }

    fn ensure_usable(&self) -> Result<(), ActiveJournalError> {
        if self.faulted {
            Err(ActiveJournalError::Faulted)
        } else {
            Ok(())
        }
    }
}

impl DurableCutoff {
    pub(crate) const fn from_manifest(
        store_id: StoreId,
        journal_generation: u64,
        checkpoint_generation: u64,
        append_sequence: u64,
        end_offset: u64,
    ) -> Self {
        Self {
            journal: JournalIdentity {
                store_id,
                generation: journal_generation,
            },
            checkpoint_generation,
            append_sequence,
            end_offset,
        }
    }
}

struct ScanResult {
    records: Vec<RecoveredAdmissionV1>,
    valid_end: u64,
    truncate_to: Option<u64>,
    recovery_plan: Option<ActiveRecoveryPlan>,
}

#[allow(clippy::too_many_lines)]
fn scan_journal(
    journal: &mut File,
    identity: JournalIdentity,
    limits: ActiveJournalLimits,
    checkpoint: CheckpointState,
    sequence_floor: u64,
    journal_len: u64,
) -> Result<ScanResult, ActiveJournalError> {
    let mut records = Vec::new();
    let mut offset = JOURNAL_V1_HEADER_LEN as u64;
    let mut previous = if sequence_floor == 0 {
        None
    } else {
        Some(AppendSequenceV1::new(sequence_floor).map_err(ActiveJournalError::Journal)?)
    };
    let mut truncate_to = None;
    while offset < journal_len {
        if records.len() >= limits.record_limit {
            return Err(ActiveJournalError::InvalidLayout);
        }
        journal
            .seek(SeekFrom::Start(offset))
            .map_err(|error| io_error(StoreIoOperation::Seek, error))?;
        let remaining = journal_len - offset;
        if remaining < JOURNAL_V1_FRAME_PREFIX_LEN as u64 {
            if offset < checkpoint.end_offset {
                return Err(ActiveJournalError::InvalidLayout);
            }
            truncate_to = Some(offset);
            break;
        }
        let mut prefix = [0_u8; JOURNAL_V1_FRAME_PREFIX_LEN];
        journal
            .read_exact(&mut prefix)
            .map_err(|error| io_error(StoreIoOperation::Read, error))?;
        let frame_len = match frame_len_from_prefix_v1(
            &prefix,
            DecodeLimitsV1::new(limits.payload_limit).map_err(ActiveJournalError::Journal)?,
        ) {
            Ok(length) => length,
            Err(_)
                if offset >= checkpoint.end_offset
                    && remaining == JOURNAL_V1_FRAME_PREFIX_LEN as u64 =>
            {
                truncate_to = Some(offset);
                break;
            }
            Err(_) if offset >= checkpoint.end_offset => {
                return Err(ActiveJournalError::InvalidLayout);
            }
            Err(_) => return Err(ActiveJournalError::InvalidLayout),
        };
        let frame_len_u64 =
            u64::try_from(frame_len).map_err(|_| ActiveJournalError::InvalidLayout)?;
        let end = offset
            .checked_add(frame_len_u64)
            .ok_or(ActiveJournalError::InvalidLayout)?;
        if end > journal_len {
            if offset < checkpoint.end_offset {
                return Err(ActiveJournalError::InvalidLayout);
            }
            truncate_to = Some(offset);
            break;
        }
        if offset < checkpoint.end_offset && end > checkpoint.end_offset {
            return Err(ActiveJournalError::InvalidLayout);
        }
        let mut frame = vec![0_u8; frame_len];
        frame[..JOURNAL_V1_FRAME_PREFIX_LEN].copy_from_slice(&prefix);
        journal
            .read_exact(&mut frame[JOURNAL_V1_FRAME_PREFIX_LEN..])
            .map_err(|error| io_error(StoreIoOperation::Read, error))?;
        let decoded = match crate::decode_admission_frame_v1(
            &frame,
            DecodeLimitsV1::new(limits.payload_limit).map_err(ActiveJournalError::Journal)?,
            previous,
        ) {
            Ok(decoded) => decoded,
            Err(_) if offset >= checkpoint.end_offset && end == journal_len => {
                truncate_to = Some(offset);
                break;
            }
            Err(_) if offset >= checkpoint.end_offset => {
                return Err(ActiveJournalError::InvalidLayout);
            }
            Err(_) => return Err(ActiveJournalError::InvalidLayout),
        };
        if decoded.store_id() != identity.store_id
            || decoded.declaration().store_id() != identity.store_id
        {
            return Err(ActiveJournalError::StoreMismatch);
        }
        previous = Some(
            AppendSequenceV1::new(decoded.append_sequence())
                .map_err(ActiveJournalError::Journal)?,
        );
        records.push(RecoveredAdmissionV1 {
            end_offset: end,
            admission: decoded,
        });
        offset = end;
    }
    if checkpoint.end_offset > offset
        || (checkpoint.end_offset == JOURNAL_V1_HEADER_LEN as u64
            && checkpoint.append_sequence != sequence_floor)
    {
        return Err(ActiveJournalError::InvalidLayout);
    }
    let prefix_sequence = records
        .iter()
        .find(|record| record.end_offset == checkpoint.end_offset)
        .map_or(sequence_floor, |record| record.admission.append_sequence());
    if checkpoint.append_sequence != prefix_sequence {
        return Err(ActiveJournalError::InvalidLayout);
    }
    Ok(ScanResult {
        records,
        valid_end: offset,
        truncate_to,
        recovery_plan: None,
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn scan_manifest_root(
    journal: &mut File,
    identity: JournalIdentity,
    limits: ActiveJournalLimits,
    checkpoint: CheckpointState,
    sequence_floor: u64,
    journal_len: u64,
    root: DurableCutoff,
) -> Result<ScanResult, ActiveJournalError> {
    let mut records = Vec::new();
    let mut offset = JOURNAL_V1_HEADER_LEN as u64;
    let mut previous = if sequence_floor == 0 {
        None
    } else {
        Some(AppendSequenceV1::new(sequence_floor).map_err(ActiveJournalError::Journal)?)
    };
    while offset < root.end_offset() {
        if records.len() >= limits.record_limit
            || root.end_offset() - offset < JOURNAL_V1_FRAME_PREFIX_LEN as u64
        {
            return Err(ActiveJournalError::InvalidLayout);
        }
        journal
            .seek(SeekFrom::Start(offset))
            .map_err(|error| io_error(StoreIoOperation::Seek, error))?;
        let mut prefix = [0_u8; JOURNAL_V1_FRAME_PREFIX_LEN];
        journal
            .read_exact(&mut prefix)
            .map_err(|error| io_error(StoreIoOperation::Read, error))?;
        let frame_len = frame_len_from_prefix_v1(
            &prefix,
            DecodeLimitsV1::new(limits.payload_limit).map_err(ActiveJournalError::Journal)?,
        )
        .map_err(|_| ActiveJournalError::InvalidLayout)?;
        let end = offset
            .checked_add(u64::try_from(frame_len).map_err(|_| ActiveJournalError::InvalidLayout)?)
            .ok_or(ActiveJournalError::InvalidLayout)?;
        if end > root.end_offset() {
            return Err(ActiveJournalError::InvalidLayout);
        }
        let mut frame = vec![0_u8; frame_len];
        frame[..JOURNAL_V1_FRAME_PREFIX_LEN].copy_from_slice(&prefix);
        journal
            .read_exact(&mut frame[JOURNAL_V1_FRAME_PREFIX_LEN..])
            .map_err(|error| io_error(StoreIoOperation::Read, error))?;
        let decoded = crate::decode_admission_frame_v1(
            &frame,
            DecodeLimitsV1::new(limits.payload_limit).map_err(ActiveJournalError::Journal)?,
            previous,
        )
        .map_err(|_| ActiveJournalError::InvalidLayout)?;
        if decoded.store_id() != identity.store_id
            || decoded.declaration().store_id() != identity.store_id
        {
            return Err(ActiveJournalError::StoreMismatch);
        }
        previous = Some(
            AppendSequenceV1::new(decoded.append_sequence())
                .map_err(ActiveJournalError::Journal)?,
        );
        records.push(RecoveredAdmissionV1 {
            end_offset: end,
            admission: decoded,
        });
        offset = end;
    }
    let prefix_sequence = records
        .last()
        .map_or(sequence_floor, |record| record.admission.append_sequence());
    if offset != root.end_offset()
        || checkpoint.append_sequence != prefix_sequence
        || root.append_sequence() != prefix_sequence
        || root.checkpoint_generation() != checkpoint.slot_generation
        || journal_len < root.end_offset()
    {
        return Err(ActiveJournalError::InvalidLayout);
    }
    if journal_len == root.end_offset() {
        return Ok(ScanResult {
            records,
            valid_end: offset,
            truncate_to: None,
            recovery_plan: None,
        });
    }

    let suffix_length = journal_len - root.end_offset();
    let expected_sequence = prefix_sequence
        .checked_add(1)
        .ok_or(ActiveJournalError::InvalidLayout)?;
    let classification = if suffix_length < JOURNAL_V1_FRAME_PREFIX_LEN as u64 {
        journal
            .seek(SeekFrom::Start(root.end_offset()))
            .map_err(|error| io_error(StoreIoOperation::Seek, error))?;
        let suffix_len =
            usize::try_from(suffix_length).map_err(|_| ActiveJournalError::InvalidLayout)?;
        let mut suffix = [0_u8; JOURNAL_V1_FRAME_PREFIX_LEN - 1];
        journal
            .read_exact(&mut suffix[..suffix_len])
            .map_err(|error| io_error(StoreIoOperation::Read, error))?;
        let mut required = [0_u8; 16];
        required[..4].copy_from_slice(&crate::JOURNAL_V1_FRAME_MAGIC);
        required[4..6].copy_from_slice(&crate::JOURNAL_V1_VERSION.to_be_bytes());
        required[6] = 1;
        required[8..16].copy_from_slice(&expected_sequence.to_be_bytes());
        let required_len = suffix_len.min(required.len());
        if suffix[..required_len] != required[..required_len] {
            return Err(ActiveJournalError::InvalidLayout);
        }
        RecoveryClassification::ShortFramePrefix
    } else {
        journal
            .seek(SeekFrom::Start(root.end_offset()))
            .map_err(|error| io_error(StoreIoOperation::Seek, error))?;
        let mut prefix = [0_u8; JOURNAL_V1_FRAME_PREFIX_LEN];
        journal
            .read_exact(&mut prefix)
            .map_err(|error| io_error(StoreIoOperation::Read, error))?;
        if u64::from_be_bytes(prefix[8..16].try_into().unwrap_or_default()) != expected_sequence {
            return Err(ActiveJournalError::SequenceMismatch);
        }
        let frame_len = match frame_len_from_prefix_v1(
            &prefix,
            DecodeLimitsV1::new(limits.payload_limit).map_err(ActiveJournalError::Journal)?,
        ) {
            Ok(length) => length,
            Err(_) if suffix_length == JOURNAL_V1_FRAME_PREFIX_LEN as u64 => {
                return Ok(ScanResult {
                    records,
                    valid_end: offset,
                    truncate_to: None,
                    recovery_plan: Some(ActiveRecoveryPlan {
                        identity,
                        cutoff: root,
                        original_length: journal_len,
                        classification: RecoveryClassification::InvalidFramePrefix,
                    }),
                });
            }
            Err(_) => return Err(ActiveJournalError::InvalidLayout),
        };
        let end = root
            .end_offset()
            .checked_add(u64::try_from(frame_len).map_err(|_| ActiveJournalError::InvalidLayout)?)
            .ok_or(ActiveJournalError::InvalidLayout)?;
        match end.cmp(&journal_len) {
            std::cmp::Ordering::Greater => RecoveryClassification::TruncatedDeclaredFrame,
            std::cmp::Ordering::Less => return Err(ActiveJournalError::InvalidLayout),
            std::cmp::Ordering::Equal => {
                let mut frame = vec![0_u8; frame_len];
                frame[..JOURNAL_V1_FRAME_PREFIX_LEN].copy_from_slice(&prefix);
                journal
                    .read_exact(&mut frame[JOURNAL_V1_FRAME_PREFIX_LEN..])
                    .map_err(|error| io_error(StoreIoOperation::Read, error))?;
                match crate::decode_admission_frame_v1(
                    &frame,
                    DecodeLimitsV1::new(limits.payload_limit)
                        .map_err(ActiveJournalError::Journal)?,
                    previous,
                ) {
                    Ok(decoded)
                        if decoded.store_id() != identity.store_id
                            || decoded.declaration().store_id() != identity.store_id =>
                    {
                        return Err(ActiveJournalError::StoreMismatch);
                    }
                    Ok(_) => return Err(ActiveJournalError::InvalidLayout),
                    Err(_) => RecoveryClassification::InvalidCompleteFrame,
                }
            }
        }
    };
    Ok(ScanResult {
        records,
        valid_end: offset,
        truncate_to: None,
        recovery_plan: Some(ActiveRecoveryPlan {
            identity,
            cutoff: root,
            original_length: journal_len,
            classification,
        }),
    })
}

const fn durable_cutoff_from_state(
    identity: JournalIdentity,
    state: CheckpointState,
) -> DurableCutoff {
    DurableCutoff {
        journal: identity,
        checkpoint_generation: state.slot_generation,
        append_sequence: state.append_sequence,
        end_offset: state.end_offset,
    }
}

fn open_directory(path: &Path) -> Result<File, ActiveJournalError> {
    let metadata = path
        .metadata()
        .map_err(|error| io_error(StoreIoOperation::OpenDirectory, error))?;
    if !metadata.is_dir() {
        return Err(ActiveJournalError::InvalidLayout);
    }
    File::open(path).map_err(|error| io_error(StoreIoOperation::OpenDirectory, error))
}

fn create_artifact(path: &Path) -> Result<File, ActiveJournalError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            if error.kind() == ErrorKind::AlreadyExists {
                ActiveJournalError::AlreadyExists
            } else {
                io_error(StoreIoOperation::CreateArtifact, error)
            }
        })
}

fn open_artifact(path: &Path) -> Result<File, ActiveJournalError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                ActiveJournalError::MissingArtifact
            } else {
                io_error(StoreIoOperation::OpenArtifact, error)
            }
        })
}

/// Proves an existing premanifest active pair is an exact current genesis
/// without acquiring its writer lock or changing either artifact.
pub(crate) fn preflight_manifest_genesis(
    directory: &Path,
    store_id: StoreId,
) -> Result<(), ActiveJournalError> {
    let identity = JournalIdentity {
        store_id,
        generation: ACTIVE_JOURNAL_GENERATION,
    };
    let journal_path = directory.join(ACTIVE_JOURNAL_FILE_NAME);
    let mut journal = File::open(&journal_path).map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            ActiveJournalError::MissingArtifact
        } else {
            io_error(StoreIoOperation::OpenArtifact, error)
        }
    })?;
    if journal
        .metadata()
        .map_err(|error| io_error(StoreIoOperation::Metadata, error))?
        .len()
        != JOURNAL_V1_HEADER_LEN as u64
    {
        return Err(ActiveJournalError::InvalidLayout);
    }
    let mut header = [0_u8; JOURNAL_V1_HEADER_LEN];
    journal
        .read_exact(&mut header)
        .map_err(|error| io_error(StoreIoOperation::Read, error))?;
    if JournalHeaderV1::decode(&header)?.store_id() != store_id {
        return Err(ActiveJournalError::StoreMismatch);
    }

    let checkpoint_path = directory.join(ACTIVE_CHECKPOINT_FILE_NAME);
    let mut checkpoint = match File::open(&checkpoint_path) {
        Ok(checkpoint) => checkpoint,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error(StoreIoOperation::OpenArtifact, error)),
    };
    let checkpoint_len = checkpoint
        .metadata()
        .map_err(|error| io_error(StoreIoOperation::Metadata, error))?
        .len();
    if checkpoint_len == 0 {
        return Ok(());
    }
    if checkpoint_len != CHECKPOINT_FILE_LEN as u64 {
        return Err(ActiveJournalError::InvalidLayout);
    }
    let mut bytes = [0_u8; CHECKPOINT_FILE_LEN];
    checkpoint
        .read_exact(&mut bytes)
        .map_err(|error| io_error(StoreIoOperation::Read, error))?;
    let expected = encode_checkpoint(identity, genesis_checkpoint_state(0));
    if bytes[..CHECKPOINT_SLOT_LEN] != expected
        || bytes[CHECKPOINT_SLOT_LEN..].iter().any(|byte| *byte != 0)
    {
        return Err(ActiveJournalError::InvalidLayout);
    }
    Ok(())
}

fn lock_journal(file: &File) -> Result<(), ActiveJournalError> {
    file.try_lock().map_err(|error| match error {
        std::fs::TryLockError::WouldBlock => ActiveJournalError::AlreadyOpen,
        std::fs::TryLockError::Error(error) => io_error(StoreIoOperation::LockJournal, error),
    })
}

fn initialize_checkpoint(
    checkpoint: &File,
    directory: &File,
    identity: JournalIdentity,
    sequence_floor: u64,
) -> Result<CheckpointState, ActiveJournalError> {
    checkpoint
        .set_len(CHECKPOINT_FILE_LEN as u64)
        .map_err(|error| io_error(StoreIoOperation::Write, error))?;
    let state = genesis_checkpoint_state(sequence_floor);
    write_checkpoint_slot(checkpoint, identity, state)?;
    checkpoint
        .sync_all()
        .map_err(|error| io_error(StoreIoOperation::SyncCheckpoint, error))?;
    directory
        .sync_all()
        .map_err(|error| io_error(StoreIoOperation::SyncDirectory, error))?;
    Ok(state)
}

const fn genesis_checkpoint_state(sequence_floor: u64) -> CheckpointState {
    CheckpointState {
        slot_generation: 1,
        append_sequence: sequence_floor,
        end_offset: JOURNAL_V1_HEADER_LEN as u64,
    }
}

fn write_checkpoint_slot(
    file: &File,
    identity: JournalIdentity,
    state: CheckpointState,
) -> Result<(), ActiveJournalError> {
    let bytes = encode_checkpoint(identity, state);
    let index = usize::try_from((state.slot_generation - 1) % 2)
        .map_err(|_| ActiveJournalError::InvalidLayout)?;
    let offset = u64::try_from(index * CHECKPOINT_SLOT_LEN)
        .map_err(|_| ActiveJournalError::InvalidLayout)?;
    let mut file = file;
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| io_error(StoreIoOperation::Seek, error))?;
    file.write_all(&bytes)
        .map_err(|error| io_error(StoreIoOperation::Write, error))
}

fn encode_checkpoint(identity: JournalIdentity, state: CheckpointState) -> [u8; 64] {
    let mut bytes = [0_u8; CHECKPOINT_SLOT_LEN];
    bytes[..8].copy_from_slice(&CHECKPOINT_MAGIC);
    bytes[8..10].copy_from_slice(&CHECKPOINT_VERSION.to_be_bytes());
    bytes[10..12].copy_from_slice(&CHECKPOINT_SLOT_LEN_U16.to_be_bytes());
    bytes[12..28].copy_from_slice(identity.store_id.as_bytes());
    bytes[28..36].copy_from_slice(&identity.generation.to_be_bytes());
    bytes[36..44].copy_from_slice(&state.slot_generation.to_be_bytes());
    bytes[44..52].copy_from_slice(&state.append_sequence.to_be_bytes());
    bytes[52..60].copy_from_slice(&state.end_offset.to_be_bytes());
    let checksum = crc32c(&bytes[..60]);
    bytes[60..64].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

enum SlotDecode {
    Zero,
    Valid(CheckpointState),
    InvalidNonzero,
}

fn read_checkpoint(
    file: &File,
    identity: JournalIdentity,
    sequence_floor: u64,
    journal_len: u64,
) -> Result<Option<CheckpointState>, ActiveJournalError> {
    let mut bytes = [0_u8; CHECKPOINT_FILE_LEN];
    let mut file = file;
    file.read_exact(&mut bytes)
        .map_err(|error| io_error(StoreIoOperation::Read, error))?;
    let first = decode_checkpoint_slot(
        &bytes[..CHECKPOINT_SLOT_LEN],
        0,
        identity,
        sequence_floor,
        journal_len,
    );
    let second = decode_checkpoint_slot(
        &bytes[CHECKPOINT_SLOT_LEN..],
        1,
        identity,
        sequence_floor,
        journal_len,
    );
    match (first, second) {
        (SlotDecode::Zero, SlotDecode::Zero) => Ok(None),
        (SlotDecode::InvalidNonzero, _) | (_, SlotDecode::InvalidNonzero) => {
            Err(ActiveJournalError::InvalidLayout)
        }
        (SlotDecode::Valid(state), SlotDecode::Zero)
        | (SlotDecode::Zero, SlotDecode::Valid(state)) => {
            if state.slot_generation == 1 {
                Ok(Some(state))
            } else {
                Err(ActiveJournalError::InvalidLayout)
            }
        }
        (SlotDecode::Valid(first), SlotDecode::Valid(second)) => {
            let (older, newer) = if first.slot_generation < second.slot_generation {
                (first, second)
            } else {
                (second, first)
            };
            if newer.slot_generation != older.slot_generation + 1
                || newer.append_sequence <= older.append_sequence
                || newer.end_offset <= older.end_offset
            {
                return Err(ActiveJournalError::InvalidLayout);
            }
            Ok(Some(newer))
        }
    }
}

fn decode_checkpoint_slot(
    bytes: &[u8],
    slot_index: usize,
    identity: JournalIdentity,
    sequence_floor: u64,
    journal_len: u64,
) -> SlotDecode {
    if bytes.iter().all(|byte| *byte == 0) {
        return SlotDecode::Zero;
    }
    if bytes.len() != CHECKPOINT_SLOT_LEN
        || bytes[..8] != CHECKPOINT_MAGIC
        || u16::from_be_bytes([bytes[8], bytes[9]]) != CHECKPOINT_VERSION
        || u16::from_be_bytes([bytes[10], bytes[11]]) != CHECKPOINT_SLOT_LEN_U16
        || crc32c(&bytes[..60]) != u32::from_be_bytes(bytes[60..64].try_into().unwrap_or_default())
    {
        return SlotDecode::InvalidNonzero;
    }
    let store_id = StoreId::from_bytes(bytes[12..28].try_into().unwrap_or_default());
    let generation = u64::from_be_bytes(bytes[28..36].try_into().unwrap_or_default());
    let slot_generation = u64::from_be_bytes(bytes[36..44].try_into().unwrap_or_default());
    let append_sequence = u64::from_be_bytes(bytes[44..52].try_into().unwrap_or_default());
    let end_offset = u64::from_be_bytes(bytes[52..60].try_into().unwrap_or_default());
    if store_id != Ok(identity.store_id)
        || generation != identity.generation
        || slot_generation == 0
        || usize::try_from((slot_generation - 1) % 2).unwrap_or_default() != slot_index
        || end_offset < JOURNAL_V1_HEADER_LEN as u64
        || end_offset > journal_len
        || (append_sequence == sequence_floor) != (end_offset == JOURNAL_V1_HEADER_LEN as u64)
    {
        return SlotDecode::InvalidNonzero;
    }
    SlotDecode::Valid(CheckpointState {
        slot_generation,
        append_sequence,
        end_offset,
    })
}

#[allow(clippy::needless_pass_by_value)]
fn io_error(operation: StoreIoOperation, error: std::io::Error) -> ActiveJournalError {
    ActiveJournalError::Io(StoreIoEvidence {
        operation,
        kind: error.kind(),
        raw_os_error: error.raw_os_error(),
    })
}

#[cfg(test)]
fn injected_io(operation: StoreIoOperation) -> ActiveJournalError {
    ActiveJournalError::Io(StoreIoEvidence {
        operation,
        kind: ErrorKind::Other,
        raw_os_error: Some(28),
    })
}

#[cfg(test)]
#[derive(Default)]
struct Faults {
    short_write: Option<usize>,
    journal_sync: bool,
    checkpoint_write: bool,
    checkpoint_sync: bool,
}

#[cfg(test)]
mod tests {
    use super::{
        ACTIVE_JOURNAL_FILE_NAME, ActiveJournal, ActiveJournalConfig, ActiveJournalError,
        ActiveJournalLimits, ActiveJournalOpenMode, StoreIoOperation,
    };
    use crate::{AppendSequenceV1, MAX_ADMISSION_PAYLOAD_V1, PreparedAdmissionV1, test_support};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

    struct Directory(PathBuf);

    impl Directory {
        fn new() -> Self {
            let value = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("och-active-fault-{}-{value}", std::process::id()));
            fs::create_dir(&path).expect("unique fault directory");
            Self(path)
        }
    }

    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn config(directory: &Directory, mode: ActiveJournalOpenMode) -> ActiveJournalConfig {
        ActiveJournalConfig::new(
            directory.0.clone(),
            test_support::store_id(1),
            mode,
            ActiveJournalLimits::new(MAX_ADMISSION_PAYLOAD_V1, 4 * 1_024 * 1_024, 8)
                .expect("fault limits"),
        )
        .expect("fault configuration")
    }

    fn frame() -> crate::PreparedFrameV1 {
        PreparedAdmissionV1::new(test_support::no_change_admission())
            .expect("fault admission")
            .into_frame(AppendSequenceV1::new(1).expect("sequence"))
            .expect("fault frame")
    }

    #[test]
    fn injected_short_write_leaves_only_a_reopen_truncatable_suffix() {
        let directory = Directory::new();
        let mut journal = ActiveJournal::open(config(&directory, ActiveJournalOpenMode::CreateNew))
            .expect("create fault journal");
        let before = journal.inspection();
        journal.faults.short_write = Some(11);
        let error = journal.append(&frame()).expect_err("short write refuses");
        let ActiveJournalError::Io(evidence) = error else {
            panic!("short write must retain generic I/O evidence");
        };
        assert_eq!(evidence.operation(), StoreIoOperation::Write);
        assert_eq!(evidence.raw_os_error(), None);
        assert_eq!(journal.inspection(), before);
        let journal_path = directory.0.join(ACTIVE_JOURNAL_FILE_NAME);
        let torn_len = fs::metadata(&journal_path)
            .expect("torn journal metadata")
            .len();
        assert!(torn_len > before.active_bytes());
        assert_eq!(
            journal.next_append_sequence(),
            Err(ActiveJournalError::Faulted)
        );
        assert_eq!(journal.append(&frame()), Err(ActiveJournalError::Faulted));
        assert_eq!(journal.sync_pending(), Err(ActiveJournalError::Faulted));
        assert_eq!(journal.inspection(), before);
        assert_eq!(
            fs::metadata(&journal_path)
                .expect("faulted journal metadata")
                .len(),
            torn_len
        );
        drop(journal);
        let mut reopened =
            ActiveJournal::open(config(&directory, ActiveJournalOpenMode::OpenExisting))
                .expect("truncate and sync torn suffix");
        assert_eq!(reopened.inspection(), before);
        let end_offset = reopened.append(&frame()).expect("reopened append");
        let cutoff = reopened.sync_pending().expect("reopened sync");
        assert_eq!(cutoff.append_sequence(), 1);
        assert_eq!(cutoff.end_offset(), end_offset);
    }

    #[test]
    fn injected_barrier_failures_never_advance_in_memory_cutoff() {
        for point in 0..3 {
            let directory = Directory::new();
            let mut journal =
                ActiveJournal::open(config(&directory, ActiveJournalOpenMode::CreateNew))
                    .expect("create barrier fault journal");
            journal
                .append(&frame())
                .expect("append before barrier fault");
            match point {
                0 => journal.faults.journal_sync = true,
                1 => journal.faults.checkpoint_write = true,
                _ => journal.faults.checkpoint_sync = true,
            }
            let error = journal.sync_pending().expect_err("barrier fault refuses");
            let ActiveJournalError::Io(evidence) = error else {
                panic!("barrier fault must retain generic I/O evidence");
            };
            let expected = match point {
                0 => StoreIoOperation::SyncJournal,
                1 => StoreIoOperation::Write,
                _ => StoreIoOperation::SyncCheckpoint,
            };
            assert_eq!(evidence.operation(), expected);
            assert_eq!(evidence.raw_os_error(), Some(28));
            assert_eq!(journal.inspection().durable_cutoff().append_sequence(), 0);
        }
    }
}
