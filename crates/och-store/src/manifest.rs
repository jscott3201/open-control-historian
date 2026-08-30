//! Bounded manifest-rooted active journal and canonical registry persistence.

use crate::active::{
    ActiveJournal, ActiveJournalConfig, ActiveRecoveryPlan, ManifestRootOpenError,
    active_checkpoint_file_name, active_journal_file_name, preflight_manifest_genesis,
};
use crate::codec::{
    Cursor, Encoder, crc32c, decode_declaration, decode_declaration_evidence, encode_declaration,
    encode_declaration_evidence, frame_len_from_prefix_v1,
};
use crate::generation::{
    CATALOG_SLOT_NAMES, GENERATION_CATALOG_STAGING_FILE_NAME, GenerationCatalogReference,
    GenerationCatalogSnapshot, GenerationCodecError, GenerationInventory,
    MAX_GENERATION_CATALOG_BYTES, MAX_SEALED_GENERATIONS, ROTATION_INTENT_FILE_NAME,
    RotationIntent, SEALED_JOURNAL_STAGING_FILE_NAME, SealedGeneration, StreamingCrc32c,
    decode_catalog, decode_rotation_intent, encode_catalog, encode_rotation_intent,
    parse_active_checkpoint_generation_name, parse_active_journal_generation_name,
    parse_sealed_generation_name, sealed_journal_file_name,
};
use crate::pressure::is_storage_pressure;
use crate::recovery::{
    RECOVERY_SLOT_NAMES, RecoveryArtifact, RecoveryCodecError, RecoveryReference,
    decode_recovery_state, encode_recovery_state,
};
use crate::retry::{
    RetryArtifactReference, RetryStateCodecError, decode_retry_state_at_slot, encode_retry_state,
};
use crate::{
    ACTIVE_CHECKPOINT_FILE_NAME, ACTIVE_JOURNAL_FILE_NAME, ACTIVE_JOURNAL_GENERATION,
    ActiveJournalError, ActiveJournalInspection, ActiveJournalLimits, ActiveJournalOpenMode,
    DurableCutoff, JournalV1Error, PreparedFrameV1, StoreWriteState,
};
use crate::{
    MAX_PERSISTED_RETRY_ENTRIES, MAX_RETRY_STATE_BYTES, PendingRetryOutcome,
    RECOVERY_STAGING_FILE_NAME, RECOVERY_STATE_LEN, RecoveryReport, RetryPersistenceOptions,
    RetryStateReference, RetryStateSnapshot,
};
use och_core::{
    CollectionEnvelope, DeclarationEvidence, DeclarationRevision, DeclaredCollectionEnvelope,
    ModelError, SeriesBinding, SeriesDeclaration, SeriesDeclarationPayload, SeriesId,
    SeriesRegistry, SeriesRegistryLimits, SeriesRegistrySnapshot, SeriesRetirement, StoreId,
};
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

/// Exact never-renamed store-level writer lock artifact.
pub const STORE_LOCK_FILE_NAME: &str = "store-v1.lock";
/// Exact Store Format V1 reset marker artifact.
pub const STORE_FORMAT_FILE_NAME: &str = "store-format-v1.och";
/// Exact Store Format V1 marker publication staging artifact.
pub const STORE_FORMAT_STAGING_FILE_NAME: &str = "store-format-v1.staging";
/// Exact Store Format V1 marker magic.
pub const STORE_FORMAT_MAGIC: [u8; 8] = *b"OCHFMT01";
/// Current and sole Store Format marker version.
pub const STORE_FORMAT_VERSION: u16 = 1;
/// Exact Store Format V1 marker length.
pub const STORE_FORMAT_LEN: usize = 32;
const STORE_FORMAT_RECORD_LEN: u16 = 32;
/// Exact first reusable manifest slot.
pub const MANIFEST_SLOT_0_FILE_NAME: &str = "manifest-v1-slot-0.och";
/// Exact second reusable manifest slot.
pub const MANIFEST_SLOT_1_FILE_NAME: &str = "manifest-v1-slot-1.och";
/// Exact first reusable registry snapshot slot.
pub const REGISTRY_SLOT_0_FILE_NAME: &str = "series-registry-v1-slot-0.och";
/// Exact second reusable registry snapshot slot.
pub const REGISTRY_SLOT_1_FILE_NAME: &str = "series-registry-v1-slot-1.och";
/// Exact third reusable registry snapshot slot.
pub const REGISTRY_SLOT_2_FILE_NAME: &str = "series-registry-v1-slot-2.och";
/// Exact fixed manifest staging artifact.
pub const MANIFEST_STAGING_FILE_NAME: &str = "manifest-v1.staging";
/// Exact fixed registry staging artifact.
pub const REGISTRY_STAGING_FILE_NAME: &str = "series-registry-v1.staging";
/// Exact first reusable durable retry snapshot slot.
pub const RETRY_SLOT_0_FILE_NAME: &str = "retry-state-v1-slot-0.och";
/// Exact second reusable durable retry snapshot slot.
pub const RETRY_SLOT_1_FILE_NAME: &str = "retry-state-v1-slot-1.och";
/// Exact third reusable durable retry snapshot slot.
pub const RETRY_SLOT_2_FILE_NAME: &str = "retry-state-v1-slot-2.och";
/// Exact fixed durable retry snapshot staging artifact.
pub const RETRY_STAGING_FILE_NAME: &str = "retry-state-v1.staging";
/// Hard maximum persisted registry series, including tombstones.
pub const MAX_PERSISTED_REGISTRY_SERIES: usize = 4_096;
/// Hard maximum persisted declaration revisions across one registry.
pub const MAX_PERSISTED_REGISTRY_REVISIONS: usize = 16_384;
/// Hard maximum bytes in one registry snapshot artifact.
pub const MAX_REGISTRY_SNAPSHOT_BYTES: usize = 64 * 1_024 * 1_024;

const MANIFEST_MAGIC: [u8; 8] = *b"OCHMAN01";
const MANIFEST_VERSION: u16 = 1;
const MANIFEST_LEN: usize = 160;
const REGISTRY_MAGIC: [u8; 8] = *b"OCHREG01";
const REGISTRY_VERSION: u16 = 1;
const REGISTRY_HEADER_LEN: usize = 64;
const REGISTRY_HEADER_LEN_U16: u16 = 64;
const REGISTRY_CRC_LEN: usize = 4;
const MANIFEST_SLOT_NAMES: [&str; 2] = [MANIFEST_SLOT_0_FILE_NAME, MANIFEST_SLOT_1_FILE_NAME];
const REGISTRY_SLOT_NAMES: [&str; 3] = [
    REGISTRY_SLOT_0_FILE_NAME,
    REGISTRY_SLOT_1_FILE_NAME,
    REGISTRY_SLOT_2_FILE_NAME,
];
const RETRY_SLOT_NAMES: [&str; 3] = [
    RETRY_SLOT_0_FILE_NAME,
    RETRY_SLOT_1_FILE_NAME,
    RETRY_SLOT_2_FILE_NAME,
];
const MAX_INVENTORY_ENTRIES: usize = 91;

#[cfg(test)]
std::thread_local! {
    static PUBLISH_FAULT: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
    static PUBLISH_FAULT_KIND: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn set_publish_fault(code: u8) {
    PUBLISH_FAULT.with(|fault| fault.set(code));
    PUBLISH_FAULT_KIND.with(|kind| kind.set(0));
}

#[cfg(test)]
fn set_pressure_fault(code: u8, kind: ErrorKind) {
    let encoded = match kind {
        ErrorKind::StorageFull => 1,
        ErrorKind::QuotaExceeded => 2,
        _ => 0,
    };
    PUBLISH_FAULT.with(|fault| fault.set(code));
    PUBLISH_FAULT_KIND.with(|stored| stored.set(encoded));
}

#[cfg(test)]
fn take_publish_fault(code: u8) -> Option<(ErrorKind, Option<i32>)> {
    PUBLISH_FAULT.with(|fault| {
        if fault.get() == code {
            fault.set(0);
            let encoded = PUBLISH_FAULT_KIND.with(|kind| kind.replace(0));
            Some(match encoded {
                1 => (ErrorKind::StorageFull, Some(28)),
                2 => (ErrorKind::QuotaExceeded, Some(122)),
                _ => (ErrorKind::Other, None),
            })
        } else {
            None
        }
    })
}

/// Explicit bounded canonical registry persistence input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryPersistenceOptions {
    limits: SeriesRegistryLimits,
}

impl RegistryPersistenceOptions {
    /// Validates finite hard persistence bounds for an empty/new registry.
    ///
    /// # Errors
    ///
    /// Refuses limits above the persisted series or revision hard maximum.
    pub const fn new(limits: SeriesRegistryLimits) -> Result<Self, ManifestStoreError> {
        if limits.max_series() > MAX_PERSISTED_REGISTRY_SERIES
            || limits.max_declaration_revisions() > MAX_PERSISTED_REGISTRY_REVISIONS
        {
            return Err(ManifestStoreError::InvalidOptions);
        }
        Ok(Self { limits })
    }

    /// Returns configured canonical registry limits.
    #[must_use]
    pub const fn limits(&self) -> SeriesRegistryLimits {
        self.limits
    }
}

/// Validated manifest-rooted blocking store configuration.
pub struct ManifestStoreConfig {
    directory: PathBuf,
    store_id: StoreId,
    mode: ActiveJournalOpenMode,
    journal_limits: ActiveJournalLimits,
    registry: RegistryPersistenceOptions,
    retry: RetryPersistenceOptions,
}

impl ManifestStoreConfig {
    /// Validates bounded store, active-journal, and registry options.
    ///
    /// # Errors
    ///
    /// Returns a path-free refusal before any filesystem mutation.
    pub fn new(
        directory: PathBuf,
        store_id: StoreId,
        mode: ActiveJournalOpenMode,
        journal_limits: ActiveJournalLimits,
        registry: RegistryPersistenceOptions,
        retry: RetryPersistenceOptions,
    ) -> Result<Self, ManifestStoreError> {
        ActiveJournalConfig::new(directory.clone(), store_id, mode, journal_limits)
            .map_err(ManifestStoreError::Active)?;
        if registry.limits.max_series() > MAX_PERSISTED_REGISTRY_SERIES
            || registry.limits.max_declaration_revisions() > MAX_PERSISTED_REGISTRY_REVISIONS
        {
            return Err(ManifestStoreError::InvalidOptions);
        }
        Ok(Self {
            directory,
            store_id,
            mode,
            journal_limits,
            registry,
            retry,
        })
    }
}

/// Filesystem operation attached to path-free manifest I/O evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestIoOperation {
    /// Open or inspect the configured directory.
    OpenDirectory,
    /// Read the bounded non-recursive store inventory.
    ReadInventory,
    /// Open an existing fixed artifact.
    OpenArtifact,
    /// Create a new fixed artifact.
    CreateArtifact,
    /// Acquire the stable store-level lock.
    LockStore,
    /// Read bounded artifact bytes.
    Read,
    /// Write a fixed staging artifact.
    Write,
    /// Synchronize a staging or published artifact.
    SyncArtifact,
    /// Atomically publish a reusable slot.
    Publish,
    /// Remove a proven redundant artifact.
    Remove,
    /// Synchronize directory entries.
    SyncDirectory,
    /// Read artifact metadata.
    Metadata,
}

/// Path-free standard-library error evidence for manifest operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestIoEvidence {
    operation: ManifestIoOperation,
    kind: ErrorKind,
    raw_os_error: Option<i32>,
}

impl ManifestIoEvidence {
    /// Returns the failed operation.
    #[must_use]
    pub const fn operation(self) -> ManifestIoOperation {
        self.operation
    }

    /// Returns the standard-library error kind.
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

/// Closed manifest, registry, genesis, publication, and lifecycle refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestStoreError {
    /// Configuration or a hard bound is invalid.
    InvalidOptions,
    /// Durable evidence does not belong to the current Store Format V1 epoch.
    UnsupportedStoreFormat,
    /// Another process or handle retains the stable store lock.
    AlreadyOpen,
    /// Store inventory contains unknown, excessive, or unsafe evidence.
    InvalidInventory,
    /// A manifest slot is invalid, ambiguous, or inconsistent.
    InvalidManifest,
    /// A registry snapshot is invalid, ambiguous, or inconsistent.
    InvalidRegistry,
    /// A durable retry snapshot is invalid, ambiguous, or inconsistent.
    InvalidRetry,
    /// Generation catalog, sealed range, or rotation intent is invalid.
    InvalidGeneration,
    /// All 64 sealed-generation catalog entries are retained.
    GenerationCatalogFull,
    /// The configured store identity differs from durable evidence.
    StoreMismatch,
    /// The admission does not carry an exact retained historical declaration.
    HistoricalDeclarationMismatch,
    /// A fixed staging artifact proves an interrupted publication.
    InterruptedPublication,
    /// A prior non-pressure mutation failure terminally faulted this authority.
    Faulted,
    /// A prior mutating boundary observed storage pressure; validated reopen is required.
    ReopenRequired,
    /// Manifest or registry generation cannot advance.
    GenerationExhausted,
    /// Canonical lifecycle semantics refused the requested operation.
    Model(ModelError),
    /// Active-journal ownership or mechanical durability refused.
    Active(ActiveJournalError),
    /// Generic path-free filesystem evidence.
    Io(ManifestIoEvidence),
    /// A store-owned mutating boundary observed normalized storage pressure.
    StoragePressure(ManifestIoEvidence),
}

impl fmt::Display for ManifestStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidOptions => "invalid manifest store options",
            Self::UnsupportedStoreFormat => "unsupported store format",
            Self::AlreadyOpen => "manifest store is already open",
            Self::InvalidInventory => "invalid manifest store inventory",
            Self::InvalidManifest => "invalid manifest evidence",
            Self::InvalidRegistry => "invalid registry evidence",
            Self::InvalidRetry => "invalid durable retry evidence",
            Self::InvalidGeneration => "invalid journal generation evidence",
            Self::GenerationCatalogFull => "sealed generation catalog is full",
            Self::StoreMismatch => "manifest store identity mismatch",
            Self::HistoricalDeclarationMismatch => "historical declaration mismatch",
            Self::InterruptedPublication => "interrupted metadata publication",
            Self::Faulted => "manifest store authority is faulted",
            Self::ReopenRequired => "manifest store requires validated reopen",
            Self::GenerationExhausted => "manifest store generation exhausted",
            Self::Model(_) => "canonical registry operation refused",
            Self::Active(_) => "active journal operation refused",
            Self::Io(_) => "manifest store I/O failed",
            Self::StoragePressure(_) => "manifest store storage pressure",
        })
    }
}

impl Error for ManifestStoreError {}

/// Additive path- and content-free classification of a store-open refusal.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestOpenClassification {
    /// Supplied options or hard bounds were invalid.
    InvalidOptions,
    /// Durable evidence is outside the current Store Format V1 epoch.
    UnsupportedFormat,
    /// A stable or active writer lock is already retained.
    AlreadyOpen,
    /// The bounded directory inventory is unsafe or inconsistent.
    InvalidInventory,
    /// Current durable authority is corrupt, ambiguous, or scope-mismatched.
    CorruptAuthority,
    /// A bounded publication is incomplete and cannot be guessed.
    InterruptedPublication,
    /// A fixed durable capacity or generation bound was exhausted.
    Capacity,
    /// The live authority was already terminally faulted.
    Faulted,
    /// A live handle observed storage pressure and must be reopened.
    ReopenRequired,
    /// A store-owned mutating boundary observed normalized storage pressure.
    StoragePressure,
    /// Canonical model semantics refused the request.
    Model,
    /// A path-free filesystem operation failed.
    Io,
}

impl ManifestStoreError {
    /// Returns an additive sanitized open/corruption classification.
    ///
    /// The original error remains the exact source authority. This view carries
    /// no paths, payloads, canonical content, handles, or unbounded strings.
    #[must_use]
    pub const fn open_classification(self) -> ManifestOpenClassification {
        match self {
            Self::InvalidOptions => ManifestOpenClassification::InvalidOptions,
            Self::UnsupportedStoreFormat => ManifestOpenClassification::UnsupportedFormat,
            Self::AlreadyOpen | Self::Active(ActiveJournalError::AlreadyOpen) => {
                ManifestOpenClassification::AlreadyOpen
            }
            Self::InvalidInventory => ManifestOpenClassification::InvalidInventory,
            Self::InterruptedPublication => ManifestOpenClassification::InterruptedPublication,
            Self::GenerationCatalogFull | Self::GenerationExhausted => {
                ManifestOpenClassification::Capacity
            }
            Self::Faulted | Self::Active(ActiveJournalError::Faulted) => {
                ManifestOpenClassification::Faulted
            }
            Self::ReopenRequired | Self::Active(ActiveJournalError::ReopenRequired) => {
                ManifestOpenClassification::ReopenRequired
            }
            Self::StoragePressure(_) | Self::Active(ActiveJournalError::StoragePressure(_)) => {
                ManifestOpenClassification::StoragePressure
            }
            Self::Model(_) => ManifestOpenClassification::Model,
            Self::Io(_) | Self::Active(ActiveJournalError::Io(_)) => ManifestOpenClassification::Io,
            Self::InvalidManifest
            | Self::InvalidRegistry
            | Self::InvalidRetry
            | Self::InvalidGeneration
            | Self::StoreMismatch
            | Self::HistoricalDeclarationMismatch
            | Self::Active(_) => ManifestOpenClassification::CorruptAuthority,
        }
    }
}

impl From<ActiveJournalError> for ManifestStoreError {
    fn from(error: ActiveJournalError) -> Self {
        Self::Active(error)
    }
}

/// Manifest-backed committed state proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestCommit {
    manifest_generation: u64,
    registry_generation: u64,
    registry_slot: u8,
    durable_cutoff: DurableCutoff,
    retry_state: RetryStateReference,
    sequence_floor: u64,
    catalog: Option<GenerationCatalogReference>,
}

impl ManifestCommit {
    /// Returns the committed manifest generation.
    #[must_use]
    pub const fn manifest_generation(self) -> u64 {
        self.manifest_generation
    }

    /// Returns the committed registry snapshot generation.
    #[must_use]
    pub const fn registry_generation(self) -> u64 {
        self.registry_generation
    }

    /// Returns the committed registry slot in `0..3`.
    #[must_use]
    pub const fn registry_slot(self) -> u8 {
        self.registry_slot
    }

    /// Returns the exact mechanical cutoff named by this manifest.
    #[must_use]
    pub const fn durable_cutoff(self) -> DurableCutoff {
        self.durable_cutoff
    }

    /// Returns the mandatory committed durable retry snapshot identity.
    #[must_use]
    pub const fn retry_state(self) -> RetryStateReference {
        self.retry_state
    }

    /// Returns the exclusive append-sequence floor of the active generation.
    #[must_use]
    pub const fn sequence_floor(self) -> u64 {
        self.sequence_floor
    }

    /// Returns committed Generation Catalog V1 identity after first rotation.
    #[must_use]
    pub const fn generation_catalog(self) -> Option<GenerationCatalogReference> {
        self.catalog
    }

    #[cfg(test)]
    pub(crate) const fn from_parts(
        manifest_generation: u64,
        registry_generation: u64,
        registry_slot: u8,
        durable_cutoff: DurableCutoff,
        retry_state: RetryStateReference,
    ) -> Self {
        Self::from_generation_parts(
            manifest_generation,
            registry_generation,
            registry_slot,
            durable_cutoff,
            retry_state,
            0,
            None,
        )
    }

    pub(crate) const fn from_generation_parts(
        manifest_generation: u64,
        registry_generation: u64,
        registry_slot: u8,
        durable_cutoff: DurableCutoff,
        retry_state: RetryStateReference,
        sequence_floor: u64,
        catalog: Option<GenerationCatalogReference>,
    ) -> Self {
        Self {
            manifest_generation,
            registry_generation,
            registry_slot,
            durable_cutoff,
            retry_state,
            sequence_floor,
            catalog,
        }
    }
}

/// Sanitized bounded manifest-rooted store inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestStoreInspection {
    active: ActiveJournalInspection,
    committed: ManifestCommit,
    generations: GenerationInventory,
    recovery: Option<RecoveryReport>,
    write_state: StoreWriteState,
}

impl ManifestStoreInspection {
    /// Returns active-journal mechanical state.
    #[must_use]
    pub const fn active(self) -> ActiveJournalInspection {
        self.active
    }

    /// Returns current manifest-backed committed state.
    #[must_use]
    pub const fn committed(self) -> ManifestCommit {
        self.committed
    }

    /// Returns bounded path-free active and sealed generation facts.
    #[must_use]
    pub const fn generations(self) -> GenerationInventory {
        self.generations
    }

    /// Returns the latest manifest-bound durable recovery event, if any.
    ///
    /// This is retained event evidence, not proof that recovery occurred during
    /// the current open.
    #[must_use]
    pub const fn latest_recovery(self) -> Option<RecoveryReport> {
        self.recovery
    }

    /// Returns volatile write custody for this composed live handle.
    #[must_use]
    pub const fn write_state(self) -> StoreWriteState {
        self.write_state
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RegistryReference {
    slot: u8,
    generation: u64,
    length: u64,
    checksum: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ManifestRecord {
    generation: u64,
    registry: RegistryReference,
    cutoff: DurableCutoff,
    retry: RetryArtifactReference,
    recovery: Option<RecoveryReference>,
    sequence_floor: u64,
    catalog: Option<GenerationCatalogReference>,
}

struct PreparedManifestPublication {
    record: ManifestRecord,
    target: usize,
    bytes: Vec<u8>,
}

struct PreparedRetryPublication {
    reference: RetryArtifactReference,
    bytes: Vec<u8>,
}

struct PreparedRegistryPublication {
    reference: RegistryReference,
    bytes: Vec<u8>,
}

struct PreparedCatalogPublication {
    snapshot: GenerationCatalogSnapshot,
    bytes: Vec<u8>,
}

struct PreparedRecoveryPublication {
    artifact: RecoveryArtifact,
    bytes: [u8; RECOVERY_STATE_LEN],
}

struct RotationPlan {
    intent: RotationIntent,
    source_generation: u64,
    sealed: SealedGeneration,
    successor_config: ActiveJournalConfig,
    catalog: PreparedCatalogPublication,
    manifest: PreparedManifestPublication,
}

struct PendingCommitPlan {
    cutoff: DurableCutoff,
    retry: RetryStateSnapshot,
    retry_publication: PreparedRetryPublication,
    manifest: PreparedManifestPublication,
}

impl ManifestRecord {
    const fn commit(self) -> ManifestCommit {
        ManifestCommit::from_generation_parts(
            self.generation,
            self.registry.generation,
            self.registry.slot,
            self.cutoff,
            self.retry.public,
            self.sequence_floor,
            self.catalog,
        )
    }
}

/// Sole blocking owner of stable locking, active journal, registry, and manifests.
pub struct ManifestStore {
    directory_path: PathBuf,
    directory: File,
    _store_lock: File,
    journal: ActiveJournal,
    registry: SeriesRegistry,
    retry: RetryStateSnapshot,
    catalog: GenerationCatalogSnapshot,
    recovery_report: Option<RecoveryReport>,
    manifest_slots: [Option<ManifestRecord>; 2],
    current_slot: usize,
    current: ManifestRecord,
    write_state: StoreWriteState,
}

impl ManifestStore {
    /// Creates or opens one current Store Format V1 manifest-rooted store.
    ///
    /// Read-only format preflight precedes stable lock creation or acquisition.
    /// Only exact current genesis and rotation publication windows may converge.
    ///
    /// # Errors
    ///
    /// Returns a bounded path-free refusal for format, lock, inventory,
    /// identity, cutoff, registry, or I/O failure.
    #[allow(clippy::too_many_lines)]
    pub fn open(config: ManifestStoreConfig) -> Result<Self, ManifestStoreError> {
        let directory = open_directory(&config.directory)?;
        let preflight = preflight_store_format(&config)?;
        preflight_genesis_records(&config)?;
        let lock_path = config.directory.join(STORE_LOCK_FILE_NAME);
        let mut lock_options = OpenOptions::new();
        lock_options.read(true).write(true).truncate(false);
        if preflight == FormatPreflight::EmptyCreate {
            lock_options.create_new(true);
        }
        let lock_operation = if preflight == FormatPreflight::EmptyCreate {
            ManifestIoOperation::CreateArtifact
        } else {
            ManifestIoOperation::OpenArtifact
        };
        let store_lock = lock_options
            .open(&lock_path)
            .map_err(|error| manifest_io(lock_operation, &error))?;
        lock_store(&store_lock)?;
        if preflight == FormatPreflight::EmptyCreate {
            directory
                .sync_all()
                .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))?;
            publish_store_format_marker(&config.directory, &directory, config.store_id)?;
        } else if preflight == FormatPreflight::MarkerStaging {
            converge_store_format_marker(&config.directory, &directory, config.store_id)?;
        }
        let repeated = preflight_store_format(&config)?;
        if repeated != FormatPreflight::Current {
            return Err(ManifestStoreError::UnsupportedStoreFormat);
        }
        let mut inventory = inspect_inventory(&config.directory)?;
        if (inventory.registry_staging || inventory.retry_staging || inventory.rotation_staging)
            && !inventory.rotation_intent
        {
            return Err(ManifestStoreError::InterruptedPublication);
        }
        let mut manifest_slots = read_manifest_slots(&config.directory, config.store_id)?;
        let mut committed_intent = None;
        if inventory.rotation_intent {
            let recovery_inventory = read_recovery_inventory(&config.directory, config.store_id)?;
            validate_recovery_manifest_progression(manifest_slots, &recovery_inventory)?;
            validate_no_pending_recovery(manifest_slots, &recovery_inventory)?;
            let (_, current) = select_current_manifest(manifest_slots)?
                .ok_or(ManifestStoreError::InvalidGeneration)?;
            let intent = read_rotation_intent(&config.directory, config.store_id)?;
            if current.cutoff.journal().generation() == intent.source_generation {
                rollback_uncommitted_rotation(
                    &config,
                    &directory,
                    manifest_slots,
                    current,
                    intent,
                )?;
                inventory = inspect_inventory(&config.directory)?;
                manifest_slots = read_manifest_slots(&config.directory, config.store_id)?;
            } else {
                if current.cutoff.journal().generation() != intent.successor_generation {
                    return Err(ManifestStoreError::InvalidGeneration);
                }
                validate_committed_rotation_transition(
                    &config.directory,
                    manifest_slots,
                    current,
                    intent,
                    config.store_id,
                )?;
                committed_intent = Some(intent);
            }
        }
        if let Some((current_slot, current)) = select_current_manifest(manifest_slots)? {
            if config.mode != ActiveJournalOpenMode::OpenExisting {
                return Err(ManifestStoreError::InvalidInventory);
            }
            let store = Self::open_committed(
                config,
                directory,
                store_lock,
                manifest_slots,
                current_slot,
                current,
                committed_intent,
            )?;
            if let Some(intent) = committed_intent {
                cleanup_committed_rotation(
                    &store.directory_path,
                    &store.directory,
                    intent.source_generation,
                )?;
                remove_unreferenced_retry_slots(
                    &store.directory_path,
                    &store.directory,
                    store.manifest_slots,
                )?;
            }
            remove_unreferenced_catalog_slots(
                &store.directory_path,
                &store.directory,
                store.manifest_slots,
            )?;
            remove_unreferenced_recovery_slots(
                &store.directory_path,
                &store.directory,
                store.manifest_slots,
                store.current.cutoff.journal().store_id(),
            )?;
            Ok(store)
        } else {
            Self::initialize_genesis(config, directory, store_lock, &inventory, manifest_slots)
        }
    }

    #[allow(clippy::large_types_passed_by_value)]
    fn open_committed(
        config: ManifestStoreConfig,
        directory: File,
        store_lock: File,
        manifest_slots: [Option<ManifestRecord>; 2],
        current_slot: usize,
        current: ManifestRecord,
        rotation_intent: Option<RotationIntent>,
    ) -> Result<Self, ManifestStoreError> {
        let active_config = ActiveJournalConfig::new(
            config.directory.clone(),
            config.store_id,
            ActiveJournalOpenMode::OpenExisting,
            config.journal_limits,
        )
        .map_err(ManifestStoreError::Active)?
        .manifest_existing()
        .manifest_generation(
            current.cutoff.journal().generation(),
            current.sequence_floor,
        )
        .map_err(ManifestStoreError::Active)?;
        let (journal, recovery_plan) =
            ActiveJournal::open_manifest_root(active_config, current.cutoff).map_err(|error| {
                match error {
                    ManifestRootOpenError::RootMismatch => ManifestStoreError::InvalidManifest,
                    ManifestRootOpenError::Active(error) => ManifestStoreError::Active(error),
                }
            })?;
        // The root scan above is read-only and retains the writer lock. All
        // remaining semantic authority is now proven while both stable and
        // active locks remain held before the single-use plan can mutate bytes.
        let registry =
            read_referenced_registry(&config.directory, current.registry, config.store_id)?;
        validate_registry_inventory(
            &config.directory,
            manifest_slots,
            current.registry,
            config.store_id,
        )?;
        let catalog = match current.catalog {
            Some(reference) => {
                read_referenced_catalog(&config.directory, reference, config.store_id)?
            }
            None => GenerationCatalogSnapshot::empty(config.store_id),
        };
        validate_catalog_inventory(&config.directory, manifest_slots, config.store_id)?;
        let inventory = inspect_inventory(&config.directory)?;
        validate_generation_inventory(&inventory, current, &catalog, rotation_intent)?;
        let recovery_inventory = read_recovery_inventory(&config.directory, config.store_id)?;
        validate_recovery_manifest_progression(manifest_slots, &recovery_inventory)?;
        validate_recovered_declarations(&registry, journal.recovered_records())
            .map_err(|_| ManifestStoreError::InvalidRegistry)?;
        let retry = read_referenced_retry(
            &config.directory,
            current,
            config.store_id,
            config.retry,
            &catalog,
        )?;
        validate_retry_inventory(
            &config.directory,
            manifest_slots,
            config.store_id,
            config.retry,
            rotation_intent.is_some(),
        )?;
        validate_recovery_report_coverage(
            manifest_slots,
            &recovery_inventory,
            current,
            &catalog,
            &journal,
        )?;
        let recovery_report = current
            .recovery
            .map(|reference| referenced_recovery(&recovery_inventory, reference))
            .transpose()?;
        let mut store = Self {
            directory_path: config.directory,
            directory,
            _store_lock: store_lock,
            journal,
            registry,
            retry,
            catalog,
            recovery_report,
            manifest_slots,
            current_slot,
            current,
            write_state: StoreWriteState::Writable,
        };
        store.converge_recovery(&recovery_inventory, recovery_plan)?;
        if store.current.recovery.is_some() {
            store
                .directory
                .sync_all()
                .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))?;
        }
        Ok(store)
    }

    #[allow(clippy::too_many_lines)]
    fn converge_recovery(
        &mut self,
        inventory: &RecoveryInventoryState,
        recovery_plan: Option<ActiveRecoveryPlan>,
    ) -> Result<(), ManifestStoreError> {
        let referenced = self
            .manifest_slots
            .iter()
            .flatten()
            .filter_map(|manifest| manifest.recovery)
            .collect::<Vec<_>>();
        let target_slot = (0_u8..3)
            .find(|slot| !referenced.iter().any(|reference| reference.slot == *slot))
            .ok_or(ManifestStoreError::InvalidManifest)?;
        let latest_generation = self
            .recovery_report
            .map_or(0, RecoveryReport::report_generation);
        let mut candidate = None;
        for artifact in inventory.slots.into_iter().flatten() {
            if referenced.contains(&artifact.reference) {
                continue;
            }
            if latest_generation > 0 && artifact.report.report_generation() < latest_generation {
                continue;
            }
            if candidate.replace(artifact).is_some() {
                return Err(ManifestStoreError::InvalidManifest);
            }
        }
        if inventory.staging.is_some() && candidate.is_some() {
            return Err(ManifestStoreError::InvalidManifest);
        }

        let expected_report_generation = latest_generation
            .checked_add(1)
            .ok_or(ManifestStoreError::GenerationExhausted)?;
        let mut artifact = if let Some(report) = inventory.staging {
            let bytes = encode_recovery_state(report).map_err(map_recovery_codec)?;
            Some((
                RecoveryArtifact {
                    reference: RecoveryReference {
                        slot: target_slot,
                        checksum: crc32c(&bytes),
                    },
                    report,
                },
                true,
            ))
        } else {
            candidate.map(|artifact| (artifact, false))
        };
        let mut prepared_report = None;

        if let Some((intent, from_staging)) = artifact {
            if intent.reference.slot != target_slot
                || intent.report.report_generation() != expected_report_generation
                || !report_matches_source(
                    intent.report,
                    self.current,
                    self.current
                        .generation
                        .checked_add(1)
                        .ok_or(ManifestStoreError::GenerationExhausted)?,
                )
            {
                return Err(ManifestStoreError::InvalidManifest);
            }
            match &recovery_plan {
                Some(plan)
                    if intent.report.original_journal_length() == plan.original_length()
                        && intent.report.classification() == plan.classification() => {}
                Some(_) => return Err(ManifestStoreError::InvalidManifest),
                None if from_staging => return Err(ManifestStoreError::InterruptedPublication),
                None => {}
            }
            artifact = Some((intent, from_staging));
        }

        if artifact.is_none() {
            if inventory.manifest_staging.is_some() {
                return Err(ManifestStoreError::InterruptedPublication);
            }
            let Some(ref plan) = recovery_plan else {
                remove_unreferenced_recovery_slots(
                    &self.directory_path,
                    &self.directory,
                    self.manifest_slots,
                    self.current.cutoff.journal().store_id(),
                )?;
                return Ok(());
            };
            let report = RecoveryReport::new(
                self.current.cutoff.journal().store_id(),
                expected_report_generation,
                self.current.generation,
                manifest_checksum(self.current),
                self.current.cutoff,
                self.current.sequence_floor,
                plan.original_length(),
                plan.classification(),
            )
            .map_err(map_recovery_codec)?;
            let prepared = Self::prepare_recovery_report(target_slot, report)?;
            artifact = Some((prepared.artifact, false));
            prepared_report = Some(prepared);
        }

        let (artifact, from_staging) = artifact.ok_or(ManifestStoreError::InvalidManifest)?;
        if recovery_plan.is_some() && inventory.manifest_staging.is_some() {
            return Err(ManifestStoreError::InvalidManifest);
        }
        let next_generation = self
            .current
            .generation
            .checked_add(1)
            .ok_or(ManifestStoreError::GenerationExhausted)?;
        let next = ManifestRecord {
            generation: next_generation,
            registry: self.current.registry,
            cutoff: self.current.cutoff,
            retry: self.current.retry,
            recovery: Some(artifact.reference),
            sequence_floor: self.current.sequence_floor,
            catalog: self.current.catalog,
        };
        let prepared_manifest = self.prepare_manifest(next)?;
        if let Some(staging) = inventory.manifest_staging
            && staging != next
        {
            return Err(ManifestStoreError::InvalidManifest);
        }
        if from_staging {
            converge_recovery_staging(
                &self.directory_path,
                &self.directory,
                artifact,
                self.current.cutoff.journal().store_id(),
            )?;
        } else if let Some(prepared) = &prepared_report {
            self.publish_prepared_recovery_report(prepared)?;
        }
        self.directory
            .sync_all()
            .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))?;
        if let Some(plan) = recovery_plan {
            self.journal.apply_recovery(plan)?;
        } else {
            self.journal
                .synchronize_recovery_cutoff(self.current.cutoff)?;
        }
        let slot = if inventory.manifest_staging.is_some() {
            converge_manifest_staging(
                &self.directory_path,
                &self.directory,
                1 - self.current_slot,
                next,
            )?
        } else {
            self.publish_prepared_manifest(&prepared_manifest)?
        };
        let mut slots = self.manifest_slots;
        slots[slot] = Some(next);
        self.manifest_slots = slots;
        self.current_slot = slot;
        self.current = next;
        self.recovery_report = Some(artifact.report);
        remove_unreferenced_catalog_slots(&self.directory_path, &self.directory, slots)?;
        remove_unreferenced_retry_slots(&self.directory_path, &self.directory, slots)?;
        remove_unreferenced_recovery_slots(
            &self.directory_path,
            &self.directory,
            slots,
            self.current.cutoff.journal().store_id(),
        )?;
        Ok(())
    }

    fn prepare_recovery_report(
        slot: u8,
        report: RecoveryReport,
    ) -> Result<PreparedRecoveryPublication, ManifestStoreError> {
        if slot >= 3 {
            return Err(ManifestStoreError::InvalidManifest);
        }
        let bytes = encode_recovery_state(report).map_err(map_recovery_codec)?;
        let artifact = RecoveryArtifact {
            reference: RecoveryReference {
                slot,
                checksum: crc32c(&bytes),
            },
            report,
        };
        Ok(PreparedRecoveryPublication { artifact, bytes })
    }

    fn publish_prepared_recovery_report(
        &self,
        prepared: &PreparedRecoveryPublication,
    ) -> Result<(), ManifestStoreError> {
        let artifact = prepared.artifact;
        let report = artifact.report;
        let slot = artifact.reference.slot;
        publish_reusable_slot(
            &self.directory_path,
            &self.directory,
            RECOVERY_STAGING_FILE_NAME,
            RECOVERY_SLOT_NAMES[usize::from(slot)],
            &prepared.bytes,
            RECOVERY_STATE_LEN,
            |candidate| {
                let decoded = decode_recovery_state(candidate, report.store_id())
                    .map_err(map_recovery_codec)?;
                if decoded != report || crc32c(candidate) != artifact.reference.checksum {
                    return Err(ManifestStoreError::InvalidManifest);
                }
                Ok(())
            },
        )?;
        Ok(())
    }

    fn initialize_genesis(
        config: ManifestStoreConfig,
        directory: File,
        store_lock: File,
        inventory: &Inventory,
        mut manifest_slots: [Option<ManifestRecord>; 2],
    ) -> Result<Self, ManifestStoreError> {
        validate_genesis_inventory(inventory)?;
        let (mode, strict_create) = if inventory.active_journals.is_empty() {
            (ActiveJournalOpenMode::CreateNew, true)
        } else {
            (ActiveJournalOpenMode::OpenExisting, false)
        };
        let active_config = ActiveJournalConfig::new(
            config.directory.clone(),
            config.store_id,
            mode,
            config.journal_limits,
        )
        .map_err(ManifestStoreError::Active)?;
        let active_config = if strict_create {
            active_config.manifest_create()
        } else {
            preflight_manifest_genesis(&config.directory, config.store_id)
                .map_err(map_manifest_genesis_preflight)?;
            active_config.manifest_genesis()
        };
        let journal = ActiveJournal::open(active_config)?;
        if !journal.recovered_records().is_empty()
            || journal.durable_cutoff() != genesis_placeholder(config.store_id)
        {
            return Err(ManifestStoreError::UnsupportedStoreFormat);
        }
        let registry = SeriesRegistry::new(config.store_id, config.registry.limits);
        let mut store = Self {
            directory_path: config.directory,
            directory,
            _store_lock: store_lock,
            journal,
            registry,
            retry: RetryStateSnapshot::empty(config.store_id, config.retry),
            catalog: GenerationCatalogSnapshot::empty(config.store_id),
            recovery_report: None,
            manifest_slots,
            current_slot: 0,
            current: ManifestRecord {
                generation: 0,
                registry: RegistryReference {
                    slot: 0,
                    generation: 0,
                    length: 0,
                    checksum: 0,
                },
                cutoff: genesis_placeholder(config.store_id),
                retry: RetryArtifactReference {
                    public: RetryStateReference::new(0, 0),
                    length: 0,
                    checksum: 0,
                },
                recovery: None,
                sequence_floor: 0,
                catalog: None,
            },
            write_state: StoreWriteState::Writable,
        };
        let registry_reference = if inventory.registry_slots == 0 {
            store.publish_registry_snapshot(1, &store.registry.snapshot())?
        } else {
            read_interrupted_genesis_registry(&store.directory_path, &store.registry.snapshot())?
        };
        let retry_reference = if inventory.retry_slots == 0 {
            let reference = RetryStateReference::new(0, 1);
            store.publish_retry_snapshot(
                reference,
                &RetryStateSnapshot::empty_persisted(
                    store.retry.store_id(),
                    store.retry.options(),
                    reference,
                ),
            )?
        } else {
            read_interrupted_genesis_retry(&store.directory_path, config.store_id, config.retry)?
        };
        let record = ManifestRecord {
            generation: 1,
            registry: registry_reference,
            cutoff: store.journal.durable_cutoff(),
            retry: retry_reference,
            recovery: None,
            sequence_floor: 0,
            catalog: None,
        };
        store.retry = read_referenced_retry(
            &store.directory_path,
            record,
            config.store_id,
            config.retry,
            &store.catalog,
        )?;
        let slot = store.publish_manifest(record)?;
        manifest_slots[slot] = Some(record);
        store.manifest_slots = manifest_slots;
        store.current_slot = slot;
        store.current = record;
        Ok(store)
    }

    /// Returns current bounded committed inspection.
    #[must_use]
    pub fn inspection(&self) -> ManifestStoreInspection {
        ManifestStoreInspection {
            active: self.journal.inspection(),
            committed: self.commit(),
            generations: GenerationInventory::from_catalog(
                self.journal.inspection().journal().generation(),
                &self.catalog,
            ),
            recovery: self.recovery_report,
            write_state: self.write_state,
        }
    }

    /// Returns decoded journal evidence without granting registry authority.
    #[must_use]
    pub fn recovered_records(&self) -> &[crate::RecoveredAdmissionV1] {
        self.journal.recovered_records()
    }

    /// Captures the complete deterministic canonical registry snapshot.
    #[must_use]
    pub fn registry_snapshot(&self) -> SeriesRegistrySnapshot {
        self.registry.snapshot()
    }

    /// Captures the immutable committed durable retry projection.
    #[must_use]
    pub fn retry_state_snapshot(&self) -> RetryStateSnapshot {
        self.retry.clone()
    }

    /// Returns the next writer-owned append sequence.
    ///
    /// # Errors
    ///
    /// Refuses a terminal fault or active-journal sequence exhaustion.
    pub fn next_append_sequence(&self) -> Result<crate::AppendSequenceV1, ManifestStoreError> {
        self.ensure_usable()?;
        self.journal
            .next_append_sequence()
            .map_err(ManifestStoreError::Active)
    }

    /// Validates one declaration against the exact retained historical authority.
    ///
    /// # Errors
    ///
    /// Refuses a terminal store or an unknown or altered historical declaration.
    pub fn preflight_historical_declaration(
        &self,
        declaration: &SeriesDeclaration,
    ) -> Result<(), ManifestStoreError> {
        self.ensure_usable()?;
        if self
            .registry
            .resolve(declaration.series_id(), declaration.revision())
            != Some(declaration)
        {
            return Err(ManifestStoreError::HistoricalDeclarationMismatch);
        }
        Ok(())
    }

    /// Validates historical declaration authority and appends one frame.
    ///
    /// # Errors
    ///
    /// Unknown or altered historical declarations refuse before journal bytes.
    pub fn append(&mut self, frame: &PreparedFrameV1) -> Result<u64, ManifestStoreError> {
        self.preflight_historical_declaration(frame.admission().declaration())?;
        match self.journal.append(frame) {
            Ok(end) => Ok(end),
            Err(error) => Err(self.record_active_error(error)),
        }
    }

    /// Reports whether a safely durable nonempty active generation must rotate
    /// before one exact encoded frame can append.
    ///
    /// # Errors
    ///
    /// Refuses a terminal store or a frame that cannot fit an empty generation.
    pub fn requires_rotation(&self, frame_len: usize) -> Result<bool, ManifestStoreError> {
        self.ensure_usable()?;
        let limits = self.journal.limits();
        let frame_len_u64 = u64::try_from(frame_len)
            .map_err(|_| ManifestStoreError::Active(ActiveJournalError::FrameTooLarge))?;
        if limits.max_active_records() == 0
            || (crate::JOURNAL_V1_HEADER_LEN as u64)
                .checked_add(frame_len_u64)
                .is_none_or(|length| length > limits.max_active_bytes())
        {
            return Err(ManifestStoreError::Active(
                ActiveJournalError::FrameTooLarge,
            ));
        }
        Ok(!self.journal.can_fit(frame_len))
    }

    /// Seals the exact fully durable nonempty active range and commits an empty
    /// successor generation through the current Manifest V1 root.
    ///
    /// # Errors
    ///
    /// Refuses without a new manifest when durability, bounds, catalog capacity,
    /// immutable readback, or any precommit publication step cannot be proved.
    /// A postcommit pressure refusal requires reopen; other failures fault it.
    pub fn rotate(&mut self) -> Result<ManifestCommit, ManifestStoreError> {
        self.ensure_usable()?;
        let plan = self.prepare_rotation()?;
        if let Err(error) =
            publish_rotation_intent(&self.directory_path, &self.directory, plan.intent)
        {
            return Err(self.record_terminal_error(error));
        }
        let result = self.complete_rotation(plan);
        match result {
            Ok(commit) => Ok(commit),
            Err(error) => Err(self.record_terminal_error(error)),
        }
    }

    fn prepare_rotation(&self) -> Result<RotationPlan, ManifestStoreError> {
        let inspection = self.journal.inspection();
        let source_cutoff = inspection.durable_cutoff();
        if inspection.active_records() == 0
            || source_cutoff != self.current.cutoff
            || inspection.last_append_sequence() != source_cutoff.append_sequence()
            || inspection.active_bytes() != source_cutoff.end_offset()
        {
            return Err(ManifestStoreError::InvalidGeneration);
        }
        if self.catalog.entries().len() >= MAX_SEALED_GENERATIONS {
            return Err(ManifestStoreError::GenerationCatalogFull);
        }
        let successor_generation = source_cutoff
            .journal()
            .generation()
            .checked_add(1)
            .ok_or(ManifestStoreError::GenerationExhausted)?;
        let catalog_generation = self.catalog.reference().map_or(Ok(1), |reference| {
            reference
                .generation()
                .checked_add(1)
                .ok_or(ManifestStoreError::GenerationExhausted)
        })?;
        let catalog_slot = self.select_catalog_candidate_slot()?;
        let manifest_generation = self
            .current
            .generation
            .checked_add(1)
            .ok_or(ManifestStoreError::GenerationExhausted)?;
        let intent = RotationIntent {
            store_id: source_cutoff.journal().store_id(),
            source_generation: source_cutoff.journal().generation(),
            successor_generation,
            sequence_cutoff: source_cutoff.append_sequence(),
            source_end_offset: source_cutoff.end_offset(),
            registry_generation: self.current.registry.generation,
            catalog_generation,
            source_checkpoint_generation: source_cutoff.checkpoint_generation(),
        };
        let sealed = prepare_sealed_generation(
            &self.directory_path,
            intent,
            self.current.sequence_floor,
            self.journal.limits(),
            &self.registry,
        )?;
        let provisional_reference =
            GenerationCatalogReference::new(catalog_slot, intent.catalog_generation, 1, 1);
        let provisional = self
            .catalog
            .advance(provisional_reference, sealed)
            .map_err(map_generation_codec)?;
        let catalog =
            self.prepare_catalog_snapshot(catalog_slot, intent.catalog_generation, &provisional)?;
        let successor_config = ActiveJournalConfig::new(
            self.directory_path.clone(),
            source_cutoff.journal().store_id(),
            ActiveJournalOpenMode::CreateNew,
            self.journal.limits(),
        )
        .map_err(ManifestStoreError::Active)?
        .manifest_create()
        .manifest_generation(intent.successor_generation, source_cutoff.append_sequence())
        .map_err(ManifestStoreError::Active)?;
        let successor_cutoff = DurableCutoff::from_manifest(
            source_cutoff.journal().store_id(),
            intent.successor_generation,
            1,
            source_cutoff.append_sequence(),
            crate::JOURNAL_V1_HEADER_LEN as u64,
        );
        let next = ManifestRecord {
            generation: manifest_generation,
            registry: self.current.registry,
            cutoff: successor_cutoff,
            retry: self.current.retry,
            recovery: self.current.recovery,
            sequence_floor: source_cutoff.append_sequence(),
            catalog: catalog.snapshot.reference(),
        };
        let manifest = self.prepare_manifest(next)?;
        Ok(RotationPlan {
            intent,
            source_generation: source_cutoff.journal().generation(),
            sealed,
            successor_config,
            catalog,
            manifest,
        })
    }

    fn complete_rotation(
        &mut self,
        plan: RotationPlan,
    ) -> Result<ManifestCommit, ManifestStoreError> {
        publish_sealed_generation(
            &self.directory_path,
            &self.directory,
            plan.intent,
            plan.sealed,
        )?;
        injected_rotation_fault(35, ManifestIoOperation::CreateArtifact)?;
        let successor =
            ActiveJournal::open(plan.successor_config).map_err(ManifestStoreError::Active)?;
        injected_rotation_fault(36, ManifestIoOperation::SyncArtifact)?;
        if successor.durable_cutoff() != plan.manifest.record.cutoff {
            return Err(ManifestStoreError::InvalidGeneration);
        }
        self.publish_prepared_catalog_snapshot(&plan.catalog)?;
        let slot = self.publish_prepared_manifest(&plan.manifest)?;
        injected_rotation_fault(38, ManifestIoOperation::Publish)?;

        let mut slots = self.manifest_slots;
        slots[slot] = Some(plan.manifest.record);
        self.journal = successor;
        self.catalog = plan.catalog.snapshot;
        self.manifest_slots = slots;
        self.current_slot = slot;
        self.current = plan.manifest.record;
        injected_rotation_fault(39, ManifestIoOperation::Remove)?;
        cleanup_committed_rotation(
            &self.directory_path,
            &self.directory,
            plan.source_generation,
        )?;
        remove_unreferenced_catalog_slots(
            &self.directory_path,
            &self.directory,
            self.manifest_slots,
        )?;
        remove_unreferenced_retry_slots(
            &self.directory_path,
            &self.directory,
            self.manifest_slots,
        )?;
        remove_unreferenced_recovery_slots(
            &self.directory_path,
            &self.directory,
            self.manifest_slots,
            self.current.cutoff.journal().store_id(),
        )?;
        Ok(self.commit())
    }

    /// Synchronizes the journal/checkpoint and commits their exact cutoff in a
    /// new manifest before returning.
    ///
    /// # Errors
    ///
    /// A publication failure returns no new committed proof. Pressure requires
    /// reopen; other mutation failures terminally fault this open authority.
    pub fn sync_pending(
        &mut self,
        pending: &[PendingRetryOutcome],
    ) -> Result<(ManifestCommit, RetryStateSnapshot), ManifestStoreError> {
        self.ensure_usable()?;
        if pending.len() > MAX_PERSISTED_RETRY_ENTRIES {
            return Err(ManifestStoreError::InvalidRetry);
        }
        let anticipated_cutoff = self
            .journal
            .pending_durable_cutoff()
            .map_err(ManifestStoreError::Active)?;
        if anticipated_cutoff == self.current.cutoff {
            if !pending.is_empty() {
                return Err(ManifestStoreError::InvalidRetry);
            }
            return Ok((self.commit(), self.retry.clone()));
        }
        // Prepare every bounded retry and manifest relationship and exact byte
        // before the journal/checkpoint durability transaction starts.
        let plan = self.prepare_synced_pending(anticipated_cutoff, pending)?;
        let cutoff = match self.journal.sync_pending() {
            Ok(cutoff) => cutoff,
            Err(error) => return Err(self.record_active_error(error)),
        };
        if cutoff != plan.cutoff {
            return Err(self.record_terminal_error(ManifestStoreError::InvalidRetry));
        }
        let result = self.commit_synced_pending(plan);
        match result {
            Ok(commit) => Ok(commit),
            Err(error) => Err(self.record_terminal_error(error)),
        }
    }

    fn prepare_synced_pending(
        &self,
        cutoff: DurableCutoff,
        pending: &[PendingRetryOutcome],
    ) -> Result<PendingCommitPlan, ManifestStoreError> {
        validate_pending_retry(self.current.cutoff, cutoff, pending)?;
        let generation = self
            .current
            .generation
            .checked_add(1)
            .ok_or(ManifestStoreError::GenerationExhausted)?;
        let retry_generation = self
            .current
            .retry
            .public
            .generation()
            .checked_add(1)
            .ok_or(ManifestStoreError::GenerationExhausted)?;
        let retry_slot = self.select_retry_candidate_slot()?;
        let retry_public = RetryStateReference::new(retry_slot, retry_generation);
        let anticipated = ManifestCommit::from_generation_parts(
            generation,
            self.current.registry.generation,
            self.current.registry.slot,
            cutoff,
            retry_public,
            self.current.sequence_floor,
            self.current.catalog,
        );
        let candidate = self
            .retry
            .advance(retry_public, pending, anticipated)
            .map_err(|_| ManifestStoreError::InvalidRetry)?;
        let retry_publication = self.prepare_retry_snapshot(retry_public, &candidate)?;
        let next = ManifestRecord {
            generation,
            registry: self.current.registry,
            cutoff,
            retry: retry_publication.reference,
            recovery: self.current.recovery,
            sequence_floor: self.current.sequence_floor,
            catalog: self.current.catalog,
        };
        let manifest = self.prepare_manifest(next)?;
        Ok(PendingCommitPlan {
            cutoff,
            retry: candidate,
            retry_publication,
            manifest,
        })
    }

    fn commit_synced_pending(
        &mut self,
        plan: PendingCommitPlan,
    ) -> Result<(ManifestCommit, RetryStateSnapshot), ManifestStoreError> {
        self.publish_prepared_retry_snapshot(&plan.retry, &plan.retry_publication)?;
        let committed = self.publish_and_adopt_prepared_manifest(&plan.manifest)?;
        self.retry = plan.retry;
        Ok((committed, self.retry.clone()))
    }

    /// Registers revision one and commits the complete resulting snapshot.
    ///
    /// # Errors
    ///
    /// Core refusal is non-mutating. Publication pressure requires reopen; other
    /// mutation failures fault the open authority and report no commit.
    pub fn register(
        &mut self,
        series_id: SeriesId,
        binding: SeriesBinding,
        payload: SeriesDeclarationPayload,
        evidence: DeclarationEvidence,
    ) -> Result<(SeriesDeclaration, ManifestCommit), ManifestStoreError> {
        self.apply_registry(|registry| registry.register(series_id, binding, payload, evidence))
    }

    /// Revises one series and commits the complete resulting snapshot.
    ///
    /// # Errors
    ///
    /// Core refusal is non-mutating. Publication pressure requires reopen; other
    /// mutation failures fault the open authority and report no commit.
    pub fn revise(
        &mut self,
        series_id: SeriesId,
        expected_revision: DeclarationRevision,
        payload: SeriesDeclarationPayload,
        evidence: DeclarationEvidence,
    ) -> Result<(SeriesDeclaration, ManifestCommit), ManifestStoreError> {
        self.apply_registry(|registry| {
            registry.revise(series_id, expected_revision, payload, evidence)
        })
    }

    /// Terminally retires one series and commits its complete tombstone state.
    ///
    /// # Errors
    ///
    /// Core refusal is non-mutating. Publication pressure requires reopen; other
    /// mutation failures fault the open authority and report no commit.
    pub fn retire(
        &mut self,
        series_id: SeriesId,
        expected_revision: DeclarationRevision,
        evidence: DeclarationEvidence,
    ) -> Result<(SeriesRetirement, ManifestCommit), ManifestStoreError> {
        self.apply_registry(|registry| registry.retire(series_id, expected_revision, evidence))
    }

    /// Uses current active registry authority to bind one envelope.
    ///
    /// # Errors
    ///
    /// Returns the exact core lifecycle or compatibility refusal without
    /// durable mutation.
    pub fn bind(
        &self,
        envelope: CollectionEnvelope,
    ) -> Result<DeclaredCollectionEnvelope, ManifestStoreError> {
        self.ensure_usable()?;
        self.registry
            .bind(envelope)
            .map_err(ManifestStoreError::Model)
    }

    fn apply_registry<T>(
        &mut self,
        operation: impl FnOnce(&mut SeriesRegistry) -> Result<T, ModelError>,
    ) -> Result<(T, ManifestCommit), ManifestStoreError> {
        self.ensure_usable()?;
        let mut candidate = restore_snapshot(&self.registry.snapshot())?;
        let before = candidate.snapshot();
        let output = operation(&mut candidate).map_err(ManifestStoreError::Model)?;
        let after = candidate.snapshot();
        if after == before {
            return Ok((output, self.commit()));
        }
        let registry_generation = self
            .current
            .registry
            .generation
            .checked_add(1)
            .ok_or(ManifestStoreError::GenerationExhausted)?;
        let manifest_generation = self
            .current
            .generation
            .checked_add(1)
            .ok_or(ManifestStoreError::GenerationExhausted)?;
        let registry_publication = self.prepare_registry_snapshot(registry_generation, &after)?;
        let record = ManifestRecord {
            generation: manifest_generation,
            registry: registry_publication.reference,
            cutoff: self.journal.durable_cutoff(),
            retry: self.current.retry,
            recovery: self.current.recovery,
            sequence_floor: self.current.sequence_floor,
            catalog: self.current.catalog,
        };
        let manifest = self.prepare_manifest(record)?;
        if let Err(error) = self.publish_prepared_registry_snapshot(&after, &registry_publication) {
            return Err(self.record_terminal_error(error));
        }
        let commit = match self.publish_and_adopt_prepared_manifest(&manifest) {
            Ok(commit) => commit,
            Err(error) => return Err(self.record_terminal_error(error)),
        };
        self.registry = candidate;
        Ok((output, commit))
    }

    fn publish_and_adopt_prepared_manifest(
        &mut self,
        prepared: &PreparedManifestPublication,
    ) -> Result<ManifestCommit, ManifestStoreError> {
        let record = prepared.record;
        let slot = match self.publish_prepared_manifest(prepared) {
            Ok(slot) => slot,
            Err(error) => return Err(self.record_terminal_error(error)),
        };
        let mut manifest_slots = self.manifest_slots;
        manifest_slots[slot] = Some(record);
        if let Err(error) =
            remove_unreferenced_catalog_slots(&self.directory_path, &self.directory, manifest_slots)
        {
            return Err(self.record_terminal_error(error));
        }
        if let Err(error) =
            remove_unreferenced_retry_slots(&self.directory_path, &self.directory, manifest_slots)
        {
            return Err(self.record_terminal_error(error));
        }
        if let Err(error) = remove_unreferenced_recovery_slots(
            &self.directory_path,
            &self.directory,
            manifest_slots,
            self.current.cutoff.journal().store_id(),
        ) {
            return Err(self.record_terminal_error(error));
        }
        self.manifest_slots = manifest_slots;
        self.current_slot = slot;
        self.current = record;
        Ok(self.commit())
    }

    fn publish_registry_snapshot(
        &self,
        generation: u64,
        snapshot: &SeriesRegistrySnapshot,
    ) -> Result<RegistryReference, ManifestStoreError> {
        let prepared = self.prepare_registry_snapshot(generation, snapshot)?;
        self.publish_prepared_registry_snapshot(snapshot, &prepared)?;
        Ok(prepared.reference)
    }

    fn prepare_registry_snapshot(
        &self,
        generation: u64,
        snapshot: &SeriesRegistrySnapshot,
    ) -> Result<PreparedRegistryPublication, ManifestStoreError> {
        let referenced = self
            .manifest_slots
            .map(|slot| slot.map(|record| record.registry.slot));
        let slot = (0_u8..3)
            .find(|candidate| !referenced.iter().flatten().any(|slot| slot == candidate))
            .ok_or(ManifestStoreError::InvalidRegistry)?;
        let bytes = encode_registry_snapshot(generation, snapshot)?;
        let reference = RegistryReference {
            slot,
            generation,
            length: u64::try_from(bytes.len()).map_err(|_| ManifestStoreError::InvalidRegistry)?,
            checksum: crc32c(&bytes),
        };
        Ok(PreparedRegistryPublication { reference, bytes })
    }

    fn publish_prepared_registry_snapshot(
        &self,
        snapshot: &SeriesRegistrySnapshot,
        prepared: &PreparedRegistryPublication,
    ) -> Result<(), ManifestStoreError> {
        let slot = prepared.reference.slot;
        publish_reusable_slot(
            &self.directory_path,
            &self.directory,
            REGISTRY_STAGING_FILE_NAME,
            REGISTRY_SLOT_NAMES[usize::from(slot)],
            &prepared.bytes,
            MAX_REGISTRY_SNAPSHOT_BYTES,
            |candidate| {
                let decoded = decode_registry_snapshot_at_slot(candidate, slot)?;
                if decoded.reference != prepared.reference
                    || decoded.registry.snapshot() != *snapshot
                {
                    return Err(ManifestStoreError::InvalidRegistry);
                }
                Ok(())
            },
        )?;
        Ok(())
    }

    fn select_retry_candidate_slot(&self) -> Result<u8, ManifestStoreError> {
        let referenced = self
            .manifest_slots
            .map(|slot| slot.map(|record| record.retry.public.slot()));
        (0_u8..3)
            .find(|candidate| !referenced.iter().flatten().any(|slot| slot == candidate))
            .ok_or(ManifestStoreError::InvalidRetry)
    }

    fn publish_retry_snapshot(
        &self,
        public: RetryStateReference,
        snapshot: &RetryStateSnapshot,
    ) -> Result<RetryArtifactReference, ManifestStoreError> {
        let prepared = self.prepare_retry_snapshot(public, snapshot)?;
        self.publish_prepared_retry_snapshot(snapshot, &prepared)?;
        Ok(prepared.reference)
    }

    fn prepare_retry_snapshot(
        &self,
        public: RetryStateReference,
        snapshot: &RetryStateSnapshot,
    ) -> Result<PreparedRetryPublication, ManifestStoreError> {
        if snapshot.reference() != Some(public) || public.slot() >= 3 {
            return Err(ManifestStoreError::InvalidRetry);
        }
        let selected = self.select_retry_candidate_slot()?;
        if selected != public.slot() {
            return Err(ManifestStoreError::InvalidRetry);
        }
        let bytes = encode_retry_state(snapshot).map_err(map_retry_codec)?;
        let reference = RetryArtifactReference {
            public,
            length: u64::try_from(bytes.len()).map_err(|_| ManifestStoreError::InvalidRetry)?,
            checksum: crc32c(&bytes),
        };
        Ok(PreparedRetryPublication { reference, bytes })
    }

    fn publish_prepared_retry_snapshot(
        &self,
        snapshot: &RetryStateSnapshot,
        prepared: &PreparedRetryPublication,
    ) -> Result<(), ManifestStoreError> {
        let public = prepared.reference.public;
        publish_reusable_slot(
            &self.directory_path,
            &self.directory,
            RETRY_STAGING_FILE_NAME,
            RETRY_SLOT_NAMES[usize::from(public.slot())],
            &prepared.bytes,
            MAX_RETRY_STATE_BYTES,
            |candidate| {
                let (decoded_reference, decoded) = decode_retry_state_at_slot(
                    candidate,
                    public.slot(),
                    snapshot.store_id(),
                    snapshot.options(),
                )
                .map_err(map_retry_codec)?;
                if decoded_reference != prepared.reference || decoded != *snapshot {
                    return Err(ManifestStoreError::InvalidRetry);
                }
                Ok(())
            },
        )?;
        Ok(())
    }

    fn select_catalog_candidate_slot(&self) -> Result<u8, ManifestStoreError> {
        let referenced = self.manifest_slots.map(|slot| {
            slot.and_then(|record| record.catalog.map(GenerationCatalogReference::slot))
        });
        (0_u8..3)
            .find(|candidate| !referenced.iter().flatten().any(|slot| slot == candidate))
            .ok_or(ManifestStoreError::InvalidGeneration)
    }

    fn prepare_catalog_snapshot(
        &self,
        slot: u8,
        generation: u64,
        provisional: &GenerationCatalogSnapshot,
    ) -> Result<PreparedCatalogPublication, ManifestStoreError> {
        if slot >= 3
            || self.select_catalog_candidate_slot()? != slot
            || provisional.reference().is_none_or(|reference| {
                reference.slot() != slot || reference.generation() != generation
            })
        {
            return Err(ManifestStoreError::InvalidGeneration);
        }
        let provisional_bytes = encode_catalog(provisional).map_err(map_generation_codec)?;
        let reference = GenerationCatalogReference::new(
            slot,
            generation,
            u64::try_from(provisional_bytes.len())
                .map_err(|_| ManifestStoreError::InvalidGeneration)?,
            crc32c(&provisional_bytes),
        );
        let snapshot = provisional
            .clone()
            .with_reference(reference)
            .map_err(map_generation_codec)?;
        let bytes = encode_catalog(&snapshot).map_err(map_generation_codec)?;
        if bytes != provisional_bytes {
            return Err(ManifestStoreError::InvalidGeneration);
        }
        Ok(PreparedCatalogPublication { snapshot, bytes })
    }

    fn publish_prepared_catalog_snapshot(
        &self,
        prepared: &PreparedCatalogPublication,
    ) -> Result<(), ManifestStoreError> {
        let reference = prepared
            .snapshot
            .reference()
            .ok_or(ManifestStoreError::InvalidGeneration)?;
        let slot = reference.slot();
        publish_reusable_slot(
            &self.directory_path,
            &self.directory,
            GENERATION_CATALOG_STAGING_FILE_NAME,
            CATALOG_SLOT_NAMES[usize::from(slot)],
            &prepared.bytes,
            MAX_GENERATION_CATALOG_BYTES,
            |candidate| {
                let decoded = decode_catalog(candidate, slot, prepared.snapshot.store_id())
                    .map_err(map_generation_codec)?;
                if decoded != prepared.snapshot {
                    return Err(ManifestStoreError::InvalidGeneration);
                }
                Ok(())
            },
        )?;
        Ok(())
    }

    fn publish_manifest(&self, record: ManifestRecord) -> Result<usize, ManifestStoreError> {
        let prepared = self.prepare_manifest(record)?;
        self.publish_prepared_manifest(&prepared)
    }

    fn prepare_manifest(
        &self,
        record: ManifestRecord,
    ) -> Result<PreparedManifestPublication, ManifestStoreError> {
        let target = if record.generation == 1 {
            self.manifest_slots
                .iter()
                .position(Option::is_none)
                .ok_or(ManifestStoreError::InvalidManifest)?
        } else {
            1 - self.current_slot
        };
        let bytes = encode_manifest(record);
        if bytes.len() != MANIFEST_LEN {
            return Err(ManifestStoreError::InvalidManifest);
        }
        Ok(PreparedManifestPublication {
            record,
            target,
            bytes,
        })
    }

    fn publish_prepared_manifest(
        &self,
        prepared: &PreparedManifestPublication,
    ) -> Result<usize, ManifestStoreError> {
        publish_reusable_slot(
            &self.directory_path,
            &self.directory,
            MANIFEST_STAGING_FILE_NAME,
            MANIFEST_SLOT_NAMES[prepared.target],
            &prepared.bytes,
            MANIFEST_LEN,
            |candidate| {
                if decode_manifest(candidate, self.current.cutoff.journal().store_id())?
                    != prepared.record
                {
                    return Err(ManifestStoreError::InvalidManifest);
                }
                Ok(())
            },
        )?;
        Ok(prepared.target)
    }

    fn commit(&self) -> ManifestCommit {
        self.current.commit()
    }

    fn ensure_usable(&self) -> Result<(), ManifestStoreError> {
        match self.write_state {
            StoreWriteState::Writable => Ok(()),
            StoreWriteState::ReopenRequired => Err(ManifestStoreError::ReopenRequired),
            StoreWriteState::Faulted => Err(ManifestStoreError::Faulted),
        }
    }

    fn record_active_error(&mut self, error: ActiveJournalError) -> ManifestStoreError {
        match self.journal.inspection().write_state() {
            StoreWriteState::Writable => {}
            StoreWriteState::ReopenRequired => {
                self.write_state = StoreWriteState::ReopenRequired;
            }
            StoreWriteState::Faulted => {
                self.write_state = StoreWriteState::Faulted;
            }
        }
        ManifestStoreError::Active(error)
    }

    fn record_terminal_error(&mut self, error: ManifestStoreError) -> ManifestStoreError {
        self.write_state = if matches!(
            error,
            ManifestStoreError::StoragePressure(_)
                | ManifestStoreError::ReopenRequired
                | ManifestStoreError::Active(
                    ActiveJournalError::StoragePressure(_) | ActiveJournalError::ReopenRequired
                )
        ) {
            StoreWriteState::ReopenRequired
        } else {
            StoreWriteState::Faulted
        };
        error
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FormatPreflight {
    EmptyCreate,
    MarkerStaging,
    Current,
}

#[derive(Clone)]
#[allow(clippy::struct_excessive_bools)]
struct Inventory {
    entries: usize,
    store_format: bool,
    store_format_staging: bool,
    registry_staging: bool,
    retry_staging: bool,
    recovery_staging: bool,
    rotation_staging: bool,
    registry_slots: usize,
    retry_slots: usize,
    manifest_slots: usize,
    catalog_slots: usize,
    recovery_slots: usize,
    rotation_intent: bool,
    store_lock: bool,
    active_journals: Vec<u64>,
    active_checkpoints: Vec<u64>,
    sealed_generations: Vec<u64>,
}

fn preflight_store_format(
    config: &ManifestStoreConfig,
) -> Result<FormatPreflight, ManifestStoreError> {
    let inventory = match inspect_inventory(&config.directory) {
        Ok(inventory) => inventory,
        Err(ManifestStoreError::Io(error)) => return Err(ManifestStoreError::Io(error)),
        Err(_) => return Err(ManifestStoreError::UnsupportedStoreFormat),
    };
    if inventory.entries == 0 {
        return if config.mode == ActiveJournalOpenMode::CreateNew {
            Ok(FormatPreflight::EmptyCreate)
        } else {
            Err(ManifestStoreError::UnsupportedStoreFormat)
        };
    }
    if !inventory.store_lock
        || inventory.store_format == inventory.store_format_staging
        || (!inventory.store_format && inventory.entries != 2)
    {
        return Err(ManifestStoreError::UnsupportedStoreFormat);
    }
    let marker_name = if inventory.store_format {
        STORE_FORMAT_FILE_NAME
    } else {
        STORE_FORMAT_STAGING_FILE_NAME
    };
    let marker = read_required_bounded(&config.directory.join(marker_name), STORE_FORMAT_LEN)
        .map_err(|_| ManifestStoreError::UnsupportedStoreFormat)?;
    decode_store_format_marker(&marker, config.store_id)?;
    if inventory.store_format_staging {
        return Ok(FormatPreflight::MarkerStaging);
    }
    preflight_current_artifact_versions(config, &inventory)?;
    if config.mode == ActiveJournalOpenMode::CreateNew && inventory.manifest_slots != 0 {
        return Err(ManifestStoreError::InvalidInventory);
    }
    Ok(FormatPreflight::Current)
}

fn preflight_genesis_records(config: &ManifestStoreConfig) -> Result<(), ManifestStoreError> {
    let registry = SeriesRegistry::new(config.store_id, config.registry.limits).snapshot();
    let registry_bytes = encode_registry_snapshot(1, &registry)?;
    let registry = RegistryReference {
        slot: 0,
        generation: 1,
        length: u64::try_from(registry_bytes.len())
            .map_err(|_| ManifestStoreError::InvalidRegistry)?,
        checksum: crc32c(&registry_bytes),
    };
    let retry_public = RetryStateReference::new(0, 1);
    let retry_snapshot =
        RetryStateSnapshot::empty_persisted(config.store_id, config.retry, retry_public);
    let retry_bytes = encode_retry_state(&retry_snapshot).map_err(map_retry_codec)?;
    let retry = RetryArtifactReference {
        public: retry_public,
        length: u64::try_from(retry_bytes.len()).map_err(|_| ManifestStoreError::InvalidRetry)?,
        checksum: crc32c(&retry_bytes),
    };
    let manifest = ManifestRecord {
        generation: 1,
        registry,
        cutoff: genesis_placeholder(config.store_id),
        retry,
        recovery: None,
        sequence_floor: 0,
        catalog: None,
    };
    if encode_manifest(manifest).len() != MANIFEST_LEN {
        return Err(ManifestStoreError::InvalidManifest);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn preflight_current_artifact_versions(
    config: &ManifestStoreConfig,
    inventory: &Inventory,
) -> Result<(), ManifestStoreError> {
    let mut manifests = [None, None];
    for (slot, name) in MANIFEST_SLOT_NAMES.iter().enumerate() {
        let Some(bytes) = read_optional_bounded(&config.directory.join(name), MANIFEST_LEN)? else {
            continue;
        };
        if bytes.len() != MANIFEST_LEN
            || bytes[..8] != MANIFEST_MAGIC
            || u16::from_be_bytes(bytes[8..10].try_into().unwrap_or_default()) != MANIFEST_VERSION
            || u16::from_be_bytes(bytes[10..12].try_into().unwrap_or_default())
                != u16::try_from(MANIFEST_LEN).unwrap_or_default()
        {
            return Err(ManifestStoreError::UnsupportedStoreFormat);
        }
        manifests[slot] = Some(decode_manifest(&bytes, config.store_id)?);
    }
    let _ = select_current_manifest(manifests)?;

    for generation in &inventory.active_journals {
        let path = config.directory.join(active_journal_file_name(*generation));
        let mut file =
            File::open(path).map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
        if file
            .metadata()
            .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
            .len()
            < crate::JOURNAL_V1_HEADER_LEN as u64
        {
            return Err(ManifestStoreError::UnsupportedStoreFormat);
        }
        let mut header = [0_u8; crate::JOURNAL_V1_HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
        if header[..8] != crate::JOURNAL_V1_HEADER_MAGIC
            || u16::from_be_bytes(header[8..10].try_into().unwrap_or_default())
                != crate::JOURNAL_V1_VERSION
            || !matches!(
                crate::JournalHeaderV1::decode(&header),
                Ok(decoded) if decoded.store_id() == config.store_id
            )
        {
            return Err(ManifestStoreError::UnsupportedStoreFormat);
        }
    }

    for (slot, name) in RETRY_SLOT_NAMES.iter().enumerate() {
        let Some(bytes) =
            read_optional_bounded(&config.directory.join(name), MAX_RETRY_STATE_BYTES)?
        else {
            continue;
        };
        if bytes.len() < crate::retry::RETRY_HEADER_LEN + 4
            || bytes[..8] != crate::retry::RETRY_MAGIC
            || u16::from_be_bytes(bytes[8..10].try_into().unwrap_or_default())
                != crate::retry::RETRY_VERSION
        {
            return Err(ManifestStoreError::UnsupportedStoreFormat);
        }
        match decode_retry_state_at_slot(
            &bytes,
            u8::try_from(slot).map_err(|_| ManifestStoreError::UnsupportedStoreFormat)?,
            config.store_id,
            config.retry,
        ) {
            Ok(_) => {}
            Err(RetryStateCodecError::OptionsMismatch) => {
                return Err(ManifestStoreError::InvalidRetry);
            }
            Err(RetryStateCodecError::StoreMismatch) => {
                return Err(ManifestStoreError::StoreMismatch);
            }
            Err(RetryStateCodecError::Invalid) => {
                return Err(ManifestStoreError::UnsupportedStoreFormat);
            }
        }
    }
    for name in RECOVERY_SLOT_NAMES {
        let Some(bytes) = read_optional_bounded(&config.directory.join(name), RECOVERY_STATE_LEN)?
        else {
            continue;
        };
        decode_recovery_state(&bytes, config.store_id).map_err(map_recovery_codec)?;
    }
    match read_optional_bounded(
        &config.directory.join(RECOVERY_STAGING_FILE_NAME),
        RECOVERY_STATE_LEN,
    ) {
        Ok(Some(bytes)) => {
            let _ = decode_recovery_state(&bytes, config.store_id)
                .map_err(|_| ManifestStoreError::InterruptedPublication)?;
        }
        Ok(None) => {}
        Err(ManifestStoreError::Io(error)) => return Err(ManifestStoreError::Io(error)),
        Err(_) => return Err(ManifestStoreError::InterruptedPublication),
    }
    match read_optional_bounded(
        &config.directory.join(MANIFEST_STAGING_FILE_NAME),
        MANIFEST_LEN,
    ) {
        Ok(Some(bytes)) => {
            if bytes.len() != MANIFEST_LEN
                || bytes[..8] != MANIFEST_MAGIC
                || u16::from_be_bytes(bytes[8..10].try_into().unwrap_or_default())
                    != MANIFEST_VERSION
                || u16::from_be_bytes(bytes[10..12].try_into().unwrap_or_default())
                    != u16::try_from(MANIFEST_LEN).unwrap_or_default()
            {
                return Err(ManifestStoreError::InterruptedPublication);
            }
        }
        Ok(None) => {}
        Err(ManifestStoreError::Io(error)) => return Err(ManifestStoreError::Io(error)),
        Err(_) => return Err(ManifestStoreError::InterruptedPublication),
    }
    Ok(())
}

fn encode_store_format_marker(store_id: StoreId) -> [u8; STORE_FORMAT_LEN] {
    let mut bytes = [0_u8; STORE_FORMAT_LEN];
    bytes[..8].copy_from_slice(&STORE_FORMAT_MAGIC);
    bytes[8..10].copy_from_slice(&STORE_FORMAT_VERSION.to_be_bytes());
    bytes[10..12].copy_from_slice(&STORE_FORMAT_RECORD_LEN.to_be_bytes());
    bytes[12..28].copy_from_slice(store_id.as_bytes());
    let checksum = crc32c(&bytes[..28]);
    bytes[28..32].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

fn decode_store_format_marker(
    bytes: &[u8],
    expected_store: StoreId,
) -> Result<(), ManifestStoreError> {
    if bytes.len() != STORE_FORMAT_LEN
        || bytes[..8] != STORE_FORMAT_MAGIC
        || u16::from_be_bytes(bytes[8..10].try_into().unwrap_or_default()) != STORE_FORMAT_VERSION
        || u16::from_be_bytes(bytes[10..12].try_into().unwrap_or_default())
            != STORE_FORMAT_RECORD_LEN
        || crc32c(&bytes[..28]) != u32::from_be_bytes(bytes[28..32].try_into().unwrap_or_default())
        || !matches!(
            StoreId::from_bytes(bytes[12..28].try_into().unwrap_or_default()),
            Ok(store_id) if store_id == expected_store
        )
    {
        return Err(ManifestStoreError::UnsupportedStoreFormat);
    }
    Ok(())
}

fn publish_store_format_marker(
    directory_path: &Path,
    directory: &File,
    store_id: StoreId,
) -> Result<(), ManifestStoreError> {
    let bytes = encode_store_format_marker(store_id);
    publish_reusable_slot(
        directory_path,
        directory,
        STORE_FORMAT_STAGING_FILE_NAME,
        STORE_FORMAT_FILE_NAME,
        &bytes,
        STORE_FORMAT_LEN,
        |candidate| decode_store_format_marker(candidate, store_id),
    )
}

fn converge_store_format_marker(
    directory_path: &Path,
    directory: &File,
    store_id: StoreId,
) -> Result<(), ManifestStoreError> {
    let staging = directory_path.join(STORE_FORMAT_STAGING_FILE_NAME);
    let final_path = directory_path.join(STORE_FORMAT_FILE_NAME);
    if final_path
        .try_exists()
        .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
    {
        return Err(ManifestStoreError::UnsupportedStoreFormat);
    }
    let bytes = read_required_bounded(&staging, STORE_FORMAT_LEN)
        .map_err(|_| ManifestStoreError::UnsupportedStoreFormat)?;
    decode_store_format_marker(&bytes, store_id)?;
    std::fs::rename(staging, final_path)
        .map_err(|error| manifest_io(ManifestIoOperation::Publish, &error))?;
    directory
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))
}

#[allow(clippy::too_many_lines)]
fn inspect_inventory(directory: &Path) -> Result<Inventory, ManifestStoreError> {
    let mut count = 0_usize;
    let mut registry_staging = false;
    let mut retry_staging = false;
    let mut recovery_staging = false;
    let mut store_format = false;
    let mut store_format_staging = false;
    let mut rotation_staging = false;
    let mut registry_slots = 0_usize;
    let mut retry_slots = 0_usize;
    let mut manifest_slots = 0_usize;
    let mut catalog_slots = 0_usize;
    let mut recovery_slots = 0_usize;
    let mut active_journals = Vec::new();
    let mut active_checkpoints = Vec::new();
    let mut sealed_generations = Vec::new();
    let mut rotation_intent = false;
    let mut store_lock = false;
    for entry in directory
        .read_dir()
        .map_err(|error| manifest_io(ManifestIoOperation::ReadInventory, &error))?
    {
        count = count
            .checked_add(1)
            .ok_or(ManifestStoreError::InvalidInventory)?;
        if count > MAX_INVENTORY_ENTRIES {
            return Err(ManifestStoreError::InvalidInventory);
        }
        let entry =
            entry.map_err(|error| manifest_io(ManifestIoOperation::ReadInventory, &error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| manifest_io(ManifestIoOperation::ReadInventory, &error))?;
        if !file_type.is_file() {
            return Err(ManifestStoreError::InvalidInventory);
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(ManifestStoreError::InvalidInventory);
        };
        if name == STORE_FORMAT_FILE_NAME {
            store_format = true;
        } else if name == STORE_FORMAT_STAGING_FILE_NAME {
            store_format_staging = true;
        } else if name == STORE_LOCK_FILE_NAME {
            store_lock = true;
        } else if name == MANIFEST_STAGING_FILE_NAME {
            // Exact staging content is validated separately under the same
            // bounded fixed-name inventory.
        } else if name == REGISTRY_STAGING_FILE_NAME {
            registry_staging = true;
        } else if name == RETRY_STAGING_FILE_NAME {
            retry_staging = true;
        } else if name == RECOVERY_STAGING_FILE_NAME {
            recovery_staging = true;
        } else if name == GENERATION_CATALOG_STAGING_FILE_NAME
            || name == SEALED_JOURNAL_STAGING_FILE_NAME
        {
            rotation_staging = true;
        } else if REGISTRY_SLOT_NAMES.contains(&name) {
            registry_slots += 1;
        } else if RETRY_SLOT_NAMES.contains(&name) {
            retry_slots += 1;
        } else if CATALOG_SLOT_NAMES.contains(&name) {
            catalog_slots += 1;
        } else if RECOVERY_SLOT_NAMES.contains(&name) {
            recovery_slots += 1;
        } else if name == ROTATION_INTENT_FILE_NAME {
            rotation_intent = true;
        } else if let Some(generation) = parse_sealed_generation_name(name) {
            sealed_generations.push(generation);
            if sealed_generations.len() > MAX_SEALED_GENERATIONS {
                return Err(ManifestStoreError::InvalidInventory);
            }
        } else if let Some(generation) = parse_active_journal_generation_name(name) {
            active_journals.push(generation);
        } else if let Some(generation) = parse_active_checkpoint_generation_name(name) {
            active_checkpoints.push(generation);
        } else if name == ACTIVE_JOURNAL_FILE_NAME {
            active_journals.push(ACTIVE_JOURNAL_GENERATION);
        } else if name == ACTIVE_CHECKPOINT_FILE_NAME {
            active_checkpoints.push(ACTIVE_JOURNAL_GENERATION);
        } else if MANIFEST_SLOT_NAMES.contains(&name) {
            manifest_slots += 1;
        } else {
            return Err(ManifestStoreError::InvalidInventory);
        }
    }
    active_journals.sort_unstable();
    active_checkpoints.sort_unstable();
    sealed_generations.sort_unstable();
    Ok(Inventory {
        entries: count,
        store_format,
        store_format_staging,
        registry_staging,
        retry_staging,
        recovery_staging,
        rotation_staging,
        registry_slots,
        retry_slots,
        manifest_slots,
        catalog_slots,
        recovery_slots,
        rotation_intent,
        store_lock,
        active_journals,
        active_checkpoints,
        sealed_generations,
    })
}

fn validate_genesis_inventory(inventory: &Inventory) -> Result<(), ManifestStoreError> {
    let active_is_valid = (inventory.active_journals.is_empty()
        && inventory.active_checkpoints.is_empty())
        || (inventory.active_journals == [ACTIVE_JOURNAL_GENERATION]
            && (inventory.active_checkpoints.is_empty()
                || inventory.active_checkpoints == [ACTIVE_JOURNAL_GENERATION]));
    if !active_is_valid
        || !inventory.sealed_generations.is_empty()
        || inventory.catalog_slots != 0
        || inventory.recovery_slots != 0
        || inventory.recovery_staging
        || inventory.rotation_intent
        || inventory.rotation_staging
    {
        return Err(ManifestStoreError::UnsupportedStoreFormat);
    }
    Ok(())
}

fn validate_generation_inventory(
    inventory: &Inventory,
    current: ManifestRecord,
    catalog: &GenerationCatalogSnapshot,
    intent: Option<RotationIntent>,
) -> Result<(), ManifestStoreError> {
    let active_generation = current.cutoff.journal().generation();
    let mut allowed_active = vec![active_generation];
    if let Some(intent) = intent {
        if active_generation == intent.source_generation {
            let successor_present = inventory
                .active_journals
                .contains(&intent.successor_generation)
                || inventory
                    .active_checkpoints
                    .contains(&intent.successor_generation);
            if successor_present {
                allowed_active.push(intent.successor_generation);
            }
        } else if active_generation == intent.successor_generation {
            if inventory
                .active_journals
                .contains(&intent.source_generation)
            {
                allowed_active.push(intent.source_generation);
            }
        } else {
            return Err(ManifestStoreError::InvalidGeneration);
        }
    }
    allowed_active.sort_unstable();
    if inventory.active_journals != allowed_active {
        return Err(ManifestStoreError::InvalidInventory);
    }
    if let Some(intent) = intent {
        if active_generation == intent.successor_generation {
            let mut allowed_checkpoints = vec![active_generation];
            if inventory
                .active_checkpoints
                .contains(&intent.source_generation)
            {
                allowed_checkpoints.push(intent.source_generation);
                allowed_checkpoints.sort_unstable();
            }
            if inventory.active_checkpoints != allowed_checkpoints {
                return Err(ManifestStoreError::InvalidInventory);
            }
        } else if inventory.active_checkpoints != allowed_active {
            return Err(ManifestStoreError::InvalidInventory);
        }
    } else if inventory.active_checkpoints != allowed_active {
        return Err(ManifestStoreError::InvalidInventory);
    }

    let mut allowed_sealed = catalog
        .entries()
        .iter()
        .map(|entry| entry.journal_generation())
        .collect::<Vec<_>>();
    if let Some(intent) = intent
        && active_generation == intent.source_generation
        && inventory
            .sealed_generations
            .contains(&intent.source_generation)
    {
        allowed_sealed.push(intent.source_generation);
    }
    if inventory.sealed_generations != allowed_sealed {
        return Err(ManifestStoreError::InvalidInventory);
    }
    Ok(())
}

fn open_directory(path: &Path) -> Result<File, ManifestStoreError> {
    let metadata = path
        .metadata()
        .map_err(|error| manifest_io(ManifestIoOperation::OpenDirectory, &error))?;
    if !metadata.is_dir() {
        return Err(ManifestStoreError::InvalidInventory);
    }
    File::open(path).map_err(|error| manifest_io(ManifestIoOperation::OpenDirectory, &error))
}

fn lock_store(file: &File) -> Result<(), ManifestStoreError> {
    file.try_lock().map_err(|error| match error {
        std::fs::TryLockError::WouldBlock => ManifestStoreError::AlreadyOpen,
        std::fs::TryLockError::Error(error) => manifest_io(ManifestIoOperation::LockStore, &error),
    })
}

fn read_manifest_slots(
    directory: &Path,
    store_id: StoreId,
) -> Result<[Option<ManifestRecord>; 2], ManifestStoreError> {
    let mut slots = [None, None];
    for (index, name) in MANIFEST_SLOT_NAMES.iter().enumerate() {
        let Some(bytes) = read_optional_bounded(&directory.join(name), MANIFEST_LEN)? else {
            continue;
        };
        slots[index] = Some(decode_manifest(&bytes, store_id)?);
    }
    Ok(slots)
}

#[allow(clippy::large_types_passed_by_value)]
fn select_current_manifest(
    slots: [Option<ManifestRecord>; 2],
) -> Result<Option<(usize, ManifestRecord)>, ManifestStoreError> {
    match (slots[0], slots[1]) {
        (None, None) => Ok(None),
        (Some(record), None) => {
            if record.generation != 1 {
                return Err(ManifestStoreError::InvalidManifest);
            }
            Ok(Some((0, record)))
        }
        (None, Some(record)) => {
            if record.generation != 1 {
                return Err(ManifestStoreError::InvalidManifest);
            }
            Ok(Some((1, record)))
        }
        (Some(first), Some(second)) => {
            let (older_index, older, newer_index, newer) = if first.generation < second.generation {
                (0, first, 1, second)
            } else {
                (1, second, 0, first)
            };
            let _ = older_index;
            if older.generation.checked_add(1) != Some(newer.generation)
                || !retry_reference_progresses(older.retry, newer.retry)
                || !catalog_reference_progresses(older.catalog, newer.catalog)
                || !recovery_reference_progresses(older.recovery, newer.recovery)
            {
                return Err(ManifestStoreError::InvalidManifest);
            }
            Ok(Some((newer_index, newer)))
        }
    }
}

fn recovery_reference_progresses(
    older: Option<RecoveryReference>,
    newer: Option<RecoveryReference>,
) -> bool {
    match (older, newer) {
        (None, None | Some(_)) => true,
        (Some(_), None) => false,
        (Some(older), Some(newer)) if older == newer => true,
        (Some(older), Some(newer)) => older.slot != newer.slot,
    }
}

fn catalog_reference_progresses(
    older: Option<GenerationCatalogReference>,
    newer: Option<GenerationCatalogReference>,
) -> bool {
    match (older, newer) {
        (None, None) => true,
        (None, Some(newer)) => newer.generation() == 1,
        (Some(_), None) => false,
        (Some(older), Some(newer)) if older.generation() == newer.generation() => older == newer,
        (Some(older), Some(newer)) => {
            older.generation().checked_add(1) == Some(newer.generation())
                && older.slot() != newer.slot()
        }
    }
}

fn retry_reference_progresses(
    older: RetryArtifactReference,
    newer: RetryArtifactReference,
) -> bool {
    if newer.public.generation() == older.public.generation() {
        newer == older
    } else {
        older.public.generation().checked_add(1) == Some(newer.public.generation())
            && newer.public.slot() != older.public.slot()
    }
}

#[derive(Clone)]
struct RecoveryInventoryState {
    slots: [Option<RecoveryArtifact>; 3],
    staging: Option<RecoveryReport>,
    manifest_staging: Option<ManifestRecord>,
}

fn read_recovery_inventory(
    directory: &Path,
    store_id: StoreId,
) -> Result<RecoveryInventoryState, ManifestStoreError> {
    let mut slots = [None, None, None];
    for (slot, name) in RECOVERY_SLOT_NAMES.iter().enumerate() {
        let Some(bytes) = read_optional_bounded(&directory.join(name), RECOVERY_STATE_LEN)? else {
            continue;
        };
        let report = decode_recovery_state(&bytes, store_id).map_err(map_recovery_codec)?;
        slots[slot] = Some(RecoveryArtifact {
            reference: RecoveryReference {
                slot: u8::try_from(slot).map_err(|_| ManifestStoreError::InvalidManifest)?,
                checksum: crc32c(&bytes),
            },
            report,
        });
    }
    let staging = read_optional_bounded(
        &directory.join(RECOVERY_STAGING_FILE_NAME),
        RECOVERY_STATE_LEN,
    )?
    .map(|bytes| decode_recovery_state(&bytes, store_id).map_err(map_recovery_codec))
    .transpose()?;
    let manifest_staging =
        read_optional_bounded(&directory.join(MANIFEST_STAGING_FILE_NAME), MANIFEST_LEN)?
            .map(|bytes| decode_manifest(&bytes, store_id))
            .transpose()?;
    Ok(RecoveryInventoryState {
        slots,
        staging,
        manifest_staging,
    })
}

fn referenced_recovery(
    inventory: &RecoveryInventoryState,
    reference: RecoveryReference,
) -> Result<RecoveryReport, ManifestStoreError> {
    inventory
        .slots
        .get(usize::from(reference.slot))
        .and_then(|artifact| *artifact)
        .filter(|artifact| artifact.reference == reference)
        .map(|artifact| artifact.report)
        .ok_or(ManifestStoreError::InvalidManifest)
}

#[allow(clippy::large_types_passed_by_value)]
fn validate_recovery_manifest_progression(
    manifests: [Option<ManifestRecord>; 2],
    inventory: &RecoveryInventoryState,
) -> Result<(), ManifestStoreError> {
    for manifest in manifests.into_iter().flatten() {
        if let Some(reference) = manifest.recovery {
            let report = referenced_recovery(inventory, reference)?;
            if report.committing_manifest_generation() > manifest.generation {
                return Err(ManifestStoreError::InvalidManifest);
            }
        }
    }
    let mut retained = manifests.into_iter().flatten().collect::<Vec<_>>();
    retained.sort_by_key(|manifest| manifest.generation);
    let [older, newer] = retained.as_slice() else {
        return Ok(());
    };
    if older.recovery == newer.recovery {
        return Ok(());
    }
    let newer_reference = newer.recovery.ok_or(ManifestStoreError::InvalidManifest)?;
    let newer_report = referenced_recovery(inventory, newer_reference)?;
    let expected_generation = match older.recovery {
        Some(reference) => referenced_recovery(inventory, reference)?
            .report_generation()
            .checked_add(1),
        None => Some(1),
    };
    if expected_generation != Some(newer_report.report_generation())
        || !manifest_authority_equal_for_recovery(*older, *newer)
        || !report_matches_source(newer_report, *older, newer.generation)
    {
        return Err(ManifestStoreError::InvalidManifest);
    }
    Ok(())
}

#[allow(clippy::large_types_passed_by_value)]
fn validate_no_pending_recovery(
    manifests: [Option<ManifestRecord>; 2],
    inventory: &RecoveryInventoryState,
) -> Result<(), ManifestStoreError> {
    if inventory.staging.is_some() {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    let referenced = manifests
        .iter()
        .flatten()
        .filter_map(|manifest| manifest.recovery)
        .collect::<Vec<_>>();
    let latest = referenced
        .iter()
        .map(|reference| referenced_recovery(inventory, *reference))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(RecoveryReport::report_generation)
        .max();
    for artifact in inventory.slots.into_iter().flatten() {
        if referenced.contains(&artifact.reference) {
            continue;
        }
        if latest.is_none_or(|generation| artifact.report.report_generation() >= generation) {
            return Err(ManifestStoreError::InvalidGeneration);
        }
    }
    Ok(())
}

fn manifest_authority_equal_for_recovery(older: ManifestRecord, newer: ManifestRecord) -> bool {
    older.generation.checked_add(1) == Some(newer.generation)
        && older.registry == newer.registry
        && older.cutoff == newer.cutoff
        && older.retry == newer.retry
        && older.sequence_floor == newer.sequence_floor
        && older.catalog == newer.catalog
}

fn report_matches_source(
    report: RecoveryReport,
    source: ManifestRecord,
    committing_generation: u64,
) -> bool {
    report.source_manifest_generation() == source.generation
        && report.committing_manifest_generation() == committing_generation
        && report.source_manifest_checksum() == manifest_checksum(source)
        && report.cutoff() == source.cutoff
        && report.active_sequence_floor() == source.sequence_floor
}

fn manifest_checksum(record: ManifestRecord) -> u32 {
    let bytes = encode_manifest(record);
    u32::from_be_bytes(bytes[156..160].try_into().unwrap_or_default())
}

fn read_referenced_registry(
    directory: &Path,
    reference: RegistryReference,
    store_id: StoreId,
) -> Result<SeriesRegistry, ManifestStoreError> {
    let index = usize::from(reference.slot);
    let name = REGISTRY_SLOT_NAMES
        .get(index)
        .ok_or(ManifestStoreError::InvalidRegistry)?;
    let bytes = read_required_bounded(&directory.join(name), MAX_REGISTRY_SNAPSHOT_BYTES)?;
    if u64::try_from(bytes.len()).ok() != Some(reference.length)
        || crc32c(&bytes) != reference.checksum
    {
        return Err(ManifestStoreError::InvalidRegistry);
    }
    let decoded = decode_registry_snapshot_at_slot(&bytes, reference.slot)?;
    if decoded.registry.store_id() != store_id {
        return Err(ManifestStoreError::StoreMismatch);
    }
    if decoded.reference != reference {
        return Err(ManifestStoreError::InvalidRegistry);
    }
    Ok(decoded.registry)
}

#[allow(clippy::large_types_passed_by_value)]
fn validate_registry_inventory(
    directory: &Path,
    manifests: [Option<ManifestRecord>; 2],
    current: RegistryReference,
    store_id: StoreId,
) -> Result<(), ManifestStoreError> {
    for (slot, name) in REGISTRY_SLOT_NAMES.iter().enumerate() {
        let Some(bytes) =
            read_optional_bounded(&directory.join(name), MAX_REGISTRY_SNAPSHOT_BYTES)?
        else {
            continue;
        };
        let decoded = decode_registry_snapshot_at_slot(
            &bytes,
            u8::try_from(slot).map_err(|_| ManifestStoreError::InvalidRegistry)?,
        )?;
        if decoded.registry.store_id() != store_id {
            return Err(ManifestStoreError::StoreMismatch);
        }
        if decoded.reference.slot != u8::try_from(slot).unwrap_or(u8::MAX) {
            return Err(ManifestStoreError::InvalidRegistry);
        }
        let referenced = manifests
            .iter()
            .flatten()
            .any(|manifest| manifest.registry == decoded.reference);
        let reachable = decoded.reference.generation <= current.generation
            || current.generation.checked_add(1) == Some(decoded.reference.generation);
        if !referenced && !reachable {
            return Err(ManifestStoreError::InvalidRegistry);
        }
    }
    for manifest in manifests.into_iter().flatten() {
        let _ = read_referenced_registry(directory, manifest.registry, store_id)?;
    }
    Ok(())
}

fn read_interrupted_genesis_registry(
    directory: &Path,
    expected: &SeriesRegistrySnapshot,
) -> Result<RegistryReference, ManifestStoreError> {
    let mut found = None;
    for (slot, name) in REGISTRY_SLOT_NAMES.iter().enumerate() {
        let Some(bytes) =
            read_optional_bounded(&directory.join(name), MAX_REGISTRY_SNAPSHOT_BYTES)?
        else {
            continue;
        };
        if found.is_some() {
            return Err(ManifestStoreError::InvalidRegistry);
        }
        let decoded = decode_registry_snapshot_at_slot(
            &bytes,
            u8::try_from(slot).map_err(|_| ManifestStoreError::InvalidRegistry)?,
        )?;
        if decoded.reference.slot != u8::try_from(slot).unwrap_or(u8::MAX)
            || decoded.reference.generation != 1
            || decoded.registry.snapshot() != *expected
        {
            return Err(ManifestStoreError::InvalidRegistry);
        }
        found = Some(decoded.reference);
    }
    found.ok_or(ManifestStoreError::InvalidRegistry)
}

fn read_referenced_catalog(
    directory: &Path,
    reference: GenerationCatalogReference,
    store_id: StoreId,
) -> Result<GenerationCatalogSnapshot, ManifestStoreError> {
    let name = CATALOG_SLOT_NAMES
        .get(usize::from(reference.slot()))
        .ok_or(ManifestStoreError::InvalidGeneration)?;
    let bytes = read_required_bounded(&directory.join(name), MAX_GENERATION_CATALOG_BYTES)?;
    if u64::try_from(bytes.len()).ok() != Some(reference.length())
        || crc32c(&bytes) != reference.checksum()
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    let snapshot =
        decode_catalog(&bytes, reference.slot(), store_id).map_err(map_generation_codec)?;
    if snapshot.reference() != Some(reference) {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    validate_sealed_inventory_metadata(directory, &snapshot)?;
    Ok(snapshot)
}

fn validate_manifest_catalog_binding(
    manifest: ManifestRecord,
    catalog: &GenerationCatalogSnapshot,
) -> Result<(), ManifestStoreError> {
    let reference = catalog
        .reference()
        .ok_or(ManifestStoreError::InvalidGeneration)?;
    let last = catalog
        .entries()
        .last()
        .ok_or(ManifestStoreError::InvalidGeneration)?;
    if manifest.catalog != Some(reference)
        || last.journal_generation().checked_add(1) != Some(manifest.cutoff.journal().generation())
        || manifest.sequence_floor != last.sequence_cutoff()
        || manifest.registry.generation < last.registry_generation()
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    Ok(())
}

fn catalog_appends_exactly_one(
    older: &GenerationCatalogSnapshot,
    newer: &GenerationCatalogSnapshot,
) -> bool {
    let Some(older_reference) = older.reference() else {
        return false;
    };
    let Some(newer_reference) = newer.reference() else {
        return false;
    };
    older_reference.generation().checked_add(1) == Some(newer_reference.generation())
        && older.entries().len().checked_add(1) == Some(newer.entries().len())
        && newer.entries().starts_with(older.entries())
}

fn validate_catalog_advancing_transition(
    older_manifest: ManifestRecord,
    newer_manifest: ManifestRecord,
    older_catalog: &GenerationCatalogSnapshot,
    newer_catalog: &GenerationCatalogSnapshot,
) -> Result<(), ManifestStoreError> {
    let appended = newer_catalog
        .entries()
        .last()
        .ok_or(ManifestStoreError::InvalidGeneration)?;
    let catalog_progresses = match older_manifest.catalog {
        None => {
            older_catalog.reference().is_none()
                && older_catalog.entries().is_empty()
                && newer_catalog.entries().len() == 1
                && newer_catalog
                    .reference()
                    .is_some_and(|reference| reference.generation() == 1)
        }
        Some(reference) => {
            older_catalog.reference() == Some(reference)
                && catalog_appends_exactly_one(older_catalog, newer_catalog)
        }
    };
    if !catalog_progresses
        || newer_manifest.catalog != newer_catalog.reference()
        || !catalog_reference_progresses(older_manifest.catalog, newer_manifest.catalog)
        || older_manifest.generation.checked_add(1) != Some(newer_manifest.generation)
        || appended.journal_generation() != older_manifest.cutoff.journal().generation()
        || appended.sequence_floor() != older_manifest.sequence_floor
        || appended.sequence_cutoff() != older_manifest.cutoff.append_sequence()
        || appended.end_offset() != older_manifest.cutoff.end_offset()
        || appended.artifact_length() != older_manifest.cutoff.end_offset()
        || appended.registry_generation() != older_manifest.registry.generation
        || older_manifest.cutoff.journal().generation().checked_add(1)
            != Some(newer_manifest.cutoff.journal().generation())
        || newer_manifest.sequence_floor != older_manifest.cutoff.append_sequence()
        || newer_manifest.cutoff.append_sequence() != older_manifest.cutoff.append_sequence()
        || newer_manifest.cutoff.end_offset() != crate::JOURNAL_V1_HEADER_LEN as u64
        || newer_manifest.cutoff.checkpoint_generation() != 1
        || newer_manifest.registry != older_manifest.registry
        || newer_manifest.retry != older_manifest.retry
        || newer_manifest.recovery != older_manifest.recovery
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    Ok(())
}

fn catalog_is_strict_prefix(
    older: &GenerationCatalogSnapshot,
    newer: &GenerationCatalogSnapshot,
) -> bool {
    older.entries().len() < newer.entries().len()
        && newer.entries().starts_with(older.entries())
        && older.reference().is_some_and(|older_reference| {
            newer.reference().is_some_and(|newer_reference| {
                older_reference.generation() < newer_reference.generation()
            })
        })
}

#[allow(clippy::large_types_passed_by_value)]
fn validate_catalog_inventory(
    directory: &Path,
    manifests: [Option<ManifestRecord>; 2],
    store_id: StoreId,
) -> Result<(), ManifestStoreError> {
    let mut referenced = Vec::new();
    for manifest in manifests.into_iter().flatten() {
        if let Some(reference) = manifest.catalog {
            let snapshot = read_referenced_catalog(directory, reference, store_id)?;
            validate_manifest_catalog_binding(manifest, &snapshot)?;
            referenced.push((manifest, snapshot));
        }
    }
    let mut retained = manifests.into_iter().flatten().collect::<Vec<_>>();
    retained.sort_by_key(|manifest| manifest.generation);
    if let [older, newer] = retained.as_slice()
        && older.catalog != newer.catalog
    {
        let older_catalog = match older.catalog {
            Some(reference) => read_referenced_catalog(directory, reference, store_id)?,
            None => GenerationCatalogSnapshot::empty(store_id),
        };
        let newer_catalog = read_referenced_catalog(
            directory,
            newer.catalog.ok_or(ManifestStoreError::InvalidGeneration)?,
            store_id,
        )?;
        validate_catalog_advancing_transition(*older, *newer, &older_catalog, &newer_catalog)?;
    }

    for (slot, name) in CATALOG_SLOT_NAMES.iter().enumerate() {
        let Some(bytes) =
            read_optional_bounded(&directory.join(name), MAX_GENERATION_CATALOG_BYTES)?
        else {
            continue;
        };
        let slot = u8::try_from(slot).map_err(|_| ManifestStoreError::InvalidGeneration)?;
        let snapshot = decode_catalog(&bytes, slot, store_id).map_err(map_generation_codec)?;
        let reference = snapshot
            .reference()
            .ok_or(ManifestStoreError::InvalidGeneration)?;
        let is_referenced = manifests
            .iter()
            .flatten()
            .any(|manifest| manifest.catalog == Some(reference));
        if !is_referenced
            && !referenced
                .iter()
                .any(|(_, current)| catalog_is_strict_prefix(&snapshot, current))
        {
            return Err(ManifestStoreError::InvalidGeneration);
        }
        validate_sealed_inventory_metadata(directory, &snapshot)?;
    }
    Ok(())
}

#[allow(clippy::large_types_passed_by_value)]
fn validate_committed_rotation_transition(
    directory: &Path,
    manifests: [Option<ManifestRecord>; 2],
    current: ManifestRecord,
    intent: RotationIntent,
    store_id: StoreId,
) -> Result<(), ManifestStoreError> {
    let source = manifests
        .iter()
        .flatten()
        .copied()
        .find(|manifest| manifest.cutoff.journal().generation() == intent.source_generation)
        .ok_or(ManifestStoreError::InvalidGeneration)?;
    let source_catalog = match source.catalog {
        Some(reference) => read_referenced_catalog(directory, reference, store_id)?,
        None => GenerationCatalogSnapshot::empty(store_id),
    };
    let current_catalog = read_referenced_catalog(
        directory,
        current
            .catalog
            .ok_or(ManifestStoreError::InvalidGeneration)?,
        store_id,
    )?;
    let last = current_catalog
        .entries()
        .last()
        .ok_or(ManifestStoreError::InvalidGeneration)?;
    validate_catalog_advancing_transition(source, current, &source_catalog, &current_catalog)?;
    let expected_catalog_generation = match source_catalog.reference() {
        Some(reference) => reference.generation().checked_add(1),
        None => Some(1),
    };
    if source.cutoff.journal().generation() != intent.source_generation
        || source.cutoff.append_sequence() != intent.sequence_cutoff
        || source.cutoff.end_offset() != intent.source_end_offset
        || source.cutoff.checkpoint_generation() != intent.source_checkpoint_generation
        || source.registry.generation != intent.registry_generation
        || current.cutoff.journal().generation() != intent.successor_generation
        || current.cutoff.checkpoint_generation() != 1
        || expected_catalog_generation != Some(intent.catalog_generation)
        || current_catalog
            .reference()
            .is_none_or(|reference| reference.generation() != intent.catalog_generation)
        || last.journal_generation() != intent.source_generation
        || last.sequence_floor() != source.sequence_floor
        || last.sequence_cutoff() != intent.sequence_cutoff
        || last.end_offset() != intent.source_end_offset
        || last.registry_generation() != intent.registry_generation
        || last.artifact_length() != intent.source_end_offset
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    validate_manifest_catalog_binding(current, &current_catalog)
}

fn validate_sealed_inventory_metadata(
    directory: &Path,
    catalog: &GenerationCatalogSnapshot,
) -> Result<(), ManifestStoreError> {
    for entry in catalog.entries() {
        let path = directory.join(sealed_journal_file_name(entry.journal_generation()));
        let mut file =
            File::open(path).map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
        let length = file
            .metadata()
            .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
            .len();
        if length != entry.artifact_length() || length != entry.end_offset() {
            return Err(ManifestStoreError::InvalidGeneration);
        }
        let mut header = [0_u8; crate::JOURNAL_V1_HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
        let decoded = crate::JournalHeaderV1::decode(&header)
            .map_err(|_| ManifestStoreError::InvalidGeneration)?;
        if decoded.store_id() != catalog.store_id() {
            return Err(ManifestStoreError::StoreMismatch);
        }
    }
    Ok(())
}

fn read_rotation_intent(
    directory: &Path,
    store_id: StoreId,
) -> Result<RotationIntent, ManifestStoreError> {
    let bytes = read_required_bounded(&directory.join(ROTATION_INTENT_FILE_NAME), 96)?;
    decode_rotation_intent(&bytes, store_id).map_err(map_generation_codec)
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::large_types_passed_by_value)]
fn rollback_uncommitted_rotation(
    config: &ManifestStoreConfig,
    directory: &File,
    manifests: [Option<ManifestRecord>; 2],
    current: ManifestRecord,
    intent: RotationIntent,
) -> Result<(), ManifestStoreError> {
    let expected_catalog_generation = match current.catalog {
        Some(reference) => reference.generation().checked_add(1),
        None => Some(1),
    };
    if current.cutoff.journal().generation() != intent.source_generation
        || current.cutoff.append_sequence() != intent.sequence_cutoff
        || current.cutoff.end_offset() != intent.source_end_offset
        || current.cutoff.checkpoint_generation() != intent.source_checkpoint_generation
        || current.registry.generation != intent.registry_generation
        || expected_catalog_generation != Some(intent.catalog_generation)
        || config
            .directory
            .join(REGISTRY_STAGING_FILE_NAME)
            .try_exists()
            .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
        || config
            .directory
            .join(RETRY_STAGING_FILE_NAME)
            .try_exists()
            .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    let prior_catalog = match current.catalog {
        Some(reference) => read_referenced_catalog(&config.directory, reference, config.store_id)?,
        None => GenerationCatalogSnapshot::empty(config.store_id),
    };
    let inventory = inspect_inventory(&config.directory)?;
    validate_generation_inventory(&inventory, current, &prior_catalog, Some(intent))?;
    let registry = read_referenced_registry(&config.directory, current.registry, config.store_id)?;
    let source_config = ActiveJournalConfig::new(
        config.directory.clone(),
        config.store_id,
        ActiveJournalOpenMode::OpenExisting,
        config.journal_limits,
    )
    .map_err(ManifestStoreError::Active)?
    .manifest_existing()
    .manifest_generation(intent.source_generation, current.sequence_floor)
    .map_err(ManifestStoreError::Active)?;
    let source = ActiveJournal::open(source_config)?;
    if source.durable_cutoff() != current.cutoff {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    validate_recovered_declarations(&registry, source.recovered_records())?;

    let expected_entry = |path: &Path| -> Result<SealedGeneration, ManifestStoreError> {
        let checksum = checksum_file_bounded(path, intent.source_end_offset)?;
        Ok(SealedGeneration::new(
            intent.source_generation,
            current.sequence_floor,
            intent.sequence_cutoff,
            intent.source_end_offset,
            intent.registry_generation,
            intent.source_end_offset,
            checksum,
        ))
    };
    let sealed_final = config
        .directory
        .join(sealed_journal_file_name(intent.source_generation));
    let sealed_staging = config.directory.join(SEALED_JOURNAL_STAGING_FILE_NAME);
    let mut sealed = None;
    for path in [&sealed_final, &sealed_staging] {
        if path
            .try_exists()
            .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
        {
            let entry = expected_entry(path)?;
            validate_sealed_journal(
                path,
                entry,
                config.store_id,
                config.journal_limits,
                &registry,
            )?;
            if sealed.is_some_and(|prior| prior != entry) {
                return Err(ManifestStoreError::InvalidGeneration);
            }
            sealed = Some(entry);
        }
    }

    let successor_journal = config
        .directory
        .join(active_journal_file_name(intent.successor_generation));
    let successor_checkpoint = config
        .directory
        .join(active_checkpoint_file_name(intent.successor_generation));
    let successor_journal_exists = successor_journal
        .try_exists()
        .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?;
    let successor_checkpoint_exists = successor_checkpoint
        .try_exists()
        .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?;
    let successor_cutoff = if successor_journal_exists || successor_checkpoint_exists {
        if !successor_journal_exists || !successor_checkpoint_exists {
            return Err(ManifestStoreError::InvalidGeneration);
        }
        let successor_config = ActiveJournalConfig::new(
            config.directory.clone(),
            config.store_id,
            ActiveJournalOpenMode::OpenExisting,
            config.journal_limits,
        )
        .map_err(ManifestStoreError::Active)?
        .manifest_existing()
        .manifest_generation(intent.successor_generation, intent.sequence_cutoff)
        .map_err(ManifestStoreError::Active)?;
        let successor = ActiveJournal::open(successor_config)?;
        if successor.inspection().active_records() != 0 {
            return Err(ManifestStoreError::InvalidGeneration);
        }
        Some(successor.durable_cutoff())
    } else {
        None
    };

    let candidate_slot = (0_u8..3)
        .find(|candidate| {
            !manifests.iter().flatten().any(|manifest| {
                manifest
                    .catalog
                    .is_some_and(|reference| reference.slot() == *candidate)
            })
        })
        .ok_or(ManifestStoreError::InvalidGeneration)?;
    let catalog_final = config
        .directory
        .join(CATALOG_SLOT_NAMES[usize::from(candidate_slot)]);
    let catalog_staging = config.directory.join(GENERATION_CATALOG_STAGING_FILE_NAME);
    let mut catalog_reference = None;
    for path in [&catalog_final, &catalog_staging] {
        if path
            .try_exists()
            .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
        {
            let bytes = read_required_bounded(path, MAX_GENERATION_CATALOG_BYTES)?;
            let decoded = decode_catalog(&bytes, candidate_slot, config.store_id)
                .map_err(map_generation_codec)?;
            let reference = decoded
                .reference()
                .ok_or(ManifestStoreError::InvalidGeneration)?;
            if reference.generation() != intent.catalog_generation
                || sealed.is_none_or(|entry| decoded.entries().last() != Some(&entry))
                || prior_catalog.entries().len().checked_add(1) != Some(decoded.entries().len())
                || decoded.entries()[..prior_catalog.entries().len()] != *prior_catalog.entries()
                || catalog_reference.is_some_and(|prior| prior != reference)
            {
                return Err(ManifestStoreError::InvalidGeneration);
            }
            catalog_reference = Some(reference);
        }
    }
    let manifest_staging = config.directory.join(MANIFEST_STAGING_FILE_NAME);
    if manifest_staging
        .try_exists()
        .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
    {
        let bytes = read_required_bounded(&manifest_staging, MANIFEST_LEN)?;
        let decoded = decode_manifest(&bytes, config.store_id)?;
        if successor_cutoff.is_none_or(|cutoff| decoded.cutoff != cutoff)
            || current.generation.checked_add(1) != Some(decoded.generation)
            || decoded.registry != current.registry
            || decoded.retry != current.retry
            || decoded.recovery != current.recovery
            || decoded.sequence_floor != intent.sequence_cutoff
            || decoded.catalog != catalog_reference
        {
            return Err(ManifestStoreError::InvalidGeneration);
        }
    }
    drop(source);
    for path in [
        manifest_staging,
        catalog_staging,
        catalog_final,
        sealed_staging,
        sealed_final,
        successor_journal,
        successor_checkpoint,
        config.directory.join(ROTATION_INTENT_FILE_NAME),
    ] {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(manifest_io(ManifestIoOperation::Remove, &error)),
        }
    }
    directory
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))
}

fn checksum_file_bounded(path: &Path, expected_length: u64) -> Result<u32, ManifestStoreError> {
    if expected_length == 0 || expected_length > crate::MAX_ACTIVE_JOURNAL_BYTES {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    let mut file =
        File::open(path).map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
    if file
        .metadata()
        .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
        .len()
        != expected_length
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    let mut remaining = expected_length;
    let mut buffer = vec![0_u8; 64 * 1_024].into_boxed_slice();
    let mut checksum = StreamingCrc32c::new();
    while remaining > 0 {
        let count = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| ManifestStoreError::InvalidGeneration)?;
        file.read_exact(&mut buffer[..count])
            .map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
        checksum.update(&buffer[..count]);
        remaining -= count as u64;
    }
    Ok(checksum.finish())
}

fn read_referenced_retry(
    directory: &Path,
    owning_manifest: ManifestRecord,
    store_id: StoreId,
    options: RetryPersistenceOptions,
    catalog: &GenerationCatalogSnapshot,
) -> Result<RetryStateSnapshot, ManifestStoreError> {
    let reference = owning_manifest.retry;
    let name = RETRY_SLOT_NAMES
        .get(usize::from(reference.public.slot()))
        .ok_or(ManifestStoreError::InvalidRetry)?;
    let bytes = read_required_bounded(&directory.join(name), MAX_RETRY_STATE_BYTES)?;
    if u64::try_from(bytes.len()).ok() != Some(reference.length)
        || crc32c(&bytes) != reference.checksum
    {
        return Err(ManifestStoreError::InvalidRetry);
    }
    let (decoded_reference, snapshot) =
        decode_retry_state_at_slot(&bytes, reference.public.slot(), store_id, options)
            .map_err(map_retry_codec)?;
    if decoded_reference != reference
        || !snapshot.validates_root_with_catalog(owning_manifest.commit(), catalog)
    {
        return Err(ManifestStoreError::InvalidRetry);
    }
    Ok(snapshot)
}

#[allow(clippy::large_types_passed_by_value)]
fn validate_retry_inventory(
    directory: &Path,
    manifests: [Option<ManifestRecord>; 2],
    store_id: StoreId,
    options: RetryPersistenceOptions,
    allow_rotation_redundancy: bool,
) -> Result<(), ManifestStoreError> {
    let oldest_referenced_generation = manifests
        .iter()
        .flatten()
        .map(|manifest| manifest.retry)
        .map(|reference| reference.public.generation())
        .min();
    for (slot, name) in RETRY_SLOT_NAMES.iter().enumerate() {
        let Some(bytes) = read_optional_bounded(&directory.join(name), MAX_RETRY_STATE_BYTES)?
        else {
            continue;
        };
        let slot = u8::try_from(slot).map_err(|_| ManifestStoreError::InvalidRetry)?;
        let (reference, _) =
            decode_retry_state_at_slot(&bytes, slot, store_id, options).map_err(map_retry_codec)?;
        let referenced = manifests
            .iter()
            .flatten()
            .any(|manifest| manifest.retry == reference);
        if !(referenced
            || allow_rotation_redundancy
                && oldest_referenced_generation
                    .is_some_and(|oldest| reference.public.generation() < oldest))
        {
            return Err(ManifestStoreError::InvalidRetry);
        }
    }
    for manifest in manifests.into_iter().flatten() {
        let manifest_catalog = match manifest.catalog {
            Some(reference) => read_referenced_catalog(directory, reference, store_id)?,
            None => GenerationCatalogSnapshot::empty(store_id),
        };
        let _ = read_referenced_retry(directory, manifest, store_id, options, &manifest_catalog)?;
    }
    Ok(())
}

fn read_interrupted_genesis_retry(
    directory: &Path,
    store_id: StoreId,
    options: RetryPersistenceOptions,
) -> Result<RetryArtifactReference, ManifestStoreError> {
    let mut found = None;
    for (slot, name) in RETRY_SLOT_NAMES.iter().enumerate() {
        let Some(bytes) = read_optional_bounded(&directory.join(name), MAX_RETRY_STATE_BYTES)?
        else {
            continue;
        };
        if found.is_some() {
            return Err(ManifestStoreError::InvalidRetry);
        }
        let slot = u8::try_from(slot).map_err(|_| ManifestStoreError::InvalidRetry)?;
        let (reference, snapshot) =
            decode_retry_state_at_slot(&bytes, slot, store_id, options).map_err(map_retry_codec)?;
        if reference.public.generation() != 1
            || !snapshot.replay().is_empty()
            || !snapshot.guard().is_empty()
        {
            return Err(ManifestStoreError::InvalidRetry);
        }
        found = Some(reference);
    }
    found.ok_or(ManifestStoreError::InvalidRetry)
}

#[allow(clippy::large_types_passed_by_value)]
fn remove_unreferenced_retry_slots(
    directory_path: &Path,
    directory: &File,
    manifests: [Option<ManifestRecord>; 2],
) -> Result<(), ManifestStoreError> {
    let mut removed = false;
    for (slot, name) in RETRY_SLOT_NAMES.iter().enumerate() {
        let slot = u8::try_from(slot).map_err(|_| ManifestStoreError::InvalidRetry)?;
        if manifests
            .iter()
            .flatten()
            .any(|manifest| manifest.retry.public.slot() == slot)
        {
            continue;
        }
        match std::fs::remove_file(directory_path.join(name)) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(manifest_io(ManifestIoOperation::Remove, &error)),
        }
    }
    if removed {
        directory
            .sync_all()
            .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))?;
    }
    Ok(())
}

fn validate_pending_retry(
    prior: DurableCutoff,
    cutoff: DurableCutoff,
    pending: &[PendingRetryOutcome],
) -> Result<(), ManifestStoreError> {
    validate_pending_retry_preflight(prior, cutoff, pending.len())?;
    let mut expected_sequence = prior
        .append_sequence()
        .checked_add(1)
        .ok_or(ManifestStoreError::GenerationExhausted)?;
    let mut previous_end = prior.end_offset();
    for entry in pending {
        if entry.append_sequence() != expected_sequence || entry.end_offset() <= previous_end {
            return Err(ManifestStoreError::InvalidRetry);
        }
        previous_end = entry.end_offset();
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(ManifestStoreError::GenerationExhausted)?;
    }
    let last = pending.last().ok_or(ManifestStoreError::InvalidRetry)?;
    if last.append_sequence() != cutoff.append_sequence()
        || last.end_offset() != cutoff.end_offset()
    {
        return Err(ManifestStoreError::InvalidRetry);
    }
    Ok(())
}

fn validate_pending_retry_preflight(
    prior: DurableCutoff,
    cutoff: DurableCutoff,
    pending_len: usize,
) -> Result<(), ManifestStoreError> {
    let delta = cutoff
        .append_sequence()
        .checked_sub(prior.append_sequence())
        .and_then(|delta| usize::try_from(delta).ok());
    if pending_len == 0
        || pending_len > MAX_PERSISTED_RETRY_ENTRIES
        || prior.journal() != cutoff.journal()
        || delta != Some(pending_len)
        || cutoff.end_offset() <= prior.end_offset()
    {
        return Err(ManifestStoreError::InvalidRetry);
    }
    Ok(())
}

fn map_retry_codec(error: RetryStateCodecError) -> ManifestStoreError {
    match error {
        RetryStateCodecError::Invalid | RetryStateCodecError::OptionsMismatch => {
            ManifestStoreError::InvalidRetry
        }
        RetryStateCodecError::StoreMismatch => ManifestStoreError::StoreMismatch,
    }
}

fn map_recovery_codec(error: RecoveryCodecError) -> ManifestStoreError {
    match error {
        RecoveryCodecError::Invalid => ManifestStoreError::InvalidManifest,
        RecoveryCodecError::StoreMismatch => ManifestStoreError::StoreMismatch,
    }
}

fn map_generation_codec(error: GenerationCodecError) -> ManifestStoreError {
    match error {
        GenerationCodecError::Invalid => ManifestStoreError::InvalidGeneration,
        GenerationCodecError::StoreMismatch => ManifestStoreError::StoreMismatch,
    }
}

fn publish_rotation_intent(
    directory_path: &Path,
    directory: &File,
    intent: RotationIntent,
) -> Result<(), ManifestStoreError> {
    let path = directory_path.join(ROTATION_INTENT_FILE_NAME);
    let bytes = encode_rotation_intent(intent);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| {
            if error.kind() == ErrorKind::AlreadyExists {
                ManifestStoreError::InterruptedPublication
            } else {
                manifest_io(ManifestIoOperation::CreateArtifact, &error)
            }
        })?;
    file.write_all(&bytes)
        .map_err(|error| manifest_io(ManifestIoOperation::Write, &error))?;
    injected_rotation_fault(20, ManifestIoOperation::Write)?;
    file.sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncArtifact, &error))?;
    injected_rotation_fault(21, ManifestIoOperation::SyncArtifact)?;
    let readback = read_required_bounded(&path, bytes.len())?;
    if decode_rotation_intent(&readback, intent.store_id).map_err(map_generation_codec)? != intent {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    injected_rotation_fault(22, ManifestIoOperation::Read)?;
    directory
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))?;
    injected_rotation_fault(23, ManifestIoOperation::SyncDirectory)
}

fn prepare_sealed_generation(
    directory_path: &Path,
    intent: RotationIntent,
    sequence_floor: u64,
    limits: ActiveJournalLimits,
    registry: &SeriesRegistry,
) -> Result<SealedGeneration, ManifestStoreError> {
    let source_path = directory_path.join(active_journal_file_name(intent.source_generation));
    let target_path = directory_path.join(sealed_journal_file_name(intent.source_generation));
    if target_path
        .try_exists()
        .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    let checksum = checksum_file_bounded(&source_path, intent.source_end_offset)?;
    let sealed = SealedGeneration::new(
        intent.source_generation,
        sequence_floor,
        intent.sequence_cutoff,
        intent.source_end_offset,
        intent.registry_generation,
        intent.source_end_offset,
        checksum,
    );
    validate_sealed_journal(&source_path, sealed, intent.store_id, limits, registry)?;
    Ok(sealed)
}

fn publish_sealed_generation(
    directory_path: &Path,
    directory: &File,
    intent: RotationIntent,
    expected: SealedGeneration,
) -> Result<(), ManifestStoreError> {
    let source_path = directory_path.join(active_journal_file_name(intent.source_generation));
    let staging_path = directory_path.join(SEALED_JOURNAL_STAGING_FILE_NAME);
    let target_path = directory_path.join(sealed_journal_file_name(intent.source_generation));
    if target_path
        .try_exists()
        .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    let mut source =
        File::open(source_path).map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
    if source
        .metadata()
        .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
        .len()
        != intent.source_end_offset
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    let mut staging = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&staging_path)
        .map_err(|error| {
            if error.kind() == ErrorKind::AlreadyExists {
                ManifestStoreError::InterruptedPublication
            } else {
                manifest_io(ManifestIoOperation::CreateArtifact, &error)
            }
        })?;
    let mut remaining = intent.source_end_offset;
    let mut buffer = vec![0_u8; 64 * 1_024].into_boxed_slice();
    let mut checksum = StreamingCrc32c::new();
    while remaining > 0 {
        let count = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| ManifestStoreError::InvalidGeneration)?;
        source
            .read_exact(&mut buffer[..count])
            .map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
        staging
            .write_all(&buffer[..count])
            .map_err(|error| manifest_io(ManifestIoOperation::Write, &error))?;
        checksum.update(&buffer[..count]);
        remaining -= count as u64;
    }
    injected_rotation_fault(24, ManifestIoOperation::Write)?;
    staging
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncArtifact, &error))?;
    injected_rotation_fault(25, ManifestIoOperation::SyncArtifact)?;
    if checksum.finish() != expected.artifact_checksum()
        || checksum_file_bounded(&staging_path, expected.artifact_length())?
            != expected.artifact_checksum()
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    injected_rotation_fault(26, ManifestIoOperation::Read)?;
    std::fs::rename(&staging_path, &target_path)
        .map_err(|error| manifest_io(ManifestIoOperation::Publish, &error))?;
    injected_rotation_fault(27, ManifestIoOperation::Publish)?;
    directory
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))?;
    injected_rotation_fault(28, ManifestIoOperation::SyncDirectory)?;
    if checksum_file_bounded(&target_path, expected.artifact_length())?
        != expected.artifact_checksum()
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    injected_rotation_fault(29, ManifestIoOperation::Read)?;
    Ok(())
}

fn validate_sealed_journal(
    path: &Path,
    expected: SealedGeneration,
    store_id: StoreId,
    limits: ActiveJournalLimits,
    registry: &SeriesRegistry,
) -> Result<(), ManifestStoreError> {
    let mut file =
        File::open(path).map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
    if file
        .metadata()
        .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
        .len()
        != expected.artifact_length()
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    let mut checksum = StreamingCrc32c::new();
    let mut header = [0_u8; crate::JOURNAL_V1_HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
    checksum.update(&header);
    let header = crate::JournalHeaderV1::decode(&header)
        .map_err(|_| ManifestStoreError::InvalidGeneration)?;
    if header.store_id() != store_id {
        return Err(ManifestStoreError::StoreMismatch);
    }
    let mut offset = crate::JOURNAL_V1_HEADER_LEN as u64;
    let mut count = 0_usize;
    let mut previous = if expected.sequence_floor() == 0 {
        None
    } else {
        Some(
            crate::AppendSequenceV1::new(expected.sequence_floor())
                .map_err(|_| ManifestStoreError::InvalidGeneration)?,
        )
    };
    while offset < expected.end_offset() {
        count = count
            .checked_add(1)
            .ok_or(ManifestStoreError::InvalidGeneration)?;
        if count > limits.max_active_records() {
            return Err(ManifestStoreError::InvalidGeneration);
        }
        let mut prefix = [0_u8; crate::JOURNAL_V1_FRAME_PREFIX_LEN];
        file.read_exact(&mut prefix)
            .map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
        let frame_len = frame_len_from_prefix_v1(
            &prefix,
            crate::DecodeLimitsV1::new(limits.max_payload_len())
                .map_err(|_| ManifestStoreError::InvalidGeneration)?,
        )
        .map_err(|_| ManifestStoreError::InvalidGeneration)?;
        let mut frame = vec![0_u8; frame_len];
        frame[..prefix.len()].copy_from_slice(&prefix);
        file.read_exact(&mut frame[prefix.len()..])
            .map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
        checksum.update(&frame);
        let decoded = crate::decode_admission_frame_v1(
            &frame,
            crate::DecodeLimitsV1::new(limits.max_payload_len())
                .map_err(|_| ManifestStoreError::InvalidGeneration)?,
            previous,
        )
        .map_err(|_| ManifestStoreError::InvalidGeneration)?;
        let declaration = registry.resolve(
            decoded.declaration().series_id(),
            decoded.declaration().revision(),
        );
        if decoded.store_id() != store_id
            || declaration
                .is_none_or(|declaration| !declaration_matches(declaration, decoded.declaration()))
        {
            return Err(ManifestStoreError::InvalidGeneration);
        }
        previous = Some(
            crate::AppendSequenceV1::new(decoded.append_sequence())
                .map_err(|_| ManifestStoreError::InvalidGeneration)?,
        );
        offset = offset
            .checked_add(
                u64::try_from(frame_len).map_err(|_| ManifestStoreError::InvalidGeneration)?,
            )
            .ok_or(ManifestStoreError::InvalidGeneration)?;
        if offset > expected.end_offset() {
            return Err(ManifestStoreError::InvalidGeneration);
        }
    }
    if offset != expected.end_offset()
        || previous.map(crate::AppendSequenceV1::get) != Some(expected.sequence_cutoff())
        || checksum.finish() != expected.artifact_checksum()
    {
        return Err(ManifestStoreError::InvalidGeneration);
    }
    Ok(())
}

fn cleanup_committed_rotation(
    directory_path: &Path,
    directory: &File,
    source_generation: u64,
) -> Result<(), ManifestStoreError> {
    for name in [
        active_journal_file_name(source_generation),
        active_checkpoint_file_name(source_generation),
        ROTATION_INTENT_FILE_NAME.to_owned(),
        SEALED_JOURNAL_STAGING_FILE_NAME.to_owned(),
    ] {
        match std::fs::remove_file(directory_path.join(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(manifest_io(ManifestIoOperation::Remove, &error)),
        }
    }
    directory
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))
}

#[allow(clippy::large_types_passed_by_value)]
fn validate_recovery_report_coverage(
    manifests: [Option<ManifestRecord>; 2],
    inventory: &RecoveryInventoryState,
    current: ManifestRecord,
    catalog: &GenerationCatalogSnapshot,
    journal: &ActiveJournal,
) -> Result<(), ManifestStoreError> {
    let mut validated = Vec::new();
    for reference in manifests
        .iter()
        .flatten()
        .filter_map(|manifest| manifest.recovery)
    {
        if validated.contains(&reference) {
            continue;
        }
        let report = referenced_recovery(inventory, reference)?;
        if report.committing_manifest_generation() > current.generation {
            return Err(ManifestStoreError::InvalidManifest);
        }
        let covered = if report.active_generation() == current.cutoff.journal().generation() {
            report.active_sequence_floor() == current.sequence_floor
                && report.append_sequence() >= current.sequence_floor
                && report.append_sequence() <= current.cutoff.append_sequence()
                && report.committed_end_offset() <= current.cutoff.end_offset()
                && ((report.append_sequence() == current.sequence_floor
                    && report.committed_end_offset() == crate::JOURNAL_V1_HEADER_LEN as u64)
                    || journal.recovered_records().iter().any(|record| {
                        record.admission().append_sequence() == report.append_sequence()
                            && record.end_offset() == report.committed_end_offset()
                    }))
        } else {
            catalog.entries().iter().any(|entry| {
                entry.journal_generation() == report.active_generation()
                    && entry.sequence_floor() == report.active_sequence_floor()
                    && ((report.append_sequence() == entry.sequence_floor()
                        && report.committed_end_offset() == crate::JOURNAL_V1_HEADER_LEN as u64)
                        || (report.append_sequence() > entry.sequence_floor()
                            && report.append_sequence() <= entry.sequence_cutoff()
                            && report.committed_end_offset() > crate::JOURNAL_V1_HEADER_LEN as u64
                            && report.committed_end_offset() <= entry.end_offset()))
            })
        };
        if !covered {
            return Err(ManifestStoreError::InvalidManifest);
        }
        validated.push(reference);
    }
    Ok(())
}

fn converge_recovery_staging(
    directory_path: &Path,
    directory: &File,
    artifact: RecoveryArtifact,
    store_id: StoreId,
) -> Result<(), ManifestStoreError> {
    let staging = directory_path.join(RECOVERY_STAGING_FILE_NAME);
    let bytes = read_required_bounded(&staging, RECOVERY_STATE_LEN)?;
    if decode_recovery_state(&bytes, store_id).map_err(map_recovery_codec)? != artifact.report
        || crc32c(&bytes) != artifact.reference.checksum
    {
        return Err(ManifestStoreError::InvalidManifest);
    }
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(&staging)
        .map_err(|error| manifest_io(ManifestIoOperation::OpenArtifact, &error))?
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncArtifact, &error))?;
    std::fs::rename(
        staging,
        directory_path.join(RECOVERY_SLOT_NAMES[usize::from(artifact.reference.slot)]),
    )
    .map_err(|error| manifest_io(ManifestIoOperation::Publish, &error))?;
    directory
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))
}

fn converge_manifest_staging(
    directory_path: &Path,
    directory: &File,
    target: usize,
    expected: ManifestRecord,
) -> Result<usize, ManifestStoreError> {
    let staging = directory_path.join(MANIFEST_STAGING_FILE_NAME);
    let bytes = read_required_bounded(&staging, MANIFEST_LEN)?;
    if bytes != encode_manifest(expected) {
        return Err(ManifestStoreError::InvalidManifest);
    }
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(&staging)
        .map_err(|error| manifest_io(ManifestIoOperation::OpenArtifact, &error))?
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncArtifact, &error))?;
    std::fs::rename(staging, directory_path.join(MANIFEST_SLOT_NAMES[target]))
        .map_err(|error| manifest_io(ManifestIoOperation::Publish, &error))?;
    directory
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))?;
    Ok(target)
}

#[allow(clippy::large_types_passed_by_value)]
fn remove_unreferenced_recovery_slots(
    directory_path: &Path,
    directory: &File,
    manifests: [Option<ManifestRecord>; 2],
    store_id: StoreId,
) -> Result<(), ManifestStoreError> {
    let referenced = manifests
        .iter()
        .flatten()
        .filter_map(|manifest| manifest.recovery)
        .collect::<Vec<_>>();
    let latest_generation = referenced
        .iter()
        .map(|reference| {
            let bytes = read_required_bounded(
                &directory_path.join(RECOVERY_SLOT_NAMES[usize::from(reference.slot)]),
                RECOVERY_STATE_LEN,
            )?;
            if crc32c(&bytes) != reference.checksum {
                return Err(ManifestStoreError::InvalidManifest);
            }
            decode_recovery_state(&bytes, store_id)
                .map_err(map_recovery_codec)
                .map(RecoveryReport::report_generation)
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max();
    let mut removed = false;
    for (slot, name) in RECOVERY_SLOT_NAMES.iter().enumerate() {
        let slot = u8::try_from(slot).map_err(|_| ManifestStoreError::InvalidManifest)?;
        if referenced.iter().any(|reference| reference.slot == slot) {
            continue;
        }
        let Some(bytes) = read_optional_bounded(&directory_path.join(name), RECOVERY_STATE_LEN)?
        else {
            continue;
        };
        let report = decode_recovery_state(&bytes, store_id).map_err(map_recovery_codec)?;
        if latest_generation.is_none_or(|latest| report.report_generation() >= latest) {
            return Err(ManifestStoreError::InvalidManifest);
        }
        #[cfg(test)]
        if let Some((kind, raw_os_error)) = take_publish_fault(53) {
            return Err(classify_manifest_io(
                ManifestIoOperation::Remove,
                kind,
                raw_os_error,
            ));
        }
        std::fs::remove_file(directory_path.join(name))
            .map_err(|error| manifest_io(ManifestIoOperation::Remove, &error))?;
        removed = true;
    }
    if removed {
        directory
            .sync_all()
            .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))?;
    }
    Ok(())
}

#[allow(clippy::large_types_passed_by_value)]
fn remove_unreferenced_catalog_slots(
    directory_path: &Path,
    directory: &File,
    manifests: [Option<ManifestRecord>; 2],
) -> Result<(), ManifestStoreError> {
    let mut removed = false;
    for (slot, name) in CATALOG_SLOT_NAMES.iter().enumerate() {
        let slot = u8::try_from(slot).map_err(|_| ManifestStoreError::InvalidGeneration)?;
        if manifests.iter().flatten().any(|manifest| {
            manifest
                .catalog
                .is_some_and(|reference| reference.slot() == slot)
        }) {
            continue;
        }
        match std::fs::remove_file(directory_path.join(name)) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(manifest_io(ManifestIoOperation::Remove, &error)),
        }
    }
    if removed {
        directory
            .sync_all()
            .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))?;
    }
    Ok(())
}

fn publish_reusable_slot(
    directory_path: &Path,
    directory: &File,
    staging_name: &str,
    target_name: &str,
    bytes: &[u8],
    maximum: usize,
    verify: impl FnOnce(&[u8]) -> Result<(), ManifestStoreError>,
) -> Result<(), ManifestStoreError> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(ManifestStoreError::InvalidOptions);
    }
    let staging_path = directory_path.join(staging_name);
    let target_path = directory_path.join(target_name);
    let mut staging = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&staging_path)
        .map_err(|error| {
            if error.kind() == ErrorKind::AlreadyExists {
                ManifestStoreError::InterruptedPublication
            } else {
                manifest_io(ManifestIoOperation::CreateArtifact, &error)
            }
        })?;
    injected_publication_fault(staging_name, PublicationPoint::Write)?;
    staging
        .write_all(bytes)
        .map_err(|error| manifest_io(ManifestIoOperation::Write, &error))?;
    injected_publication_fault(staging_name, PublicationPoint::SyncArtifact)?;
    staging
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncArtifact, &error))?;
    injected_publication_fault(staging_name, PublicationPoint::Readback)?;
    let candidate = read_required_bounded(&staging_path, maximum)?;
    verify(&candidate)?;
    injected_publication_fault(staging_name, PublicationPoint::Publish)?;
    std::fs::rename(&staging_path, &target_path)
        .map_err(|error| manifest_io(ManifestIoOperation::Publish, &error))?;
    injected_publication_fault(staging_name, PublicationPoint::SyncDirectory)?;
    directory
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))
}

#[derive(Clone, Copy)]
enum PublicationPoint {
    Write = 1,
    SyncArtifact = 2,
    Readback = 3,
    Publish = 4,
    SyncDirectory = 5,
}

#[cfg(not(test))]
#[allow(clippy::unnecessary_wraps)]
const fn injected_publication_fault(
    _staging_name: &str,
    _point: PublicationPoint,
) -> Result<(), ManifestStoreError> {
    Ok(())
}

#[cfg(test)]
fn injected_publication_fault(
    staging_name: &str,
    point: PublicationPoint,
) -> Result<(), ManifestStoreError> {
    let code = match (staging_name, point) {
        (STORE_FORMAT_STAGING_FILE_NAME, PublicationPoint::Write) => 40,
        (STORE_FORMAT_STAGING_FILE_NAME, PublicationPoint::SyncArtifact) => 41,
        (STORE_FORMAT_STAGING_FILE_NAME, PublicationPoint::Readback) => 42,
        (STORE_FORMAT_STAGING_FILE_NAME, PublicationPoint::Publish) => 43,
        (STORE_FORMAT_STAGING_FILE_NAME, PublicationPoint::SyncDirectory) => 44,
        (REGISTRY_STAGING_FILE_NAME, PublicationPoint::Write) => 1,
        (REGISTRY_STAGING_FILE_NAME, PublicationPoint::SyncArtifact) => 2,
        (REGISTRY_STAGING_FILE_NAME, PublicationPoint::Readback) => 9,
        (REGISTRY_STAGING_FILE_NAME, PublicationPoint::Publish) => 3,
        (REGISTRY_STAGING_FILE_NAME, PublicationPoint::SyncDirectory) => 4,
        (MANIFEST_STAGING_FILE_NAME, PublicationPoint::Write) => 5,
        (MANIFEST_STAGING_FILE_NAME, PublicationPoint::SyncArtifact) => 6,
        (MANIFEST_STAGING_FILE_NAME, PublicationPoint::Readback) => 10,
        (MANIFEST_STAGING_FILE_NAME, PublicationPoint::Publish) => 7,
        (MANIFEST_STAGING_FILE_NAME, PublicationPoint::SyncDirectory) => 8,
        (RETRY_STAGING_FILE_NAME, PublicationPoint::Write) => 11,
        (RETRY_STAGING_FILE_NAME, PublicationPoint::SyncArtifact) => 12,
        (RETRY_STAGING_FILE_NAME, PublicationPoint::Readback) => 13,
        (RETRY_STAGING_FILE_NAME, PublicationPoint::Publish) => 14,
        (RETRY_STAGING_FILE_NAME, PublicationPoint::SyncDirectory) => 15,
        (GENERATION_CATALOG_STAGING_FILE_NAME, PublicationPoint::Write) => 30,
        (GENERATION_CATALOG_STAGING_FILE_NAME, PublicationPoint::SyncArtifact) => 31,
        (GENERATION_CATALOG_STAGING_FILE_NAME, PublicationPoint::Readback) => 32,
        (GENERATION_CATALOG_STAGING_FILE_NAME, PublicationPoint::Publish) => 33,
        (GENERATION_CATALOG_STAGING_FILE_NAME, PublicationPoint::SyncDirectory) => 34,
        (RECOVERY_STAGING_FILE_NAME, PublicationPoint::Write) => 45,
        (RECOVERY_STAGING_FILE_NAME, PublicationPoint::SyncArtifact) => 46,
        (RECOVERY_STAGING_FILE_NAME, PublicationPoint::Readback) => 47,
        (RECOVERY_STAGING_FILE_NAME, PublicationPoint::Publish) => 48,
        (RECOVERY_STAGING_FILE_NAME, PublicationPoint::SyncDirectory) => 49,
        _ => 0,
    };
    if code == 0 {
        return Ok(());
    }
    if let Some((kind, raw_os_error)) = take_publish_fault(code) {
        let operation = match point {
            PublicationPoint::Write => ManifestIoOperation::Write,
            PublicationPoint::SyncArtifact => ManifestIoOperation::SyncArtifact,
            PublicationPoint::Readback => ManifestIoOperation::Read,
            PublicationPoint::Publish => ManifestIoOperation::Publish,
            PublicationPoint::SyncDirectory => ManifestIoOperation::SyncDirectory,
        };
        return Err(classify_manifest_io(operation, kind, raw_os_error));
    }
    Ok(())
}

#[cfg(not(test))]
#[allow(clippy::unnecessary_wraps)]
const fn injected_rotation_fault(
    _code: u8,
    _operation: ManifestIoOperation,
) -> Result<(), ManifestStoreError> {
    Ok(())
}

#[cfg(test)]
fn injected_rotation_fault(
    code: u8,
    operation: ManifestIoOperation,
) -> Result<(), ManifestStoreError> {
    if let Some((kind, raw_os_error)) = take_publish_fault(code) {
        return Err(classify_manifest_io(operation, kind, raw_os_error));
    }
    Ok(())
}

fn read_optional_bounded(
    path: &Path,
    maximum: usize,
) -> Result<Option<Vec<u8>>, ManifestStoreError> {
    match read_required_bounded(path, maximum) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(ManifestStoreError::Io(evidence)) if evidence.kind == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn read_required_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>, ManifestStoreError> {
    let mut file =
        File::open(path).map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
    let length = file
        .metadata()
        .map_err(|error| manifest_io(ManifestIoOperation::Metadata, &error))?
        .len();
    let length = usize::try_from(length).map_err(|_| ManifestStoreError::InvalidInventory)?;
    if length == 0 || length > maximum {
        return Err(ManifestStoreError::InvalidInventory);
    }
    let mut bytes = vec![0_u8; length];
    file.read_exact(&mut bytes)
        .map_err(|error| manifest_io(ManifestIoOperation::Read, &error))?;
    Ok(bytes)
}

fn encode_manifest(record: ManifestRecord) -> Vec<u8> {
    let mut bytes = vec![0_u8; MANIFEST_LEN];
    bytes[..8].copy_from_slice(&MANIFEST_MAGIC);
    bytes[8..10].copy_from_slice(&MANIFEST_VERSION.to_be_bytes());
    let length_u16 = u16::try_from(MANIFEST_LEN).expect("fixed manifest length fits u16");
    bytes[10..12].copy_from_slice(&length_u16.to_be_bytes());
    bytes[12..28].copy_from_slice(record.cutoff.journal().store_id().as_bytes());
    bytes[28..36].copy_from_slice(&record.generation.to_be_bytes());
    bytes[36..44].copy_from_slice(&record.cutoff.journal().generation().to_be_bytes());
    bytes[44..52].copy_from_slice(&record.cutoff.checkpoint_generation().to_be_bytes());
    bytes[52..60].copy_from_slice(&record.cutoff.append_sequence().to_be_bytes());
    bytes[60..68].copy_from_slice(&record.cutoff.end_offset().to_be_bytes());
    bytes[68] = record.registry.slot;
    bytes[72..80].copy_from_slice(&record.registry.generation.to_be_bytes());
    bytes[80..88].copy_from_slice(&record.registry.length.to_be_bytes());
    bytes[88..92].copy_from_slice(&record.registry.checksum.to_be_bytes());
    bytes[92] = record.retry.public.slot();
    bytes[96..104].copy_from_slice(&record.retry.public.generation().to_be_bytes());
    bytes[104..112].copy_from_slice(&record.retry.length.to_be_bytes());
    bytes[112..116].copy_from_slice(&record.retry.checksum.to_be_bytes());
    if let Some(recovery) = record.recovery {
        bytes[116] = 1;
        bytes[117] = recovery.slot;
        bytes[120..124].copy_from_slice(&recovery.checksum.to_be_bytes());
    }
    bytes[124..132].copy_from_slice(&record.sequence_floor.to_be_bytes());
    if let Some(catalog) = record.catalog {
        bytes[132] = catalog.slot();
        bytes[136..144].copy_from_slice(&catalog.generation().to_be_bytes());
        bytes[144..152].copy_from_slice(&catalog.length().to_be_bytes());
        bytes[152..156].copy_from_slice(&catalog.checksum().to_be_bytes());
    }
    let checksum = crc32c(&bytes[..156]);
    bytes[156..160].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

#[allow(clippy::too_many_lines)]
fn decode_manifest(bytes: &[u8], store_id: StoreId) -> Result<ManifestRecord, ManifestStoreError> {
    if bytes.len() != MANIFEST_LEN
        || bytes[..8] != MANIFEST_MAGIC
        || u16::from_be_bytes(bytes[8..10].try_into().unwrap_or_default()) != MANIFEST_VERSION
        || u16::from_be_bytes(bytes[10..12].try_into().unwrap_or_default())
            != u16::try_from(MANIFEST_LEN).unwrap_or_default()
        || bytes[69..72].iter().any(|byte| *byte != 0)
        || bytes[93..96].iter().any(|byte| *byte != 0)
        || bytes[133..136].iter().any(|byte| *byte != 0)
        || crc32c(&bytes[..156])
            != u32::from_be_bytes(bytes[156..160].try_into().unwrap_or_default())
    {
        return Err(ManifestStoreError::InvalidManifest);
    }
    let retry = RetryArtifactReference {
        public: RetryStateReference::new(
            bytes[92],
            u64::from_be_bytes(bytes[96..104].try_into().unwrap_or_default()),
        ),
        length: u64::from_be_bytes(bytes[104..112].try_into().unwrap_or_default()),
        checksum: u32::from_be_bytes(bytes[112..116].try_into().unwrap_or_default()),
    };
    let recovery = match bytes[116] {
        0 if bytes[117..124].iter().all(|byte| *byte == 0) => None,
        1 if bytes[117] < 3 && bytes[118..120].iter().all(|byte| *byte == 0) => {
            Some(RecoveryReference {
                slot: bytes[117],
                checksum: u32::from_be_bytes(bytes[120..124].try_into().unwrap_or_default()),
            })
        }
        _ => return Err(ManifestStoreError::InvalidManifest),
    };
    if retry.public.slot() >= 3
        || retry.public.generation() == 0
        || retry.length == 0
        || usize::try_from(retry.length).map_or(true, |length| length > MAX_RETRY_STATE_BYTES)
    {
        return Err(ManifestStoreError::InvalidManifest);
    }
    let sequence_floor = u64::from_be_bytes(bytes[124..132].try_into().unwrap_or_default());
    let catalog = if bytes[132..156].iter().all(|byte| *byte == 0) {
        None
    } else {
        let reference = GenerationCatalogReference::new(
            bytes[132],
            u64::from_be_bytes(bytes[136..144].try_into().unwrap_or_default()),
            u64::from_be_bytes(bytes[144..152].try_into().unwrap_or_default()),
            u32::from_be_bytes(bytes[152..156].try_into().unwrap_or_default()),
        );
        if reference.slot() >= 3
            || reference.generation() == 0
            || reference.length() == 0
            || usize::try_from(reference.length())
                .map_or(true, |length| length > MAX_GENERATION_CATALOG_BYTES)
        {
            return Err(ManifestStoreError::InvalidManifest);
        }
        Some(reference)
    };
    let decoded_store = StoreId::from_bytes(bytes[12..28].try_into().unwrap_or_default())
        .map_err(|_| ManifestStoreError::InvalidManifest)?;
    if decoded_store != store_id {
        return Err(ManifestStoreError::StoreMismatch);
    }
    let generation = u64::from_be_bytes(bytes[28..36].try_into().unwrap_or_default());
    let journal_generation = u64::from_be_bytes(bytes[36..44].try_into().unwrap_or_default());
    let checkpoint_generation = u64::from_be_bytes(bytes[44..52].try_into().unwrap_or_default());
    let append_sequence = u64::from_be_bytes(bytes[52..60].try_into().unwrap_or_default());
    let end_offset = u64::from_be_bytes(bytes[60..68].try_into().unwrap_or_default());
    let registry = RegistryReference {
        slot: bytes[68],
        generation: u64::from_be_bytes(bytes[72..80].try_into().unwrap_or_default()),
        length: u64::from_be_bytes(bytes[80..88].try_into().unwrap_or_default()),
        checksum: u32::from_be_bytes(bytes[88..92].try_into().unwrap_or_default()),
    };
    if generation == 0
        || journal_generation == 0
        || (journal_generation == ACTIVE_JOURNAL_GENERATION
            && (sequence_floor != 0 || catalog.is_some()))
        || (journal_generation > ACTIVE_JOURNAL_GENERATION
            && (sequence_floor == 0 || catalog.is_none()))
        || checkpoint_generation == 0
        || registry.slot >= 3
        || registry.generation == 0
        || registry.generation > generation
        || registry.length == 0
        || usize::try_from(registry.length)
            .map_or(true, |length| length > MAX_REGISTRY_SNAPSHOT_BYTES)
        || retry.public.generation() > generation
        || append_sequence < sequence_floor
        || (append_sequence == sequence_floor)
            != (end_offset == crate::JOURNAL_V1_HEADER_LEN as u64)
    {
        return Err(ManifestStoreError::InvalidManifest);
    }
    Ok(ManifestRecord {
        generation,
        registry,
        cutoff: DurableCutoff::from_manifest(
            store_id,
            journal_generation,
            checkpoint_generation,
            append_sequence,
            end_offset,
        ),
        retry,
        recovery,
        sequence_floor,
        catalog,
    })
}

struct DecodedRegistry {
    reference: RegistryReference,
    registry: SeriesRegistry,
}

fn encode_registry_snapshot(
    generation: u64,
    snapshot: &SeriesRegistrySnapshot,
) -> Result<Vec<u8>, ManifestStoreError> {
    encode_registry_snapshot_with_limit(generation, snapshot, MAX_REGISTRY_SNAPSHOT_BYTES)
}

fn encode_registry_snapshot_with_limit(
    generation: u64,
    snapshot: &SeriesRegistrySnapshot,
    maximum: usize,
) -> Result<Vec<u8>, ManifestStoreError> {
    validate_snapshot_bounds(snapshot)?;
    if generation == 0 {
        return Err(ManifestStoreError::GenerationExhausted);
    }
    let mut counter = Encoder::counting();
    encode_registry_payload(&mut counter, snapshot)?;
    let payload_len = counter.len();
    let total = REGISTRY_HEADER_LEN
        .checked_add(payload_len)
        .and_then(|value| value.checked_add(REGISTRY_CRC_LEN))
        .ok_or(ManifestStoreError::InvalidRegistry)?;
    if total > maximum || total > MAX_REGISTRY_SNAPSHOT_BYTES {
        return Err(ManifestStoreError::InvalidRegistry);
    }
    let mut payload = Encoder::new();
    encode_registry_payload(&mut payload, snapshot)?;
    let payload = payload.finish();
    if payload.len() != payload_len {
        return Err(ManifestStoreError::InvalidRegistry);
    }
    let mut bytes = vec![0_u8; total];
    bytes[..8].copy_from_slice(&REGISTRY_MAGIC);
    bytes[8..10].copy_from_slice(&REGISTRY_VERSION.to_be_bytes());
    bytes[10..12].copy_from_slice(&REGISTRY_HEADER_LEN_U16.to_be_bytes());
    bytes[12..28].copy_from_slice(snapshot.store_id().as_bytes());
    bytes[28..36].copy_from_slice(&generation.to_be_bytes());
    bytes[36..40].copy_from_slice(
        &u32::try_from(snapshot.limits().max_series())
            .map_err(|_| ManifestStoreError::InvalidRegistry)?
            .to_be_bytes(),
    );
    bytes[40..44].copy_from_slice(
        &u32::try_from(snapshot.limits().max_declaration_revisions())
            .map_err(|_| ManifestStoreError::InvalidRegistry)?
            .to_be_bytes(),
    );
    bytes[44..48].copy_from_slice(
        &u32::try_from(snapshot.series().len())
            .map_err(|_| ManifestStoreError::InvalidRegistry)?
            .to_be_bytes(),
    );
    bytes[48..52].copy_from_slice(
        &u32::try_from(snapshot.declaration_revision_count())
            .map_err(|_| ManifestStoreError::InvalidRegistry)?
            .to_be_bytes(),
    );
    bytes[52..60].copy_from_slice(
        &u64::try_from(payload_len)
            .map_err(|_| ManifestStoreError::InvalidRegistry)?
            .to_be_bytes(),
    );
    bytes[REGISTRY_HEADER_LEN..REGISTRY_HEADER_LEN + payload_len].copy_from_slice(&payload);
    let checksum_offset = total - REGISTRY_CRC_LEN;
    let checksum = crc32c(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    Ok(bytes)
}

fn encode_registry_payload(
    payload: &mut Encoder,
    snapshot: &SeriesRegistrySnapshot,
) -> Result<(), ManifestStoreError> {
    for history in snapshot.series() {
        payload.bytes(history.series_id().as_bytes());
        payload
            .count(history.declarations().len())
            .map_err(|_| ManifestStoreError::InvalidRegistry)?;
        for declaration in history.declarations() {
            encode_declaration(payload, declaration)
                .map_err(|_| ManifestStoreError::InvalidRegistry)?;
        }
        match history.retirement() {
            Some(retirement) => {
                payload.u8(1);
                payload.u128(retirement.declaration_revision().get());
                encode_declaration_evidence(payload, retirement.evidence())
                    .map_err(|_| ManifestStoreError::InvalidRegistry)?;
            }
            None => payload.u8(0),
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn decode_registry_snapshot(bytes: &[u8]) -> Result<DecodedRegistry, ManifestStoreError> {
    if bytes.len() < REGISTRY_HEADER_LEN + REGISTRY_CRC_LEN
        || bytes.len() > MAX_REGISTRY_SNAPSHOT_BYTES
        || bytes[..8] != REGISTRY_MAGIC
        || u16::from_be_bytes(bytes[8..10].try_into().unwrap_or_default()) != REGISTRY_VERSION
        || u16::from_be_bytes(bytes[10..12].try_into().unwrap_or_default())
            != REGISTRY_HEADER_LEN_U16
        || bytes[60..64].iter().any(|byte| *byte != 0)
    {
        return Err(ManifestStoreError::InvalidRegistry);
    }
    let checksum_offset = bytes.len() - REGISTRY_CRC_LEN;
    if crc32c(&bytes[..checksum_offset])
        != u32::from_be_bytes(bytes[checksum_offset..].try_into().unwrap_or_default())
    {
        return Err(ManifestStoreError::InvalidRegistry);
    }
    let store_id = StoreId::from_bytes(bytes[12..28].try_into().unwrap_or_default())
        .map_err(|_| ManifestStoreError::InvalidRegistry)?;
    let generation = u64::from_be_bytes(bytes[28..36].try_into().unwrap_or_default());
    let max_series = usize::try_from(u32::from_be_bytes(
        bytes[36..40].try_into().unwrap_or_default(),
    ))
    .map_err(|_| ManifestStoreError::InvalidRegistry)?;
    let max_revisions = usize::try_from(u32::from_be_bytes(
        bytes[40..44].try_into().unwrap_or_default(),
    ))
    .map_err(|_| ManifestStoreError::InvalidRegistry)?;
    let series_count = usize::try_from(u32::from_be_bytes(
        bytes[44..48].try_into().unwrap_or_default(),
    ))
    .map_err(|_| ManifestStoreError::InvalidRegistry)?;
    let revision_count = usize::try_from(u32::from_be_bytes(
        bytes[48..52].try_into().unwrap_or_default(),
    ))
    .map_err(|_| ManifestStoreError::InvalidRegistry)?;
    let payload_len = usize::try_from(u64::from_be_bytes(
        bytes[52..60].try_into().unwrap_or_default(),
    ))
    .map_err(|_| ManifestStoreError::InvalidRegistry)?;
    if generation == 0
        || max_series > MAX_PERSISTED_REGISTRY_SERIES
        || max_revisions > MAX_PERSISTED_REGISTRY_REVISIONS
        || series_count > max_series
        || revision_count > max_revisions
        || REGISTRY_HEADER_LEN.checked_add(payload_len) != Some(checksum_offset)
    {
        return Err(ManifestStoreError::InvalidRegistry);
    }
    let limits = SeriesRegistryLimits::new(max_series, max_revisions);
    let mut registry = SeriesRegistry::new(store_id, limits);
    let mut cursor = Cursor::new(&bytes[REGISTRY_HEADER_LEN..checksum_offset]);
    let mut decoded_revisions = 0_usize;
    let mut previous_series = None;
    for _ in 0..series_count {
        let series_id = SeriesId::from_bytes(
            cursor
                .take(16)
                .map_err(invalid_registry_journal)?
                .try_into()
                .unwrap_or_default(),
        )
        .map_err(|_| ManifestStoreError::InvalidRegistry)?;
        if previous_series.is_some_and(|previous| previous >= series_id) {
            return Err(ManifestStoreError::InvalidRegistry);
        }
        previous_series = Some(series_id);
        let declaration_count = usize::try_from(cursor.u32().map_err(invalid_registry_journal)?)
            .map_err(|_| ManifestStoreError::InvalidRegistry)?;
        if declaration_count == 0
            || declaration_count > max_revisions.saturating_sub(decoded_revisions)
        {
            return Err(ManifestStoreError::InvalidRegistry);
        }
        for index in 0..declaration_count {
            let declaration = decode_declaration(&mut cursor).map_err(invalid_registry_journal)?;
            if declaration.store_id() != store_id || declaration.series_id() != series_id {
                return Err(ManifestStoreError::InvalidRegistry);
            }
            let actual = if index == 0 {
                registry.register(
                    series_id,
                    declaration.binding().clone(),
                    declaration.payload().clone(),
                    declaration.evidence().clone(),
                )
            } else {
                let expected = declaration
                    .previous_revision()
                    .ok_or(ManifestStoreError::InvalidRegistry)?;
                registry.revise(
                    series_id,
                    expected,
                    declaration.payload().clone(),
                    declaration.evidence().clone(),
                )
            }
            .map_err(|_| ManifestStoreError::InvalidRegistry)?;
            if !declaration_matches(&actual, &declaration) {
                return Err(ManifestStoreError::InvalidRegistry);
            }
            decoded_revisions += 1;
        }
        if match cursor.u8().map_err(invalid_registry_journal)? {
            0 => false,
            1 => true,
            _ => return Err(ManifestStoreError::InvalidRegistry),
        } {
            let revision =
                DeclarationRevision::new(cursor.u128().map_err(invalid_registry_journal)?)
                    .map_err(|_| ManifestStoreError::InvalidRegistry)?;
            let evidence =
                decode_declaration_evidence(&mut cursor).map_err(invalid_registry_journal)?;
            registry
                .retire(series_id, revision, evidence)
                .map_err(|_| ManifestStoreError::InvalidRegistry)?;
        }
    }
    cursor.finish().map_err(invalid_registry_journal)?;
    if decoded_revisions != revision_count {
        return Err(ManifestStoreError::InvalidRegistry);
    }
    let canonical = encode_registry_snapshot(generation, &registry.snapshot())?;
    if canonical != bytes {
        return Err(ManifestStoreError::InvalidRegistry);
    }
    Ok(DecodedRegistry {
        reference: RegistryReference {
            slot: u8::MAX,
            generation,
            length: u64::try_from(bytes.len()).map_err(|_| ManifestStoreError::InvalidRegistry)?,
            checksum: crc32c(bytes),
        },
        registry,
    })
}

fn decode_registry_snapshot_at_slot(
    bytes: &[u8],
    slot: u8,
) -> Result<DecodedRegistry, ManifestStoreError> {
    if slot >= 3 {
        return Err(ManifestStoreError::InvalidRegistry);
    }
    let mut decoded = decode_registry_snapshot(bytes)?;
    decoded.reference.slot = slot;
    Ok(decoded)
}

fn restore_snapshot(
    snapshot: &SeriesRegistrySnapshot,
) -> Result<SeriesRegistry, ManifestStoreError> {
    validate_snapshot_bounds(snapshot)?;
    let mut registry = SeriesRegistry::new(snapshot.store_id(), snapshot.limits());
    for history in snapshot.series() {
        for (index, declaration) in history.declarations().iter().enumerate() {
            let actual = if index == 0 {
                registry.register(
                    history.series_id(),
                    history.binding().clone(),
                    declaration.payload().clone(),
                    declaration.evidence().clone(),
                )
            } else {
                registry.revise(
                    history.series_id(),
                    declaration
                        .previous_revision()
                        .ok_or(ManifestStoreError::InvalidRegistry)?,
                    declaration.payload().clone(),
                    declaration.evidence().clone(),
                )
            }
            .map_err(|_| ManifestStoreError::InvalidRegistry)?;
            if &actual != declaration {
                return Err(ManifestStoreError::InvalidRegistry);
            }
        }
        if let Some(retirement) = history.retirement() {
            let actual = registry
                .retire(
                    history.series_id(),
                    retirement.declaration_revision(),
                    retirement.evidence().clone(),
                )
                .map_err(|_| ManifestStoreError::InvalidRegistry)?;
            if &actual != retirement {
                return Err(ManifestStoreError::InvalidRegistry);
            }
        }
    }
    if registry.snapshot() != *snapshot {
        return Err(ManifestStoreError::InvalidRegistry);
    }
    Ok(registry)
}

fn validate_snapshot_bounds(snapshot: &SeriesRegistrySnapshot) -> Result<(), ManifestStoreError> {
    if snapshot.limits().max_series() > MAX_PERSISTED_REGISTRY_SERIES
        || snapshot.limits().max_declaration_revisions() > MAX_PERSISTED_REGISTRY_REVISIONS
        || snapshot.series().len() > snapshot.limits().max_series()
        || snapshot.declaration_revision_count() > snapshot.limits().max_declaration_revisions()
    {
        return Err(ManifestStoreError::InvalidRegistry);
    }
    Ok(())
}

fn validate_recovered_declarations(
    registry: &SeriesRegistry,
    records: &[crate::RecoveredAdmissionV1],
) -> Result<(), ManifestStoreError> {
    for record in records {
        let decoded = record.admission().declaration();
        let Some(declaration) = registry.resolve(decoded.series_id(), decoded.revision()) else {
            return Err(ManifestStoreError::HistoricalDeclarationMismatch);
        };
        if !declaration_matches(declaration, decoded) {
            return Err(ManifestStoreError::HistoricalDeclarationMismatch);
        }
    }
    Ok(())
}

fn declaration_matches(
    declaration: &SeriesDeclaration,
    decoded: &crate::DecodedDeclarationV1,
) -> bool {
    declaration.store_id() == decoded.store_id()
        && declaration.series_id() == decoded.series_id()
        && declaration.revision() == decoded.revision()
        && declaration.previous_revision() == decoded.previous_revision()
        && declaration.binding() == decoded.binding()
        && declaration.payload() == decoded.payload()
        && declaration.evidence() == decoded.evidence()
}

fn invalid_registry_journal(_: JournalV1Error) -> ManifestStoreError {
    ManifestStoreError::InvalidRegistry
}

fn map_manifest_genesis_preflight(error: ActiveJournalError) -> ManifestStoreError {
    if matches!(
        error,
        ActiveJournalError::Io(_) | ActiveJournalError::StoragePressure(_)
    ) {
        ManifestStoreError::Active(error)
    } else {
        ManifestStoreError::UnsupportedStoreFormat
    }
}

fn manifest_io(operation: ManifestIoOperation, error: &std::io::Error) -> ManifestStoreError {
    classify_manifest_io(operation, error.kind(), error.raw_os_error())
}

const fn classify_manifest_io(
    operation: ManifestIoOperation,
    kind: ErrorKind,
    raw_os_error: Option<i32>,
) -> ManifestStoreError {
    let evidence = ManifestIoEvidence {
        operation,
        kind,
        raw_os_error,
    };
    if operation.is_mutating() && is_storage_pressure(kind) {
        ManifestStoreError::StoragePressure(evidence)
    } else {
        ManifestStoreError::Io(evidence)
    }
}

impl ManifestIoOperation {
    const fn is_mutating(self) -> bool {
        matches!(
            self,
            Self::CreateArtifact
                | Self::Write
                | Self::SyncArtifact
                | Self::Publish
                | Self::Remove
                | Self::SyncDirectory
        )
    }
}

fn genesis_placeholder(store_id: StoreId) -> DurableCutoff {
    DurableCutoff::from_manifest(
        store_id,
        ACTIVE_JOURNAL_GENERATION,
        1,
        0,
        crate::JOURNAL_V1_HEADER_LEN as u64,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PreparedAdmissionV1, test_support};
    use std::fs;
    use std::io::{Seek, SeekFrom};
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

    fn test_directory(code: u8) -> PathBuf {
        let sequence = NEXT_DIRECTORY.fetch_add(1, AtomicOrdering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "och-manifest-fault-{}-{code}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("unique manifest fault directory");
        path
    }

    fn test_config(directory: PathBuf, mode: ActiveJournalOpenMode) -> ManifestStoreConfig {
        ManifestStoreConfig::new(
            directory,
            test_support::store_id(1),
            mode,
            ActiveJournalLimits::new(crate::MAX_ADMISSION_PAYLOAD_V1, 16 * 1_024 * 1_024, 8)
                .expect("fault journal limits"),
            RegistryPersistenceOptions::new(SeriesRegistryLimits::new(2, 4))
                .expect("fault registry limits"),
            RetryPersistenceOptions::new(2, 2).expect("fault retry limits"),
        )
        .expect("fault store config")
    }

    fn register_rotation_fixture(store: &mut ManifestStore) {
        let admission = test_support::no_change_admission();
        let declaration = admission.declaration();
        store
            .register(
                declaration.series_id(),
                declaration.binding().clone(),
                declaration.payload().clone(),
                declaration.evidence().clone(),
            )
            .expect("register rotation fixture");
    }

    fn append_durable(
        store: &mut ManifestStore,
        retry_key: &str,
    ) -> (ManifestCommit, och_core::RetryQualification) {
        let admission = test_support::no_change_admission_with_retry_key(retry_key);
        let qualification = admission.retry().clone();
        let sequence = store.next_append_sequence().expect("fixture sequence");
        let frame = PreparedAdmissionV1::new(admission)
            .expect("fixture admission")
            .into_frame(sequence)
            .expect("fixture frame");
        let end = store.append(&frame).expect("fixture append");
        let (commit, _) = store
            .sync_pending(&[PendingRetryOutcome::new(
                qualification.clone(),
                sequence.get(),
                end,
            )])
            .expect("fixture durability");
        (commit, qualification)
    }

    fn append_raw_suffix(directory: &Path, generation: u64, bytes: &[u8]) {
        let path = directory.join(active_journal_file_name(generation));
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open active suffix fixture");
        file.write_all(bytes).expect("append active suffix fixture");
        file.sync_all().expect("sync active suffix fixture");
    }

    fn directory_bytes(directory: &Path) -> Vec<(String, Vec<u8>)> {
        let mut artifacts = directory
            .read_dir()
            .expect("read fixture inventory")
            .map(|entry| {
                let entry = entry.expect("read fixture entry");
                (
                    entry.file_name().into_string().expect("ASCII fixture name"),
                    fs::read(entry.path()).expect("read fixture artifact"),
                )
            })
            .collect::<Vec<_>>();
        artifacts.sort_by(|left, right| left.0.cmp(&right.0));
        artifacts
    }

    #[test]
    fn store_format_v1_marker_refuses_hostile_magic_version_length_scope_and_checksum() {
        let store_id = test_support::store_id(1);
        let canonical = encode_store_format_marker(store_id);
        assert_eq!(canonical.len(), STORE_FORMAT_LEN);
        decode_store_format_marker(&canonical, store_id).expect("canonical marker");

        let mut candidates = Vec::new();
        let mut wrong_magic = canonical.to_vec();
        wrong_magic[0] ^= 1;
        candidates.push(wrong_magic);
        let mut wrong_version = canonical.to_vec();
        wrong_version[8..10].copy_from_slice(&2_u16.to_be_bytes());
        let checksum = crc32c(&wrong_version[..28]);
        wrong_version[28..].copy_from_slice(&checksum.to_be_bytes());
        candidates.push(wrong_version);
        let mut wrong_length = canonical.to_vec();
        wrong_length[10..12].copy_from_slice(&31_u16.to_be_bytes());
        let checksum = crc32c(&wrong_length[..28]);
        wrong_length[28..].copy_from_slice(&checksum.to_be_bytes());
        candidates.push(wrong_length);
        candidates.push(encode_store_format_marker(test_support::store_id(2)).to_vec());
        let mut wrong_checksum = canonical.to_vec();
        wrong_checksum[31] ^= 1;
        candidates.push(wrong_checksum);
        candidates.push(canonical[..31].to_vec());
        let mut trailing = canonical.to_vec();
        trailing.push(0);
        candidates.push(trailing);

        for candidate in candidates {
            assert_eq!(
                decode_store_format_marker(&candidate, store_id),
                Err(ManifestStoreError::UnsupportedStoreFormat)
            );
        }
    }

    #[test]
    fn historical_declaration_preflight_and_append_share_exact_refusal() {
        let directory = test_directory(23);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create historical-preflight fixture");
        let admission = test_support::no_change_admission();
        let sequence = store.next_append_sequence().expect("fixture sequence");
        let frame = PreparedAdmissionV1::new(admission)
            .expect("fixture admission")
            .into_frame(sequence)
            .expect("fixture frame");
        let before = directory_bytes(&directory);

        assert_eq!(
            store.preflight_historical_declaration(frame.admission().declaration()),
            Err(ManifestStoreError::HistoricalDeclarationMismatch)
        );
        assert_eq!(
            store.append(&frame),
            Err(ManifestStoreError::HistoricalDeclarationMismatch)
        );
        assert_eq!(directory_bytes(&directory), before);

        drop(store);
        fs::remove_dir_all(directory).expect("remove historical-preflight fixture");
    }

    #[test]
    fn current_marker_and_genesis_publication_faults_converge_or_refuse_unchanged() {
        for code in 40_u8..=44 {
            let directory = test_directory(code);
            set_publish_fault(code);
            assert!(
                ManifestStore::open(test_config(
                    directory.clone(),
                    ActiveJournalOpenMode::CreateNew,
                ))
                .is_err()
            );
            let before = directory_bytes(&directory);
            let reopened = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::OpenExisting,
            ));
            if code == 40 {
                assert!(matches!(
                    reopened,
                    Err(ManifestStoreError::UnsupportedStoreFormat)
                ));
                assert_eq!(directory_bytes(&directory), before);
            } else {
                let reopened = reopened.expect("exact marker publication converges");
                assert_eq!(reopened.inspection().committed().manifest_generation(), 1);
                assert_eq!(
                    reopened.inspection().committed().retry_state().generation(),
                    1
                );
            }
            fs::remove_dir_all(directory).expect("remove marker fault fixture");
        }

        for (code, published) in [
            (1_u8, false),
            (2, false),
            (9, false),
            (3, false),
            (4, true),
            (11, false),
            (12, false),
            (13, false),
            (14, false),
            (15, true),
            (5, false),
            (6, false),
            (10, false),
            (7, false),
            (8, true),
        ] {
            let directory = test_directory(code);
            set_publish_fault(code);
            assert!(
                ManifestStore::open(test_config(
                    directory.clone(),
                    ActiveJournalOpenMode::CreateNew,
                ))
                .is_err()
            );
            let before = directory_bytes(&directory);
            let reopened = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::OpenExisting,
            ));
            if published {
                let reopened = reopened.expect("published genesis phase converges");
                assert_eq!(reopened.inspection().committed().manifest_generation(), 1);
            } else {
                assert!(matches!(
                    reopened,
                    Err(ManifestStoreError::InterruptedPublication
                        | ManifestStoreError::InvalidManifest
                        | ManifestStoreError::InvalidRetry
                        | ManifestStoreError::Active(ActiveJournalError::InvalidLayout))
                ));
                assert_eq!(directory_bytes(&directory), before);
            }
            fs::remove_dir_all(directory).expect("remove genesis fault fixture");
        }
    }

    fn rewrite_catalog_entry_registry_generation(
        directory: &Path,
        current: ManifestCommit,
        entry_index: usize,
        registry_generation: u64,
    ) {
        let catalog = current
            .generation_catalog()
            .expect("fixture catalog reference");
        let catalog_name = CATALOG_SLOT_NAMES[usize::from(catalog.slot())];
        let catalog_path = directory.join(catalog_name);
        let mut catalog_bytes = fs::read(&catalog_path).expect("read hostile catalog");
        let entry_offset = crate::generation::CATALOG_HEADER_LEN
            .checked_add(
                entry_index
                    .checked_mul(crate::generation::CATALOG_ENTRY_LEN)
                    .expect("entry offset"),
            )
            .expect("catalog entry offset");
        catalog_bytes[entry_offset + 32..entry_offset + 40]
            .copy_from_slice(&registry_generation.to_be_bytes());
        let catalog_checksum_offset = catalog_bytes.len() - 4;
        let catalog_checksum = crc32c(&catalog_bytes[..catalog_checksum_offset]);
        catalog_bytes[catalog_checksum_offset..].copy_from_slice(&catalog_checksum.to_be_bytes());
        fs::write(&catalog_path, &catalog_bytes).expect("write hostile catalog");

        let (manifest_name, mut manifest_bytes) = MANIFEST_SLOT_NAMES
            .iter()
            .find_map(|name| {
                let bytes = fs::read(directory.join(name)).ok()?;
                (u64::from_be_bytes(bytes.get(28..36)?.try_into().ok()?)
                    == current.manifest_generation())
                .then_some((*name, bytes))
            })
            .expect("find current Manifest V1");
        assert_eq!(manifest_bytes.len(), MANIFEST_LEN);
        manifest_bytes[152..156].copy_from_slice(&catalog_checksum.to_be_bytes());
        let manifest_checksum = crc32c(&manifest_bytes[..156]);
        manifest_bytes[156..160].copy_from_slice(&manifest_checksum.to_be_bytes());
        fs::write(directory.join(manifest_name), manifest_bytes)
            .expect("write independently repaired hostile manifest");
    }

    #[test]
    fn pending_retry_preflight_enforces_exact_delta_and_hard_count_before_entry_walk() {
        let store_id = test_support::store_id(1);
        let prior = DurableCutoff::from_manifest(store_id, 1, 7, 10, 1_000);
        let exact = DurableCutoff::from_manifest(
            store_id,
            1,
            8,
            10 + MAX_PERSISTED_RETRY_ENTRIES as u64,
            2_000,
        );
        assert_eq!(
            validate_pending_retry_preflight(prior, exact, MAX_PERSISTED_RETRY_ENTRIES),
            Ok(())
        );
        let one_over = DurableCutoff::from_manifest(
            store_id,
            1,
            8,
            11 + MAX_PERSISTED_RETRY_ENTRIES as u64,
            2_000,
        );
        assert_eq!(
            validate_pending_retry_preflight(prior, one_over, MAX_PERSISTED_RETRY_ENTRIES + 1,),
            Err(ManifestStoreError::InvalidRetry)
        );
        assert_eq!(
            validate_pending_retry_preflight(prior, exact, MAX_PERSISTED_RETRY_ENTRIES - 1),
            Err(ManifestStoreError::InvalidRetry)
        );
        assert_eq!(
            validate_pending_retry_preflight(prior, exact, 0),
            Err(ManifestStoreError::InvalidRetry)
        );
    }

    #[test]
    fn first_rotation_commits_current_v1_and_reopens_empty_successor_without_rewriting_retry() {
        let directory = test_directory(17);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create rotation fixture");
        let admission = test_support::no_change_admission();
        let declaration = admission.declaration();
        store
            .register(
                declaration.series_id(),
                declaration.binding().clone(),
                declaration.payload().clone(),
                declaration.evidence().clone(),
            )
            .expect("register rotation declaration");
        let sequence = store.next_append_sequence().expect("rotation sequence");
        let frame = PreparedAdmissionV1::new(admission)
            .expect("rotation admission")
            .into_frame(sequence)
            .expect("rotation frame");
        let end = store.append(&frame).expect("rotation append");
        let (original, retry) = store
            .sync_pending(&[PendingRetryOutcome::new(
                frame.admission().retry().clone(),
                sequence.get(),
                end,
            )])
            .expect("rotation durability");
        let rotated = store.rotate().expect("commit first rotation");
        assert_eq!(rotated.retry_state(), original.retry_state());
        assert_eq!(rotated.sequence_floor(), sequence.get());
        assert!(rotated.generation_catalog().is_some());
        assert_eq!(store.retry_state_snapshot(), retry);
        let inspection = store.inspection();
        assert_eq!(inspection.active().journal().generation(), 2);
        assert_eq!(inspection.active().active_records(), 0);
        assert_eq!(inspection.generations().sealed_count(), 1);
        drop(store);

        let reopened = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("reopen first rotation");
        assert_eq!(reopened.inspection().committed(), rotated);
        assert_eq!(reopened.retry_state_snapshot(), retry);
        assert_eq!(
            reopened
                .next_append_sequence()
                .expect("successor sequence")
                .get(),
            2
        );
        fs::remove_dir_all(directory).expect("remove rotation fixture");
    }

    #[test]
    fn two_rotations_then_successor_append_cleans_catalog_and_reopens_exact_replay() {
        let directory = test_directory(44);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create repeated-rotation fixture");
        register_rotation_fixture(&mut store);
        append_durable(&mut store, "rotation-one");
        store.rotate().expect("first rotation");
        append_durable(&mut store, "rotation-two");
        store.rotate().expect("second rotation");
        assert_eq!(store.inspection().generations().sealed_count(), 2);
        let (third_commit, third_qualification) = append_durable(&mut store, "successor-three");
        let expected_retry = store.retry_state_snapshot();
        assert_eq!(
            CATALOG_SLOT_NAMES
                .iter()
                .filter(|name| directory.join(name).exists())
                .count(),
            1,
            "ordinary adoption removes the newly unreferenced catalog prefix"
        );
        drop(store);

        let reopened = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("reopen after successor append");
        assert_eq!(reopened.inspection().committed(), third_commit);
        assert_eq!(reopened.retry_state_snapshot(), expected_retry);
        let crate::RetryStateMatch::Replay(outcome) = reopened
            .retry_state_snapshot()
            .classify(&third_qualification)
        else {
            panic!("successor retry remains replayable");
        };
        assert_eq!(outcome.manifest_commit(), third_commit);
        assert_eq!(outcome.append_sequence(), 3);
        drop(reopened);
        fs::remove_dir_all(directory).expect("remove repeated-rotation fixture");
    }

    #[test]
    fn crash_after_ordinary_manifest_adoption_accepts_only_exact_catalog_prefix() {
        let directory = test_directory(45);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create catalog-prefix fixture");
        register_rotation_fixture(&mut store);
        append_durable(&mut store, "prefix-one");
        store.rotate().expect("first prefix rotation");
        append_durable(&mut store, "prefix-two");
        store.rotate().expect("second prefix rotation");
        assert_eq!(
            CATALOG_SLOT_NAMES
                .iter()
                .filter(|name| directory.join(name).exists())
                .count(),
            2
        );

        let second = test_support::observed_admission(
            vec![och_core::ExactValue::Boolean(true)],
            och_core::ValueFamily::Boolean,
            0,
            false,
        );
        let declaration = second.declaration();
        set_publish_fault(8);
        assert!(
            store
                .register(
                    declaration.series_id(),
                    declaration.binding().clone(),
                    declaration.payload().clone(),
                    declaration.evidence().clone(),
                )
                .is_err(),
            "post-rename directory-sync fault reports no registry commit"
        );
        drop(store);
        assert_eq!(
            CATALOG_SLOT_NAMES
                .iter()
                .filter(|name| directory.join(name).exists())
                .count(),
            2,
            "crash window retains the exact older catalog prefix"
        );

        let reopened = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("exact redundant prefix converges to the newer root");
        assert_eq!(reopened.registry_snapshot().series().len(), 2);
        drop(reopened);
        assert_eq!(
            CATALOG_SLOT_NAMES
                .iter()
                .filter(|name| directory.join(name).exists())
                .count(),
            1,
            "verified redundant prefix is removed idempotently"
        );
        fs::remove_dir_all(directory).expect("remove catalog-prefix fixture");
    }

    #[test]
    fn first_rotation_catalog_entry_must_describe_retained_source_manifest() {
        let directory = test_directory(50);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create first-transition hostile fixture");
        register_rotation_fixture(&mut store);
        let (source, _) = append_durable(&mut store, "first-transition-source");
        let current = store.rotate().expect("first transition rotation");
        drop(store);

        rewrite_catalog_entry_registry_generation(
            &directory,
            current,
            0,
            source
                .registry_generation()
                .checked_sub(1)
                .expect("positive mismatched registry generation"),
        );
        let before = directory_bytes(&directory);
        let Err(error) = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        )) else {
            panic!("locally canonical first catalog mismatch must refuse");
        };
        assert_eq!(error, ManifestStoreError::InvalidGeneration);
        assert_eq!(directory_bytes(&directory), before);
        fs::remove_dir_all(directory).expect("remove first-transition hostile fixture");
    }

    #[test]
    fn later_rotation_catalog_append_must_describe_retained_source_manifest() {
        let directory = test_directory(51);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create later-transition hostile fixture");
        register_rotation_fixture(&mut store);
        append_durable(&mut store, "later-transition-one");
        store.rotate().expect("first later-transition rotation");
        let (source, _) = append_durable(&mut store, "later-transition-two");
        let current = store.rotate().expect("second later-transition rotation");
        drop(store);

        rewrite_catalog_entry_registry_generation(
            &directory,
            current,
            1,
            source
                .registry_generation()
                .checked_sub(1)
                .expect("positive mismatched registry generation"),
        );
        let before = directory_bytes(&directory);
        let Err(error) = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        )) else {
            panic!("locally canonical later catalog append mismatch must refuse");
        };
        assert_eq!(error, ManifestStoreError::InvalidGeneration);
        assert_eq!(directory_bytes(&directory), before);
        fs::remove_dir_all(directory).expect("remove later-transition hostile fixture");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn catalog_binding_and_consecutive_prefix_are_exact_after_canonical_decode() {
        let store_id = test_support::store_id(1);
        let finalize = |snapshot: GenerationCatalogSnapshot| {
            let provisional = encode_catalog(&snapshot).expect("provisional catalog");
            let reference = snapshot.reference().expect("provisional reference");
            snapshot
                .with_reference(GenerationCatalogReference::new(
                    reference.slot(),
                    reference.generation(),
                    u64::try_from(provisional.len()).expect("catalog length"),
                    crc32c(&provisional),
                ))
                .expect("final catalog")
        };
        let first = finalize(
            GenerationCatalogSnapshot::empty(store_id)
                .advance(
                    GenerationCatalogReference::new(0, 1, 1, 1),
                    SealedGeneration::new(1, 0, 1, 100, 2, 100, 11),
                )
                .expect("first catalog"),
        );
        let first = decode_catalog(&encode_catalog(&first).expect("first bytes"), 0, store_id)
            .expect("canonical first decode");
        let valid = ManifestRecord {
            generation: 4,
            registry: RegistryReference {
                slot: 0,
                generation: 2,
                length: 68,
                checksum: 9,
            },
            cutoff: DurableCutoff::from_manifest(
                store_id,
                2,
                1,
                1,
                crate::JOURNAL_V1_HEADER_LEN as u64,
            ),
            retry: RetryArtifactReference {
                public: RetryStateReference::new(0, 1),
                length: 68,
                checksum: 7,
            },
            recovery: None,
            sequence_floor: 1,
            catalog: first.reference(),
        };
        let valid = decode_manifest(&encode_manifest(valid), store_id).expect("canonical root");
        assert_eq!(validate_manifest_catalog_binding(valid, &first), Ok(()));

        for hostile in [
            ManifestRecord {
                cutoff: DurableCutoff::from_manifest(
                    store_id,
                    3,
                    1,
                    1,
                    crate::JOURNAL_V1_HEADER_LEN as u64,
                ),
                ..valid
            },
            ManifestRecord {
                cutoff: DurableCutoff::from_manifest(
                    store_id,
                    2,
                    1,
                    2,
                    crate::JOURNAL_V1_HEADER_LEN as u64,
                ),
                sequence_floor: 2,
                ..valid
            },
            ManifestRecord {
                registry: RegistryReference {
                    generation: 1,
                    ..valid.registry
                },
                ..valid
            },
        ] {
            let decoded = decode_manifest(&encode_manifest(hostile), store_id)
                .expect("hostile relation remains locally canonical");
            assert_eq!(
                validate_manifest_catalog_binding(decoded, &first),
                Err(ManifestStoreError::InvalidGeneration)
            );
        }

        let second = finalize(
            first
                .advance(
                    GenerationCatalogReference::new(1, 2, 1, 1),
                    SealedGeneration::new(2, 1, 2, 101, 2, 101, 12),
                )
                .expect("second catalog"),
        );
        assert!(catalog_appends_exactly_one(&first, &second));
        let forked_first = finalize(
            GenerationCatalogSnapshot::empty(store_id)
                .advance(
                    GenerationCatalogReference::new(2, 1, 1, 1),
                    SealedGeneration::new(1, 0, 1, 100, 2, 100, 99),
                )
                .expect("forked first catalog"),
        );
        let forked_second = finalize(
            forked_first
                .advance(
                    GenerationCatalogReference::new(0, 2, 1, 1),
                    SealedGeneration::new(2, 1, 2, 101, 2, 101, 12),
                )
                .expect("forked second catalog"),
        );
        let forked_second = decode_catalog(
            &encode_catalog(&forked_second).expect("forked canonical bytes"),
            0,
            store_id,
        )
        .expect("forked catalog remains locally canonical");
        assert!(!catalog_appends_exactly_one(&first, &forked_second));
        assert!(!catalog_is_strict_prefix(&first, &forked_second));
    }

    #[test]
    fn mismatched_committed_rotation_intent_refuses_without_cleanup() {
        let directory = test_directory(46);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create mismatched-intent fixture");
        register_rotation_fixture(&mut store);
        append_durable(&mut store, "intent-root");
        set_publish_fault(39);
        assert!(store.rotate().is_err());
        drop(store);

        let intent_path = directory.join(ROTATION_INTENT_FILE_NAME);
        let mut hostile = fs::read(&intent_path).expect("read committed intent");
        let registry_generation =
            u64::from_be_bytes(hostile[60..68].try_into().expect("registry bytes"));
        hostile[60..68].copy_from_slice(
            &registry_generation
                .checked_add(1)
                .expect("bounded registry generation")
                .to_be_bytes(),
        );
        let checksum = crc32c(&hostile[..92]);
        hostile[92..96].copy_from_slice(&checksum.to_be_bytes());
        fs::write(&intent_path, &hostile).expect("publish hostile committed intent");
        let source_journal = directory.join(active_journal_file_name(1));
        let source_checkpoint = directory.join(active_checkpoint_file_name(1));
        assert!(source_journal.exists());
        assert!(source_checkpoint.exists());

        let Err(error) = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        )) else {
            panic!("mismatched committed intent must refuse");
        };
        assert_eq!(error, ManifestStoreError::InvalidGeneration);
        assert_eq!(fs::read(&intent_path).expect("intent retained"), hostile);
        assert!(source_journal.exists());
        assert!(source_checkpoint.exists());
        assert!(directory.join(sealed_journal_file_name(1)).exists());
        fs::remove_dir_all(directory).expect("remove mismatched-intent fixture");
    }

    #[test]
    fn extra_recognized_active_checkpoint_and_sealed_names_refuse_unchanged() {
        for (code, name) in [
            (47_u8, active_journal_file_name(2)),
            (48, active_checkpoint_file_name(2)),
            (49, sealed_journal_file_name(2)),
        ] {
            let directory = test_directory(code);
            let store = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::CreateNew,
            ))
            .expect("create exact-inventory fixture");
            drop(store);
            let extra = directory.join(name);
            fs::write(&extra, [code]).expect("write recognized extra artifact");
            let Err(error) = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::OpenExisting,
            )) else {
                panic!("extra recognized generation artifact must refuse");
            };
            assert!(matches!(
                error,
                ManifestStoreError::InvalidInventory
                    | ManifestStoreError::InvalidGeneration
                    | ManifestStoreError::UnsupportedStoreFormat
            ));
            assert_eq!(fs::read(&extra).expect("extra artifact retained"), [code]);
            fs::remove_dir_all(directory).expect("remove exact-inventory fixture");
        }
    }

    #[test]
    fn normal_open_reads_sealed_metadata_and_header_without_scanning_payload() {
        let directory = test_directory(41);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create bounded-open fixture");
        let admission = test_support::no_change_admission();
        let declaration = admission.declaration();
        store
            .register(
                declaration.series_id(),
                declaration.binding().clone(),
                declaration.payload().clone(),
                declaration.evidence().clone(),
            )
            .expect("register bounded-open declaration");
        let sequence = store.next_append_sequence().expect("bounded-open sequence");
        let frame = PreparedAdmissionV1::new(admission)
            .expect("bounded-open admission")
            .into_frame(sequence)
            .expect("bounded-open frame");
        let end = store.append(&frame).expect("bounded-open append");
        store
            .sync_pending(&[PendingRetryOutcome::new(
                frame.admission().retry().clone(),
                sequence.get(),
                end,
            )])
            .expect("bounded-open durability");
        let committed = store.rotate().expect("bounded-open rotation");
        drop(store);

        let sealed = directory.join(sealed_journal_file_name(1));
        let mut file = OpenOptions::new()
            .write(true)
            .open(&sealed)
            .expect("open hostile sealed payload");
        file.seek(SeekFrom::Start(crate::JOURNAL_V1_HEADER_LEN as u64 + 4))
            .expect("seek into sealed payload");
        file.write_all(&[0xff]).expect("mutate only sealed payload");
        file.sync_all().expect("sync hostile external mutation");
        drop(file);

        let reopened = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("normal open must not scan sealed payload bytes");
        assert_eq!(reopened.inspection().committed(), committed);
        assert_eq!(reopened.inspection().generations().sealed_count(), 1);
        drop(reopened);
        fs::remove_dir_all(directory).expect("remove bounded-open fixture");
    }

    #[test]
    fn oversized_frame_refuses_on_empty_active_without_rotation_loop_or_artifacts() {
        let directory = test_directory(42);
        let limits = ActiveJournalLimits::new(
            crate::MAX_ADMISSION_PAYLOAD_V1,
            crate::JOURNAL_V1_HEADER_LEN as u64 + 10,
            1,
        )
        .expect("small active limit");
        let config = ManifestStoreConfig::new(
            directory.clone(),
            test_support::store_id(1),
            ActiveJournalOpenMode::CreateNew,
            limits,
            RegistryPersistenceOptions::new(SeriesRegistryLimits::new(2, 4))
                .expect("registry limits"),
            RetryPersistenceOptions::new(2, 2).expect("retry limits"),
        )
        .expect("small store config");
        let store = ManifestStore::open(config).expect("create empty small store");
        assert_eq!(
            store.requires_rotation(11),
            Err(ManifestStoreError::Active(
                ActiveJournalError::FrameTooLarge
            ))
        );
        assert!(!directory.join(ROTATION_INTENT_FILE_NAME).exists());
        drop(store);
        fs::remove_dir_all(directory).expect("remove small store fixture");
    }

    #[test]
    fn full_catalog_refusal_is_typed_and_leaves_every_artifact_unchanged() {
        let directory = test_directory(43);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create catalog-full fixture");
        let admission = test_support::no_change_admission();
        let declaration = admission.declaration();
        store
            .register(
                declaration.series_id(),
                declaration.binding().clone(),
                declaration.payload().clone(),
                declaration.evidence().clone(),
            )
            .expect("register catalog-full declaration");
        let sequence = store.next_append_sequence().expect("catalog-full sequence");
        let frame = PreparedAdmissionV1::new(admission)
            .expect("catalog-full admission")
            .into_frame(sequence)
            .expect("catalog-full frame");
        let end = store.append(&frame).expect("catalog-full append");
        store
            .sync_pending(&[PendingRetryOutcome::new(
                frame.admission().retry().clone(),
                sequence.get(),
                end,
            )])
            .expect("catalog-full durability");

        let mut catalog = GenerationCatalogSnapshot::empty(test_support::store_id(1));
        for generation in 1_u64..=MAX_SEALED_GENERATIONS as u64 {
            let slot = u8::try_from((generation - 1) % 3).expect("catalog slot");
            let provisional = catalog
                .advance(
                    GenerationCatalogReference::new(slot, generation, 1, 1),
                    SealedGeneration::new(
                        generation,
                        generation - 1,
                        generation,
                        29,
                        1,
                        29,
                        u32::try_from(generation).expect("bounded catalog generation"),
                    ),
                )
                .expect("fill bounded catalog");
            let bytes = encode_catalog(&provisional).expect("provisional catalog");
            catalog = provisional
                .with_reference(GenerationCatalogReference::new(
                    slot,
                    generation,
                    bytes.len() as u64,
                    crc32c(&bytes),
                ))
                .expect("final catalog reference");
        }
        store.catalog = catalog;
        let before = fs::read_dir(&directory)
            .expect("read catalog-full inventory")
            .map(|entry| {
                let path = entry.expect("inventory entry").path();
                (
                    path.file_name().expect("name").to_owned(),
                    fs::read(path).expect("bytes"),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            store.rotate(),
            Err(ManifestStoreError::GenerationCatalogFull)
        );
        let after = fs::read_dir(&directory)
            .expect("reread catalog-full inventory")
            .map(|entry| {
                let path = entry.expect("inventory entry").path();
                (
                    path.file_name().expect("name").to_owned(),
                    fs::read(path).expect("bytes"),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(after, before);
        assert!(!directory.join(ROTATION_INTENT_FILE_NAME).exists());
        drop(store);
        fs::remove_dir_all(directory).expect("remove catalog-full fixture");
    }

    #[test]
    fn retry_transition_refusal_is_preflighted_before_sync_without_poisoning() {
        let directory = test_directory(16);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create transition-refusal fixture");
        let admission = test_support::no_change_admission();
        let declaration = admission.declaration();
        store
            .register(
                declaration.series_id(),
                declaration.binding().clone(),
                declaration.payload().clone(),
                declaration.evidence().clone(),
            )
            .expect("register transition-refusal declaration");

        let first_sequence = store.next_append_sequence().expect("first sequence");
        let first = PreparedAdmissionV1::new(admission.clone())
            .expect("first prepared admission")
            .into_frame(first_sequence)
            .expect("first frame");
        let first_end = store.append(&first).expect("first append");
        store
            .sync_pending(&[PendingRetryOutcome::new(
                first.admission().retry().clone(),
                first_sequence.get(),
                first_end,
            )])
            .expect("first durable retry transition");

        let second_sequence = store.next_append_sequence().expect("second sequence");
        let second = PreparedAdmissionV1::new(admission)
            .expect("second prepared admission")
            .into_frame(second_sequence)
            .expect("second frame");
        let second_end = store.append(&second).expect("second append");
        assert_eq!(
            store.sync_pending(&[PendingRetryOutcome::new(
                second.admission().retry().clone(),
                second_sequence.get(),
                second_end,
            )]),
            Err(ManifestStoreError::InvalidRetry),
            "a retained key cannot be advanced as fresh before journal sync"
        );
        assert!(store.bind(second.admission().envelope().clone()).is_ok());
        assert_eq!(store.inspection().write_state(), StoreWriteState::Writable);
        assert_eq!(
            store
                .inspection()
                .active()
                .durable_cutoff()
                .append_sequence(),
            first_sequence.get()
        );
        drop(store);
        fs::remove_dir_all(directory).expect("remove transition-refusal fixture");
    }

    #[test]
    fn every_registry_and_manifest_publication_boundary_fails_stop_without_false_success() {
        let cases = [
            (1, ManifestIoOperation::Write, false, false),
            (2, ManifestIoOperation::SyncArtifact, false, false),
            (9, ManifestIoOperation::Read, false, false),
            (3, ManifestIoOperation::Publish, false, false),
            (4, ManifestIoOperation::SyncDirectory, true, false),
            (5, ManifestIoOperation::Write, false, true),
            (6, ManifestIoOperation::SyncArtifact, false, true),
            (10, ManifestIoOperation::Read, false, true),
            (7, ManifestIoOperation::Publish, false, true),
            (8, ManifestIoOperation::SyncDirectory, true, true),
        ];
        for (code, operation, published, manifest_family) in cases {
            let directory = test_directory(code);
            let mut store = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::CreateNew,
            ))
            .expect("create fault fixture");
            let admission = test_support::no_change_admission();
            let declaration = admission.declaration();
            set_publish_fault(code);
            let error = store
                .register(
                    declaration.series_id(),
                    declaration.binding().clone(),
                    declaration.payload().clone(),
                    declaration.evidence().clone(),
                )
                .expect_err("injected publication boundary must refuse");
            assert!(matches!(
                error,
                ManifestStoreError::Io(evidence) if evidence.operation() == operation
            ));
            assert_eq!(
                store.bind(admission.envelope().clone()),
                Err(ManifestStoreError::Faulted)
            );
            drop(store);

            let reopened = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::OpenExisting,
            ));
            if published {
                let reopened = reopened.expect("published final slot has deterministic reopen");
                assert_eq!(
                    reopened.registry_snapshot().series().len(),
                    usize::from(manifest_family),
                    "only a renamed manifest can commit the new registry"
                );
            } else {
                assert!(matches!(
                    reopened,
                    Err(ManifestStoreError::InterruptedPublication
                        | ManifestStoreError::InvalidManifest
                        | ManifestStoreError::InvalidRetry
                        | ManifestStoreError::Active(ActiveJournalError::InvalidLayout))
                ));
            }
            fs::remove_dir_all(directory).expect("remove manifest fault directory");
        }
    }

    #[test]
    fn every_retry_and_following_manifest_publication_boundary_has_no_false_commit() {
        let cases = [
            (11, ManifestIoOperation::Write, false),
            (12, ManifestIoOperation::SyncArtifact, false),
            (13, ManifestIoOperation::Read, false),
            (14, ManifestIoOperation::Publish, false),
            (15, ManifestIoOperation::SyncDirectory, false),
            (5, ManifestIoOperation::Write, false),
            (6, ManifestIoOperation::SyncArtifact, false),
            (10, ManifestIoOperation::Read, false),
            (7, ManifestIoOperation::Publish, false),
            (8, ManifestIoOperation::SyncDirectory, true),
        ];
        for (code, operation, manifest_published) in cases {
            let directory = test_directory(code);
            let mut store = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::CreateNew,
            ))
            .expect("create retry fault fixture");
            let admission = test_support::no_change_admission();
            let declaration = admission.declaration();
            store
                .register(
                    declaration.series_id(),
                    declaration.binding().clone(),
                    declaration.payload().clone(),
                    declaration.evidence().clone(),
                )
                .expect("register retry fault declaration");
            let sequence = store.next_append_sequence().expect("fault append sequence");
            let frame = PreparedAdmissionV1::new(admission)
                .expect("fault prepared admission")
                .into_frame(sequence)
                .expect("fault prepared frame");
            let end_offset = store.append(&frame).expect("fault append");
            let pending = [PendingRetryOutcome::new(
                frame.admission().retry().clone(),
                sequence.get(),
                end_offset,
            )];
            set_publish_fault(code);
            let error = store
                .sync_pending(&pending)
                .expect_err("injected retry publication boundary must refuse");
            assert!(matches!(
                error,
                ManifestStoreError::Io(evidence) if evidence.operation() == operation
            ));
            assert_eq!(
                store.bind(frame.admission().envelope().clone()),
                Err(ManifestStoreError::Faulted)
            );
            drop(store);

            let reopened = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::OpenExisting,
            ));
            if manifest_published {
                let reopened = reopened.expect("renamed manifest commits retry projection");
                assert_eq!(reopened.retry_state_snapshot().replay().len(), 1);
            } else {
                assert!(matches!(
                    reopened,
                    Err(ManifestStoreError::InterruptedPublication
                        | ManifestStoreError::InvalidManifest
                        | ManifestStoreError::InvalidRetry
                        | ManifestStoreError::Active(ActiveJournalError::InvalidLayout))
                ));
            }
            fs::remove_dir_all(directory).expect("remove retry fault directory");
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn rotation_fault_matrix_preserves_exact_prior_or_committed_current_v1_root() {
        let precommit = [
            20_u8, 21, 22, 23, 24, 25, 26, 27, 28, 29, 35, 36, 30, 31, 32, 33, 34, 5, 6, 10, 7,
        ];
        for code in precommit {
            let directory = test_directory(code);
            let mut store = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::CreateNew,
            ))
            .expect("create precommit rotation fault fixture");
            let admission = test_support::no_change_admission();
            let declaration = admission.declaration();
            store
                .register(
                    declaration.series_id(),
                    declaration.binding().clone(),
                    declaration.payload().clone(),
                    declaration.evidence().clone(),
                )
                .expect("register precommit rotation declaration");
            let sequence = store.next_append_sequence().expect("rotation sequence");
            let frame = PreparedAdmissionV1::new(admission.clone())
                .expect("rotation admission")
                .into_frame(sequence)
                .expect("rotation frame");
            let end = store.append(&frame).expect("rotation append");
            let (prior, _) = store
                .sync_pending(&[PendingRetryOutcome::new(
                    frame.admission().retry().clone(),
                    sequence.get(),
                    end,
                )])
                .expect("rotation prerequisite durability");
            let prior_slots = MANIFEST_SLOT_NAMES.map(|name| fs::read(directory.join(name)).ok());
            set_publish_fault(code);
            assert!(
                store.rotate().is_err(),
                "fault {code} must not report rotation"
            );
            assert_eq!(
                store.bind(admission.envelope().clone()),
                Err(ManifestStoreError::Faulted)
            );
            drop(store);
            assert_eq!(
                MANIFEST_SLOT_NAMES.map(|name| fs::read(directory.join(name)).ok()),
                prior_slots,
                "precommit fault {code} must not replace either authority slot"
            );
            match ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::OpenExisting,
            )) {
                Ok(reopened) => assert_eq!(reopened.inspection().committed(), prior),
                Err(error) => assert!(
                    matches!(
                        error,
                        ManifestStoreError::InvalidGeneration
                            | ManifestStoreError::InterruptedPublication
                            | ManifestStoreError::InvalidInventory
                    ),
                    "ambiguous precommit evidence must refuse unchanged: {error:?}"
                ),
            }
            fs::remove_dir_all(directory).expect("remove precommit rotation fault directory");
        }

        for code in [8_u8, 38, 39] {
            let directory = test_directory(code);
            let mut store = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::CreateNew,
            ))
            .expect("create postcommit rotation fault fixture");
            let admission = test_support::no_change_admission();
            let declaration = admission.declaration();
            store
                .register(
                    declaration.series_id(),
                    declaration.binding().clone(),
                    declaration.payload().clone(),
                    declaration.evidence().clone(),
                )
                .expect("register postcommit rotation declaration");
            let sequence = store.next_append_sequence().expect("rotation sequence");
            let frame = PreparedAdmissionV1::new(admission.clone())
                .expect("rotation admission")
                .into_frame(sequence)
                .expect("rotation frame");
            let end = store.append(&frame).expect("rotation append");
            store
                .sync_pending(&[PendingRetryOutcome::new(
                    frame.admission().retry().clone(),
                    sequence.get(),
                    end,
                )])
                .expect("rotation prerequisite durability");
            set_publish_fault(code);
            assert!(
                store.rotate().is_err(),
                "fault {code} must not report rotation"
            );
            drop(store);
            let reopened = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::OpenExisting,
            ))
            .expect("postcommit evidence converges only to current Manifest V1");
            let inspection = reopened.inspection();
            assert_eq!(inspection.generations().active_generation(), 2);
            assert_eq!(inspection.generations().sealed_count(), 1);
            assert!(inspection.committed().generation_catalog().is_some());
            drop(reopened);
            assert!(!directory.join(ROTATION_INTENT_FILE_NAME).exists());
            fs::remove_dir_all(directory).expect("remove postcommit rotation fault directory");
        }
    }

    #[test]
    fn manifest_root_requires_existing_checkpoint_before_registry_validation() {
        let directory = test_directory(67);
        let store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create committed manifest-root fixture");
        let committed = store.inspection().committed();
        assert_eq!(
            fs::metadata(directory.join(ACTIVE_JOURNAL_FILE_NAME))
                .expect("header-only journal metadata")
                .len(),
            crate::JOURNAL_V1_HEADER_LEN as u64
        );
        drop(store);

        let checkpoint_path = directory.join(ACTIVE_CHECKPOINT_FILE_NAME);
        let canonical_checkpoint =
            fs::read(&checkpoint_path).expect("read committed checkpoint authority");
        assert_eq!(canonical_checkpoint.len(), 128);
        let zero_checkpoint = vec![0_u8; canonical_checkpoint.len()];
        fs::write(&checkpoint_path, &zero_checkpoint).expect("install absent checkpoint slots");

        let registry_path =
            directory.join(REGISTRY_SLOT_NAMES[usize::from(committed.registry_slot())]);
        let mut invalid_registry = fs::read(&registry_path).expect("read committed registry");
        let registry_checksum_byte = invalid_registry
            .last_mut()
            .expect("registry snapshot includes checksum");
        *registry_checksum_byte ^= 1;
        fs::write(&registry_path, invalid_registry).expect("install invalid registry authority");

        let before_checkpoint_refusal = directory_bytes(&directory);
        let Err(error) = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        )) else {
            panic!("manifest-root open must require checkpoint authority");
        };
        assert_eq!(
            error,
            ManifestStoreError::Active(ActiveJournalError::InvalidLayout)
        );
        assert_eq!(
            fs::read(&checkpoint_path).expect("read refused zero checkpoint"),
            zero_checkpoint
        );
        assert_eq!(directory_bytes(&directory), before_checkpoint_refusal);

        fs::write(&checkpoint_path, canonical_checkpoint)
            .expect("restore checkpoint to prove later registry refusal");
        let before_registry_refusal = directory_bytes(&directory);
        let Err(error) = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        )) else {
            panic!("invalid registry authority must refuse");
        };
        assert_eq!(error, ManifestStoreError::InvalidRegistry);
        assert_eq!(directory_bytes(&directory), before_registry_refusal);
        fs::remove_dir_all(directory).expect("remove manifest-root checkpoint fixture");
    }

    #[test]
    fn terminal_suffix_recovery_is_manifest_bound_durable_and_idempotent() {
        let directory = test_directory(60);
        let store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create recovery fixture");
        let source = store.inspection().committed();
        let checkpoint =
            fs::read(directory.join(ACTIVE_CHECKPOINT_FILE_NAME)).expect("read source checkpoint");
        drop(store);
        append_raw_suffix(&directory, 1, b"OCHF\0\x01\x01\0\0\0\0");
        let original_length = fs::metadata(directory.join(ACTIVE_JOURNAL_FILE_NAME))
            .expect("suffix metadata")
            .len();

        let recovered = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("recover terminal short prefix");
        let inspection = recovered.inspection();
        let report = inspection
            .latest_recovery()
            .expect("committed recovery report");
        assert_eq!(
            inspection.committed().manifest_generation(),
            source.manifest_generation() + 1
        );
        assert_eq!(
            inspection.committed().durable_cutoff(),
            source.durable_cutoff()
        );
        assert_eq!(report.report_generation(), 1);
        assert_eq!(
            report.source_manifest_generation(),
            source.manifest_generation()
        );
        assert_eq!(
            report.committing_manifest_generation(),
            inspection.committed().manifest_generation()
        );
        assert_eq!(report.active_generation(), 1);
        assert_eq!(report.active_sequence_floor(), 0);
        assert_eq!(report.checkpoint_generation(), 1);
        assert_eq!(report.append_sequence(), 0);
        assert_eq!(
            report.committed_end_offset(),
            crate::JOURNAL_V1_HEADER_LEN as u64
        );
        assert_eq!(report.original_journal_length(), original_length);
        assert_eq!(report.removed_bytes(), 11);
        assert_eq!(
            report.classification(),
            crate::RecoveryClassification::ShortFramePrefix
        );
        assert_eq!(
            report.action(),
            crate::RecoveryAction::TruncateToCommittedRoot
        );
        assert_eq!(
            fs::metadata(directory.join(ACTIVE_JOURNAL_FILE_NAME))
                .expect("recovered journal metadata")
                .len(),
            source.durable_cutoff().end_offset()
        );
        assert_eq!(
            fs::read(directory.join(ACTIVE_CHECKPOINT_FILE_NAME))
                .expect("read unchanged checkpoint"),
            checkpoint
        );
        let committed = inspection.committed();
        drop(recovered);

        let reopened = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("clean reopen retains report without another recovery");
        assert_eq!(reopened.inspection().committed(), committed);
        assert_eq!(reopened.inspection().latest_recovery(), Some(report));
        drop(reopened);
        fs::remove_dir_all(directory).expect("remove recovery fixture");
    }

    #[test]
    fn each_closed_terminal_suffix_subtype_recovers_without_adopting_bytes() {
        for (name, expected) in [
            (
                "invalid-prefix",
                crate::RecoveryClassification::InvalidFramePrefix,
            ),
            (
                "truncated-frame",
                crate::RecoveryClassification::TruncatedDeclaredFrame,
            ),
            (
                "invalid-frame",
                crate::RecoveryClassification::InvalidCompleteFrame,
            ),
        ] {
            let directory = test_directory(63);
            let store = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::CreateNew,
            ))
            .expect("create subtype fixture");
            drop(store);
            let frame = PreparedAdmissionV1::new(test_support::no_change_admission())
                .expect("subtype admission")
                .into_frame(crate::AppendSequenceV1::new(1).expect("subtype sequence"))
                .expect("subtype frame");
            let bytes = match name {
                "invalid-prefix" => {
                    let mut bytes = vec![0_u8; crate::JOURNAL_V1_FRAME_PREFIX_LEN];
                    bytes[8..16].copy_from_slice(&1_u64.to_be_bytes());
                    bytes
                }
                "truncated-frame" => frame.bytes()[..25].to_vec(),
                "invalid-frame" => {
                    let mut bytes = frame.bytes().to_vec();
                    let last = bytes.len() - 1;
                    bytes[last] ^= 1;
                    bytes
                }
                _ => unreachable!("closed subtype fixture"),
            };
            append_raw_suffix(&directory, 1, &bytes);
            let recovered = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::OpenExisting,
            ))
            .expect("closed terminal subtype recovers");
            assert_eq!(
                recovered
                    .inspection()
                    .latest_recovery()
                    .expect("subtype report")
                    .classification(),
                expected
            );
            assert!(recovered.recovered_records().is_empty());
            drop(recovered);
            fs::remove_dir_all(directory).expect("remove subtype fixture");
        }
    }

    #[test]
    fn consecutive_recoveries_advance_reports_and_ordinary_commit_cleans_only_older_slot() {
        let directory = test_directory(64);
        let store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create repeated recovery fixture");
        drop(store);
        append_raw_suffix(&directory, 1, b"OCHF\0");
        let first = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("first recovery");
        let first_report = first.inspection().latest_recovery().expect("first report");
        drop(first);

        append_raw_suffix(&directory, 1, b"OCHF\0\x01");
        let mut second = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("second recovery");
        let second_report = second
            .inspection()
            .latest_recovery()
            .expect("second report");
        assert_eq!(second_report.report_generation(), 2);
        assert_eq!(
            second_report.source_manifest_generation(),
            first_report.committing_manifest_generation()
        );
        assert_eq!(
            RECOVERY_SLOT_NAMES
                .iter()
                .filter(|name| directory.join(name).exists())
                .count(),
            2,
            "both retained manifests protect their exact report slots"
        );
        set_publish_fault(53);
        let admission = test_support::no_change_admission();
        let declaration = admission.declaration();
        assert!(
            second
                .register(
                    declaration.series_id(),
                    declaration.binding().clone(),
                    declaration.payload().clone(),
                    declaration.evidence().clone(),
                )
                .is_err(),
            "postcommit cleanup fault reports no registry success"
        );
        drop(second);
        let reopened = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("reopen after bounded report cleanup");
        assert_eq!(reopened.inspection().latest_recovery(), Some(second_report));
        assert_eq!(
            RECOVERY_SLOT_NAMES
                .iter()
                .filter(|name| directory.join(name).exists())
                .count(),
            1,
            "reopen removes only the strictly older unreferenced report"
        );
        drop(reopened);
        fs::remove_dir_all(directory).expect("remove repeated recovery fixture");
    }

    #[test]
    fn rotated_recovery_preserves_retry_registry_catalog_and_report_history() {
        let directory = test_directory(61);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create rotated recovery fixture");
        register_rotation_fixture(&mut store);
        append_durable(&mut store, "recovery-rotation-one");
        let rotated = store.rotate().expect("rotate recovery fixture");
        let registry = store.registry_snapshot();
        let retry = store.retry_state_snapshot();
        let catalog = rotated.generation_catalog();
        drop(store);
        append_raw_suffix(&directory, 2, b"OCHF\0\x01\x01");

        let mut recovered = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("recover rotated active suffix");
        let report = recovered
            .inspection()
            .latest_recovery()
            .expect("rotated report");
        assert_eq!(report.active_generation(), 2);
        assert_eq!(report.active_sequence_floor(), 1);
        assert_eq!(report.append_sequence(), 1);
        assert_eq!(
            report.committed_end_offset(),
            crate::JOURNAL_V1_HEADER_LEN as u64
        );
        assert_eq!(recovered.registry_snapshot(), registry);
        assert_eq!(recovered.retry_state_snapshot(), retry);
        assert_eq!(
            recovered.inspection().committed().generation_catalog(),
            catalog
        );

        let (_, qualification) = append_durable(&mut recovered, "recovery-rotation-two");
        assert_eq!(recovered.inspection().latest_recovery(), Some(report));
        recovered
            .rotate()
            .expect("rotate recovered active generation");
        assert_eq!(recovered.inspection().latest_recovery(), Some(report));
        let expected_retry = recovered.retry_state_snapshot();
        drop(recovered);

        let reopened = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("historical report is covered by catalog");
        assert_eq!(reopened.inspection().latest_recovery(), Some(report));
        assert_eq!(reopened.retry_state_snapshot(), expected_retry);
        assert!(matches!(
            reopened.retry_state_snapshot().classify(&qualification),
            crate::RetryStateMatch::Replay(_)
        ));
        drop(reopened);
        fs::remove_dir_all(directory).expect("remove rotated recovery fixture");
    }

    #[test]
    fn valid_or_ambiguous_post_root_bytes_refuse_without_mutation() {
        for shape in [
            "valid",
            "valid-torn",
            "malformed-later",
            "sequence",
            "prefix-corruption",
        ] {
            let directory = test_directory(62);
            let mut store = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::CreateNew,
            ))
            .expect("create suffix refusal fixture");
            register_rotation_fixture(&mut store);
            append_durable(&mut store, "suffix-root");
            let admission = test_support::no_change_admission_with_retry_key("suffix-candidate");
            let sequence = if shape == "sequence" { 3 } else { 2 };
            let frame = PreparedAdmissionV1::new(admission)
                .expect("suffix admission")
                .into_frame(crate::AppendSequenceV1::new(sequence).expect("suffix sequence"))
                .expect("suffix frame");
            drop(store);
            match shape {
                "valid" | "sequence" => append_raw_suffix(&directory, 1, frame.bytes()),
                "valid-torn" => {
                    let mut bytes = frame.bytes().to_vec();
                    bytes.extend_from_slice(&[0xa5; 7]);
                    append_raw_suffix(&directory, 1, &bytes);
                }
                "malformed-later" => {
                    let mut bytes = [0_u8; crate::JOURNAL_V1_FRAME_PREFIX_LEN].to_vec();
                    bytes[8..16].copy_from_slice(&2_u64.to_be_bytes());
                    bytes.extend_from_slice(frame.bytes());
                    append_raw_suffix(&directory, 1, &bytes);
                }
                "prefix-corruption" => append_raw_suffix(&directory, 1, &[0xa5; 11]),
                _ => unreachable!("closed suffix shape"),
            }
            let before = directory_bytes(&directory);
            assert!(
                ManifestStore::open(test_config(
                    directory.clone(),
                    ActiveJournalOpenMode::OpenExisting,
                ))
                .is_err()
            );
            assert_eq!(directory_bytes(&directory), before, "shape {shape}");
            fs::remove_dir_all(directory).expect("remove suffix refusal fixture");
        }
    }

    #[test]
    fn possible_newer_manifest_damage_never_falls_back_and_malformed_recovery_staging_refuses() {
        let newer = test_directory(65);
        let mut store =
            ManifestStore::open(test_config(newer.clone(), ActiveJournalOpenMode::CreateNew))
                .expect("create newer-manifest fixture");
        register_rotation_fixture(&mut store);
        assert_eq!(store.inspection().committed().manifest_generation(), 2);
        drop(store);
        let path = newer.join(MANIFEST_SLOT_1_FILE_NAME);
        let mut bytes = fs::read(&path).expect("read newest manifest");
        bytes[MANIFEST_LEN - 4] ^= 1;
        fs::write(path, bytes).expect("damage newest manifest evidence");
        let before = directory_bytes(&newer);
        assert!(
            ManifestStore::open(test_config(
                newer.clone(),
                ActiveJournalOpenMode::OpenExisting,
            ))
            .is_err(),
            "an older parseable root cannot replace possible newer evidence"
        );
        assert_eq!(directory_bytes(&newer), before);
        fs::remove_dir_all(newer).expect("remove newer-manifest fixture");

        let staging = test_directory(66);
        let store = ManifestStore::open(test_config(
            staging.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create recovery-staging fixture");
        drop(store);
        fs::write(staging.join(RECOVERY_STAGING_FILE_NAME), [0_u8; 17])
            .expect("write partial recovery staging");
        let before = directory_bytes(&staging);
        assert!(matches!(
            ManifestStore::open(test_config(
                staging.clone(),
                ActiveJournalOpenMode::OpenExisting,
            )),
            Err(ManifestStoreError::InterruptedPublication)
        ));
        assert_eq!(directory_bytes(&staging), before);
        fs::remove_dir_all(staging).expect("remove recovery-staging fixture");
    }

    #[test]
    fn sanitized_open_classification_preserves_exact_error_authority() {
        let exact = ManifestStoreError::InvalidManifest;
        assert_eq!(
            exact.open_classification(),
            ManifestOpenClassification::CorruptAuthority
        );
        assert_eq!(exact, ManifestStoreError::InvalidManifest);
        assert_eq!(
            ManifestStoreError::InterruptedPublication.open_classification(),
            ManifestOpenClassification::InterruptedPublication
        );
        assert_eq!(
            ManifestStoreError::UnsupportedStoreFormat.open_classification(),
            ManifestOpenClassification::UnsupportedFormat
        );
        let pressure =
            classify_manifest_io(ManifestIoOperation::Write, ErrorKind::StorageFull, Some(28));
        assert_eq!(
            pressure.open_classification(),
            ManifestOpenClassification::StoragePressure
        );
        assert!(matches!(pressure, ManifestStoreError::StoragePressure(_)));
        assert_eq!(
            ManifestStoreError::ReopenRequired.open_classification(),
            ManifestOpenClassification::ReopenRequired
        );
    }

    #[test]
    fn manifest_pressure_classification_uses_kind_and_mutating_boundary_only() {
        for kind in [ErrorKind::StorageFull, ErrorKind::QuotaExceeded] {
            let ManifestStoreError::StoragePressure(evidence) =
                classify_manifest_io(ManifestIoOperation::Write, kind, Some(777))
            else {
                panic!("normalized pressure must be typed at a mutating boundary");
            };
            assert_eq!(evidence.kind(), kind);
            assert_eq!(evidence.raw_os_error(), Some(777));
        }
        for kind in [
            ErrorKind::FileTooLarge,
            ErrorKind::ReadOnlyFilesystem,
            ErrorKind::PermissionDenied,
            ErrorKind::Other,
        ] {
            let ManifestStoreError::Io(evidence) =
                classify_manifest_io(ManifestIoOperation::Write, kind, Some(28))
            else {
                panic!("raw code and non-pressure kinds must remain generic I/O");
            };
            assert_eq!(evidence.kind(), kind);
            assert_eq!(evidence.raw_os_error(), Some(28));
        }
        for operation in [
            ManifestIoOperation::OpenArtifact,
            ManifestIoOperation::Read,
            ManifestIoOperation::Metadata,
            ManifestIoOperation::LockStore,
        ] {
            assert!(matches!(
                classify_manifest_io(operation, ErrorKind::StorageFull, Some(28)),
                ManifestStoreError::Io(_)
            ));
        }
    }

    #[test]
    fn active_pressure_is_preserved_by_manifest_and_hostile_repeats_do_not_mutate() {
        let directory = test_directory(68);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create active-pressure store");
        register_rotation_fixture(&mut store);
        let admission = test_support::no_change_admission();
        let sequence = store.next_append_sequence().expect("pressure sequence");
        let frame = PreparedAdmissionV1::new(admission.clone())
            .expect("pressure admission")
            .into_frame(sequence)
            .expect("pressure frame");
        let committed = store.inspection().committed();
        store
            .journal
            .inject_append_pressure(11, ErrorKind::StorageFull);
        let error = store.append(&frame).expect_err("active pressure refuses");
        let ManifestStoreError::Active(ActiveJournalError::StoragePressure(evidence)) = error
        else {
            panic!("manifest wrapping must retain active pressure evidence");
        };
        assert_eq!(evidence.operation(), crate::StoreIoOperation::Write);
        assert_eq!(evidence.kind(), ErrorKind::StorageFull);
        let pressured = store.inspection();
        assert_eq!(pressured.write_state(), StoreWriteState::ReopenRequired);
        assert_eq!(
            pressured.active().write_state(),
            StoreWriteState::ReopenRequired
        );
        assert_eq!(pressured.committed(), committed);
        let after_pressure = directory_bytes(&directory);
        for _ in 0..16 {
            assert_eq!(
                store.next_append_sequence(),
                Err(ManifestStoreError::ReopenRequired)
            );
            assert_eq!(
                store.preflight_historical_declaration(frame.admission().declaration()),
                Err(ManifestStoreError::ReopenRequired)
            );
            assert_eq!(
                store.append(&frame),
                Err(ManifestStoreError::ReopenRequired)
            );
            assert_eq!(
                store.sync_pending(&[]),
                Err(ManifestStoreError::ReopenRequired)
            );
            assert_eq!(store.rotate(), Err(ManifestStoreError::ReopenRequired));
            assert_eq!(
                store.bind(admission.envelope().clone()),
                Err(ManifestStoreError::ReopenRequired)
            );
        }
        assert_eq!(store.inspection(), pressured);
        assert_eq!(directory_bytes(&directory), after_pressure);
        drop(store);

        let reopened = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("current conservative recovery handles terminal pressure suffix");
        assert_eq!(
            reopened.inspection().write_state(),
            StoreWriteState::Writable
        );
        assert!(reopened.inspection().latest_recovery().is_some());
        drop(reopened);
        fs::remove_dir_all(directory).expect("remove active-pressure fixture");
    }

    #[test]
    fn active_checkpoint_pressure_does_not_advance_manifest_or_mechanical_cutoff() {
        let directory = test_directory(73);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create active-sync-pressure store");
        register_rotation_fixture(&mut store);
        let admission = test_support::no_change_admission_with_retry_key("active-sync-pressure");
        let sequence = store
            .next_append_sequence()
            .expect("active pressure sequence");
        let frame = PreparedAdmissionV1::new(admission)
            .expect("active pressure admission")
            .into_frame(sequence)
            .expect("active pressure frame");
        let end = store.append(&frame).expect("append before active pressure");
        let pending = [PendingRetryOutcome::new(
            frame.admission().retry().clone(),
            sequence.get(),
            end,
        )];
        let before = store.inspection();
        store
            .journal
            .inject_sync_pressure(1, ErrorKind::QuotaExceeded);
        let error = store
            .sync_pending(&pending)
            .expect_err("checkpoint pressure refuses");
        assert!(matches!(
            error,
            ManifestStoreError::Active(ActiveJournalError::StoragePressure(evidence))
                if evidence.operation() == crate::StoreIoOperation::Write
                    && evidence.kind() == ErrorKind::QuotaExceeded
        ));
        let pressured = store.inspection();
        assert_eq!(pressured.write_state(), StoreWriteState::ReopenRequired);
        assert_eq!(pressured.committed(), before.committed());
        assert_eq!(
            pressured.active().durable_cutoff(),
            before.active().durable_cutoff()
        );
        assert_eq!(
            pressured.active().sync_count(),
            before.active().sync_count()
        );
        let after_pressure = directory_bytes(&directory);
        for _ in 0..16 {
            assert_eq!(
                store.sync_pending(&pending),
                Err(ManifestStoreError::ReopenRequired)
            );
        }
        assert_eq!(directory_bytes(&directory), after_pressure);
        drop(store);
        let before_reopen = directory_bytes(&directory);
        assert!(
            ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::OpenExisting,
            ))
            .is_err()
        );
        assert_eq!(directory_bytes(&directory), before_reopen);
        fs::remove_dir_all(directory).expect("remove active-sync-pressure fixture");
    }

    #[test]
    fn store_publication_pressure_is_sticky_before_and_after_slot_publication() {
        for (code, operation) in [
            (1_u8, ManifestIoOperation::Write),
            (2, ManifestIoOperation::SyncArtifact),
            (3, ManifestIoOperation::Publish),
            (4, ManifestIoOperation::SyncDirectory),
        ] {
            let directory = test_directory(69);
            let mut store = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::CreateNew,
            ))
            .expect("create registry-pressure store");
            let admission = test_support::no_change_admission();
            let declaration = admission.declaration();
            set_pressure_fault(code, ErrorKind::QuotaExceeded);
            let error = store
                .register(
                    declaration.series_id(),
                    declaration.binding().clone(),
                    declaration.payload().clone(),
                    declaration.evidence().clone(),
                )
                .expect_err("registry pressure refuses");
            let ManifestStoreError::StoragePressure(evidence) = error else {
                panic!("first store publication pressure must retain evidence");
            };
            assert_eq!(evidence.operation(), operation);
            assert_eq!(evidence.kind(), ErrorKind::QuotaExceeded);
            assert_eq!(
                store.inspection().write_state(),
                StoreWriteState::ReopenRequired
            );
            assert!(store.registry_snapshot().series().is_empty());
            let after_pressure = directory_bytes(&directory);
            for _ in 0..16 {
                assert_eq!(
                    store.register(
                        declaration.series_id(),
                        declaration.binding().clone(),
                        declaration.payload().clone(),
                        declaration.evidence().clone(),
                    ),
                    Err(ManifestStoreError::ReopenRequired)
                );
                assert_eq!(
                    store.bind(admission.envelope().clone()),
                    Err(ManifestStoreError::ReopenRequired)
                );
                assert_eq!(
                    store.sync_pending(&[]),
                    Err(ManifestStoreError::ReopenRequired)
                );
            }
            assert_eq!(directory_bytes(&directory), after_pressure);
            drop(store);
            let before_reopen = directory_bytes(&directory);
            let reopened = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::OpenExisting,
            ));
            if let Ok(reopened) = reopened {
                assert!(reopened.registry_snapshot().series().is_empty());
            } else {
                assert_eq!(directory_bytes(&directory), before_reopen);
            }
            fs::remove_dir_all(directory).expect("remove registry-pressure fixture");
        }
    }

    #[test]
    fn lifecycle_and_rotation_cleanup_pressure_preserve_committed_authority() {
        let lifecycle = test_directory(74);
        let mut store = ManifestStore::open(test_config(
            lifecycle.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create lifecycle-pressure store");
        let admission = test_support::no_change_admission();
        let declaration = admission.declaration();
        let (registered, committed) = store
            .register(
                declaration.series_id(),
                declaration.binding().clone(),
                declaration.payload().clone(),
                declaration.evidence().clone(),
            )
            .expect("register lifecycle fixture");
        let registry = store.registry_snapshot();
        set_pressure_fault(1, ErrorKind::StorageFull);
        let error = store
            .retire(
                registered.series_id(),
                registered.revision(),
                registered.evidence().clone(),
            )
            .expect_err("retirement publication pressure refuses");
        assert!(matches!(
            error,
            ManifestStoreError::StoragePressure(evidence)
                if evidence.operation() == ManifestIoOperation::Write
                    && evidence.kind() == ErrorKind::StorageFull
        ));
        assert_eq!(store.registry_snapshot(), registry);
        assert_eq!(store.inspection().committed(), committed);
        assert_eq!(
            store.revise(
                registered.series_id(),
                registered.revision(),
                registered.payload().clone(),
                registered.evidence().clone(),
            ),
            Err(ManifestStoreError::ReopenRequired)
        );
        assert_eq!(
            store.register(
                registered.series_id(),
                registered.binding().clone(),
                registered.payload().clone(),
                registered.evidence().clone(),
            ),
            Err(ManifestStoreError::ReopenRequired)
        );
        assert_eq!(
            store.bind(admission.envelope().clone()),
            Err(ManifestStoreError::ReopenRequired)
        );
        drop(store);
        fs::remove_dir_all(lifecycle).expect("remove lifecycle-pressure fixture");

        let rotation = test_directory(75);
        let mut store = ManifestStore::open(test_config(
            rotation.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create rotation-cleanup-pressure store");
        register_rotation_fixture(&mut store);
        let (prior, _) = append_durable(&mut store, "rotation-cleanup-pressure");
        set_pressure_fault(39, ErrorKind::QuotaExceeded);
        let error = store
            .rotate()
            .expect_err("rotation cleanup pressure refuses");
        assert!(matches!(
            error,
            ManifestStoreError::StoragePressure(evidence)
                if evidence.operation() == ManifestIoOperation::Remove
                    && evidence.kind() == ErrorKind::QuotaExceeded
        ));
        let pressured = store.inspection();
        assert_eq!(pressured.write_state(), StoreWriteState::ReopenRequired);
        assert_eq!(
            pressured.committed().manifest_generation(),
            prior.manifest_generation() + 1
        );
        assert_eq!(pressured.generations().active_generation(), 2);
        let after_pressure = directory_bytes(&rotation);
        for _ in 0..16 {
            assert_eq!(store.rotate(), Err(ManifestStoreError::ReopenRequired));
        }
        assert_eq!(directory_bytes(&rotation), after_pressure);
        drop(store);
        let reopened = ManifestStore::open(test_config(
            rotation.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("reopen converges committed rotation cleanup");
        assert_eq!(reopened.inspection().committed(), pressured.committed());
        assert_eq!(
            reopened.inspection().write_state(),
            StoreWriteState::Writable
        );
        drop(reopened);
        fs::remove_dir_all(rotation).expect("remove rotation-cleanup-pressure fixture");
    }

    #[test]
    fn manifest_root_publication_pressure_never_reports_false_commit() {
        for (code, operation, published) in [
            (5_u8, ManifestIoOperation::Write, false),
            (6, ManifestIoOperation::SyncArtifact, false),
            (7, ManifestIoOperation::Publish, false),
            (8, ManifestIoOperation::SyncDirectory, true),
        ] {
            let directory = test_directory(77);
            let mut store = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::CreateNew,
            ))
            .expect("create manifest-root-pressure store");
            register_rotation_fixture(&mut store);
            let admission =
                test_support::no_change_admission_with_retry_key("manifest-root-pressure");
            let sequence = store
                .next_append_sequence()
                .expect("manifest pressure sequence");
            let frame = PreparedAdmissionV1::new(admission)
                .expect("manifest pressure admission")
                .into_frame(sequence)
                .expect("manifest pressure frame");
            let end = store.append(&frame).expect("manifest pressure append");
            let pending = [PendingRetryOutcome::new(
                frame.admission().retry().clone(),
                sequence.get(),
                end,
            )];
            let prior = store.inspection().committed();
            set_pressure_fault(code, ErrorKind::StorageFull);
            let error = store
                .sync_pending(&pending)
                .expect_err("manifest publication pressure refuses");
            assert!(matches!(
                error,
                ManifestStoreError::StoragePressure(evidence)
                    if evidence.operation() == operation
                        && evidence.kind() == ErrorKind::StorageFull
            ));
            assert_eq!(
                store.inspection().write_state(),
                StoreWriteState::ReopenRequired
            );
            assert_eq!(store.inspection().committed(), prior);
            let after_pressure = directory_bytes(&directory);
            for _ in 0..16 {
                assert_eq!(
                    store.sync_pending(&pending),
                    Err(ManifestStoreError::ReopenRequired)
                );
            }
            assert_eq!(directory_bytes(&directory), after_pressure);
            drop(store);

            let reopened = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::OpenExisting,
            ));
            if published {
                let reopened = reopened.expect("published manifest is selected only by reopen");
                assert_eq!(
                    reopened.inspection().committed().manifest_generation(),
                    prior.manifest_generation() + 1
                );
                drop(reopened);
            } else {
                assert!(matches!(
                    reopened,
                    Err(ManifestStoreError::InterruptedPublication
                        | ManifestStoreError::InvalidManifest
                        | ManifestStoreError::InvalidRetry
                        | ManifestStoreError::Active(ActiveJournalError::InvalidLayout))
                ));
                assert_eq!(directory_bytes(&directory), after_pressure);
            }
            fs::remove_dir_all(directory).expect("remove manifest-root-pressure fixture");
        }
    }

    #[test]
    fn logical_retry_preflight_refuses_before_durability_without_poisoning() {
        let directory = test_directory(70);
        let mut store = ManifestStore::open(test_config(
            directory.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create logical-preflight store");
        register_rotation_fixture(&mut store);
        let admission = test_support::no_change_admission_with_retry_key("preflight");
        let sequence = store.next_append_sequence().expect("preflight sequence");
        let frame = PreparedAdmissionV1::new(admission)
            .expect("preflight admission")
            .into_frame(sequence)
            .expect("preflight frame");
        let end = store.append(&frame).expect("preflight append");
        let before = directory_bytes(&directory);
        assert_eq!(
            store.sync_pending(&[]),
            Err(ManifestStoreError::InvalidRetry)
        );
        assert_eq!(directory_bytes(&directory), before);
        assert_eq!(store.inspection().write_state(), StoreWriteState::Writable);
        assert_eq!(
            store
                .inspection()
                .active()
                .durable_cutoff()
                .append_sequence(),
            0
        );
        store
            .sync_pending(&[PendingRetryOutcome::new(
                frame.admission().retry().clone(),
                sequence.get(),
                end,
            )])
            .expect("valid prepared transaction remains usable");
        fs::remove_dir_all(directory).expect("remove logical-preflight fixture");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn recovery_and_genesis_pressure_are_typed_and_only_reopen_can_continue() {
        let genesis = test_directory(71);
        set_pressure_fault(41, ErrorKind::StorageFull);
        let Err(error) = ManifestStore::open(test_config(
            genesis.clone(),
            ActiveJournalOpenMode::CreateNew,
        )) else {
            panic!("genesis pressure must refuse");
        };
        assert!(matches!(
            error,
            ManifestStoreError::StoragePressure(evidence)
                if evidence.operation() == ManifestIoOperation::SyncArtifact
                    && evidence.kind() == ErrorKind::StorageFull
        ));
        let reopened = ManifestStore::open(test_config(
            genesis.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ));
        assert!(matches!(
            &reopened,
            Ok(_) | Err(ManifestStoreError::InterruptedPublication)
        ));
        drop(reopened);
        fs::remove_dir_all(genesis).expect("remove genesis-pressure fixture");

        let recovery = test_directory(72);
        let store = ManifestStore::open(test_config(
            recovery.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create recovery-pressure store");
        let source = store.inspection().committed();
        drop(store);
        append_raw_suffix(&recovery, 1, b"OCHF\0\x01\x01\0\0\0\0");
        crate::active::set_recovery_pressure_fault(50, ErrorKind::QuotaExceeded);
        let Err(error) = ManifestStore::open(test_config(
            recovery.clone(),
            ActiveJournalOpenMode::OpenExisting,
        )) else {
            panic!("recovery truncate pressure must refuse");
        };
        assert!(matches!(
            error,
            ManifestStoreError::Active(ActiveJournalError::StoragePressure(evidence))
                if evidence.operation() == crate::StoreIoOperation::Truncate
                    && evidence.kind() == ErrorKind::QuotaExceeded
        ));
        let after_pressure = directory_bytes(&recovery);
        assert_eq!(
            select_current_manifest(
                read_manifest_slots(&recovery, test_support::store_id(1))
                    .expect("read unchanged recovery root")
            )
            .expect("select unchanged recovery root")
            .expect("recovery source root")
            .1
            .generation,
            source.manifest_generation()
        );
        let reopened = ManifestStore::open(test_config(
            recovery.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("reopen alone continues current recovery transaction");
        assert!(reopened.inspection().latest_recovery().is_some());
        assert_ne!(directory_bytes(&recovery), after_pressure);
        drop(reopened);
        fs::remove_dir_all(recovery).expect("remove recovery-pressure fixture");

        let recovery_sync = test_directory(76);
        let store = ManifestStore::open(test_config(
            recovery_sync.clone(),
            ActiveJournalOpenMode::CreateNew,
        ))
        .expect("create recovery-sync-pressure store");
        let source = store.inspection().committed();
        drop(store);
        append_raw_suffix(&recovery_sync, 1, b"OCHF\0\x01\x01\0\0\0\0");
        crate::active::set_recovery_pressure_fault(51, ErrorKind::StorageFull);
        let Err(error) = ManifestStore::open(test_config(
            recovery_sync.clone(),
            ActiveJournalOpenMode::OpenExisting,
        )) else {
            panic!("recovery sync pressure must refuse");
        };
        assert!(matches!(
            error,
            ManifestStoreError::Active(ActiveJournalError::StoragePressure(evidence))
                if evidence.operation() == crate::StoreIoOperation::SyncJournal
                    && evidence.kind() == ErrorKind::StorageFull
        ));
        assert_eq!(
            select_current_manifest(
                read_manifest_slots(&recovery_sync, test_support::store_id(1))
                    .expect("read recovery-sync root")
            )
            .expect("select recovery-sync root")
            .expect("recovery-sync source root")
            .1
            .generation,
            source.manifest_generation()
        );
        let reopened = ManifestStore::open(test_config(
            recovery_sync.clone(),
            ActiveJournalOpenMode::OpenExisting,
        ))
        .expect("reopen completes recovery after sync pressure");
        assert!(reopened.inspection().latest_recovery().is_some());
        drop(reopened);
        fs::remove_dir_all(recovery_sync).expect("remove recovery-sync-pressure fixture");
    }

    #[test]
    fn recovery_publication_and_truncate_faults_converge_or_refuse_typed() {
        for code in [45_u8, 46, 47, 48, 49, 50, 51, 52, 5, 6, 10, 7, 8] {
            let directory = test_directory(code);
            let store = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::CreateNew,
            ))
            .expect("create recovery fault fixture");
            drop(store);
            append_raw_suffix(&directory, 1, b"OCHF\0\x01\x01\0\0");
            if (50..=52).contains(&code) {
                crate::active::set_recovery_fault(code);
            } else {
                set_publish_fault(code);
            }
            assert!(
                ManifestStore::open(test_config(
                    directory.clone(),
                    ActiveJournalOpenMode::OpenExisting,
                ))
                .is_err()
            );
            let after_fault = directory_bytes(&directory);
            let reopened = ManifestStore::open(test_config(
                directory.clone(),
                ActiveJournalOpenMode::OpenExisting,
            ));
            if matches!(code, 45 | 5) {
                assert!(matches!(
                    reopened,
                    Err(ManifestStoreError::InterruptedPublication)
                ));
                assert_eq!(directory_bytes(&directory), after_fault);
            } else {
                let reopened = reopened.expect("complete recovery evidence converges");
                assert_eq!(reopened.inspection().committed().manifest_generation(), 2);
                assert!(reopened.inspection().latest_recovery().is_some());
                drop(reopened);
            }
            fs::remove_dir_all(directory).expect("remove recovery fault fixture");
        }
    }

    #[test]
    fn current_manifest_v1_is_exactly_160_bytes_and_refuses_hostile_fields() {
        let store_id = test_support::store_id(1);
        let record = ManifestRecord {
            generation: 4,
            registry: RegistryReference {
                slot: 1,
                generation: 2,
                length: 68,
                checksum: 7,
            },
            cutoff: DurableCutoff::from_manifest(
                store_id,
                2,
                1,
                1,
                crate::JOURNAL_V1_HEADER_LEN as u64,
            ),
            retry: RetryArtifactReference {
                public: RetryStateReference::new(1, 3),
                length: 200,
                checksum: 9,
            },
            recovery: None,
            sequence_floor: 1,
            catalog: Some(GenerationCatalogReference::new(0, 1, 132, 11)),
        };
        let canonical = encode_manifest(record);
        assert_eq!(canonical.len(), MANIFEST_LEN);
        assert_eq!(decode_manifest(&canonical, store_id), Ok(record));
        assert!(decode_manifest(&canonical[..159], store_id).is_err());
        let mut trailing = canonical.clone();
        trailing.push(0);
        assert!(decode_manifest(&trailing, store_id).is_err());

        for (offset, length, value) in [
            (8_usize, 2_usize, 2_u8),
            (10, 2, 0),
            (36, 8, 0),
            (52, 8, 0),
            (60, 8, 29),
            (116, 1, 2),
            (124, 8, 0),
            (132, 1, 3),
            (133, 1, 1),
            (136, 8, 0),
            (144, 8, 0),
        ] {
            let mut hostile = canonical.clone();
            hostile[offset..offset + length].fill(value);
            let checksum = crc32c(&hostile[..156]);
            hostile[156..160].copy_from_slice(&checksum.to_be_bytes());
            assert!(
                decode_manifest(&hostile, store_id).is_err(),
                "hostile current Manifest V1 field at {offset} must refuse"
            );
        }
        let mut checksum = canonical;
        checksum[159] ^= 1;
        assert!(decode_manifest(&checksum, store_id).is_err());
    }

    #[test]
    fn registry_parser_refuses_hostile_counts_lengths_limits_reserved_and_checksum() {
        let snapshot =
            SeriesRegistry::new(test_support::store_id(1), SeriesRegistryLimits::new(2, 4))
                .snapshot();
        let canonical = encode_registry_snapshot(1, &snapshot).expect("empty registry encoding");
        assert!(decode_registry_snapshot(&canonical).is_ok());

        for offset in [9_usize, 60, canonical.len() - 1] {
            let mut hostile = canonical.clone();
            hostile[offset] ^= 0xff;
            if offset != canonical.len() - 1 {
                let checksum_offset = hostile.len() - 4;
                let checksum = crc32c(&hostile[..checksum_offset]);
                hostile[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
            }
            assert!(matches!(
                decode_registry_snapshot(&hostile),
                Err(ManifestStoreError::InvalidRegistry)
            ));
        }
        let assert_hostile = |range: std::ops::Range<usize>, replacement: &[u8]| {
            let mut hostile = canonical.clone();
            hostile[range].copy_from_slice(replacement);
            let checksum_offset = hostile.len() - 4;
            let checksum = crc32c(&hostile[..checksum_offset]);
            hostile[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
            assert!(matches!(
                decode_registry_snapshot(&hostile),
                Err(ManifestStoreError::InvalidRegistry)
            ));
        };
        assert_hostile(28..36, &0_u64.to_be_bytes());
        assert_hostile(
            36..40,
            &u32::try_from(MAX_PERSISTED_REGISTRY_SERIES + 1)
                .expect("hard series bound fits u32")
                .to_be_bytes(),
        );
        assert_hostile(
            40..44,
            &u32::try_from(MAX_PERSISTED_REGISTRY_REVISIONS + 1)
                .expect("hard revision bound fits u32")
                .to_be_bytes(),
        );
        assert_hostile(44..48, &1_u32.to_be_bytes());
        assert_hostile(48..52, &1_u32.to_be_bytes());
        assert_hostile(52..60, &1_u64.to_be_bytes());
        let mut trailing = canonical.clone();
        trailing.push(0);
        assert!(matches!(
            decode_registry_snapshot(&trailing),
            Err(ManifestStoreError::InvalidRegistry)
        ));
        assert!(matches!(
            decode_registry_snapshot(&canonical[..canonical.len() - 1]),
            Err(ManifestStoreError::InvalidRegistry)
        ));
    }

    #[test]
    fn registry_encoding_preflights_exact_bytes_before_payload_allocation() {
        let mut registry =
            SeriesRegistry::new(test_support::store_id(1), SeriesRegistryLimits::new(1, 1));
        let admission = test_support::no_change_admission();
        let declaration = admission.declaration();
        registry
            .register(
                declaration.series_id(),
                declaration.binding().clone(),
                declaration.payload().clone(),
                declaration.evidence().clone(),
            )
            .expect("register preflight fixture");
        let snapshot = registry.snapshot();
        let encoded = encode_registry_snapshot(1, &snapshot).expect("bounded registry encoding");
        assert_eq!(
            encode_registry_snapshot_with_limit(1, &snapshot, encoded.len() - 1),
            Err(ManifestStoreError::InvalidRegistry)
        );
        assert_eq!(
            encode_registry_snapshot_with_limit(1, &snapshot, encoded.len()),
            Ok(encoded)
        );
    }
}
