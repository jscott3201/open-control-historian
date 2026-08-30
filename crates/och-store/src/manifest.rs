//! Bounded manifest-rooted active journal and canonical registry persistence.

use crate::active::{
    ActiveJournal, ActiveJournalConfig, active_checkpoint_file_name, active_journal_file_name,
    preflight_manifest_genesis,
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
use crate::retry::{
    RetryArtifactReference, RetryStateCodecError, decode_retry_state_at_slot, encode_retry_state,
};
use crate::{
    ACTIVE_CHECKPOINT_FILE_NAME, ACTIVE_JOURNAL_FILE_NAME, ACTIVE_JOURNAL_GENERATION,
    ActiveJournalError, ActiveJournalInspection, ActiveJournalLimits, ActiveJournalOpenMode,
    DurableCutoff, JournalV1Error, PreparedFrameV1,
};
use crate::{
    MAX_PERSISTED_RETRY_ENTRIES, MAX_RETRY_STATE_BYTES, PendingRetryOutcome,
    RetryPersistenceOptions, RetryStateReference, RetryStateSnapshot,
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
const MAX_INVENTORY_ENTRIES: usize = 87;

#[cfg(test)]
std::thread_local! {
    static PUBLISH_FAULT: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn set_publish_fault(code: u8) {
    PUBLISH_FAULT.with(|fault| fault.set(code));
}

#[cfg(test)]
fn take_publish_fault(code: u8) -> bool {
    PUBLISH_FAULT.with(|fault| {
        if fault.get() == code {
            fault.set(0);
            true
        } else {
            false
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
    /// Create or open a fixed artifact.
    OpenArtifact,
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
    /// A prior publication failure terminally faulted this authority.
    Faulted,
    /// Manifest or registry generation cannot advance.
    GenerationExhausted,
    /// Canonical lifecycle semantics refused the requested operation.
    Model(ModelError),
    /// Active-journal ownership or mechanical durability refused.
    Active(ActiveJournalError),
    /// Generic path-free filesystem evidence.
    Io(ManifestIoEvidence),
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
            Self::GenerationExhausted => "manifest store generation exhausted",
            Self::Model(_) => "canonical registry operation refused",
            Self::Active(_) => "active journal operation refused",
            Self::Io(_) => "manifest store I/O failed",
        })
    }
}

impl Error for ManifestStoreError {}

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
    sequence_floor: u64,
    catalog: Option<GenerationCatalogReference>,
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
    manifest_slots: [Option<ManifestRecord>; 2],
    current_slot: usize,
    current: ManifestRecord,
    faulted: bool,
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
    pub fn open(config: ManifestStoreConfig) -> Result<Self, ManifestStoreError> {
        let directory = open_directory(&config.directory)?;
        let preflight = preflight_store_format(&config)?;
        let lock_path = config.directory.join(STORE_LOCK_FILE_NAME);
        let mut lock_options = OpenOptions::new();
        lock_options.read(true).write(true).truncate(false);
        if preflight == FormatPreflight::EmptyCreate {
            lock_options.create_new(true);
        }
        let store_lock = lock_options
            .open(&lock_path)
            .map_err(|error| manifest_io(ManifestIoOperation::OpenArtifact, &error))?;
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
        if (inventory.staging || inventory.rotation_staging) && !inventory.rotation_intent {
            return Err(ManifestStoreError::InterruptedPublication);
        }
        let mut manifest_slots = read_manifest_slots(&config.directory, config.store_id)?;
        let mut committed_intent = None;
        if inventory.rotation_intent {
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
        let journal = ActiveJournal::open(active_config)?;
        if journal.durable_cutoff() != current.cutoff {
            return Err(ManifestStoreError::InvalidManifest);
        }
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
        Ok(Self {
            directory_path: config.directory,
            directory,
            _store_lock: store_lock,
            journal,
            registry,
            retry,
            catalog,
            manifest_slots,
            current_slot,
            current,
            faulted: false,
        })
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
                sequence_floor: 0,
                catalog: None,
            },
            faulted: false,
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
        self.journal
            .append(frame)
            .map_err(ManifestStoreError::Active)
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
    /// A postcommit cleanup refusal terminally faults this live handle.
    pub fn rotate(&mut self) -> Result<ManifestCommit, ManifestStoreError> {
        self.ensure_usable()?;
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
        if let Err(error) = publish_rotation_intent(&self.directory_path, &self.directory, intent) {
            self.faulted = true;
            return Err(error);
        }
        let result =
            self.complete_rotation(intent, source_cutoff, catalog_slot, manifest_generation);
        if result.is_err() {
            self.faulted = true;
        }
        result
    }

    fn complete_rotation(
        &mut self,
        intent: RotationIntent,
        source_cutoff: DurableCutoff,
        catalog_slot: u8,
        manifest_generation: u64,
    ) -> Result<ManifestCommit, ManifestStoreError> {
        let sealed = publish_sealed_generation(
            &self.directory_path,
            &self.directory,
            intent,
            self.current.sequence_floor,
            self.journal.limits(),
            &self.registry,
        )?;
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
        injected_rotation_fault(35, ManifestIoOperation::OpenArtifact)?;
        let successor =
            ActiveJournal::open(successor_config).map_err(ManifestStoreError::Active)?;
        injected_rotation_fault(36, ManifestIoOperation::SyncArtifact)?;
        let provisional_reference =
            GenerationCatalogReference::new(catalog_slot, intent.catalog_generation, 1, 1);
        let provisional = self
            .catalog
            .advance(provisional_reference, sealed)
            .map_err(map_generation_codec)?;
        let catalog =
            self.publish_catalog_snapshot(catalog_slot, intent.catalog_generation, &provisional)?;
        let next = ManifestRecord {
            generation: manifest_generation,
            registry: self.current.registry,
            cutoff: successor.durable_cutoff(),
            retry: self.current.retry,
            sequence_floor: source_cutoff.append_sequence(),
            catalog: catalog.reference(),
        };
        let slot = self.publish_manifest(next)?;
        injected_rotation_fault(38, ManifestIoOperation::Publish)?;

        let source_generation = source_cutoff.journal().generation();
        let mut slots = self.manifest_slots;
        slots[slot] = Some(next);
        self.journal = successor;
        self.catalog = catalog;
        self.manifest_slots = slots;
        self.current_slot = slot;
        self.current = next;
        injected_rotation_fault(39, ManifestIoOperation::Publish)?;
        cleanup_committed_rotation(&self.directory_path, &self.directory, source_generation)?;
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
        Ok(self.commit())
    }

    /// Synchronizes the journal/checkpoint and commits their exact cutoff in a
    /// new manifest before returning.
    ///
    /// # Errors
    ///
    /// A publication failure returns no committed proof and terminally faults
    /// this open authority.
    pub fn sync_pending(
        &mut self,
        pending: &[PendingRetryOutcome],
    ) -> Result<(ManifestCommit, RetryStateSnapshot), ManifestStoreError> {
        self.ensure_usable()?;
        if pending.len() > MAX_PERSISTED_RETRY_ENTRIES {
            return Err(ManifestStoreError::InvalidRetry);
        }
        let cutoff = self.journal.sync_pending()?;
        if cutoff == self.current.cutoff {
            if !pending.is_empty() {
                self.faulted = true;
                return Err(ManifestStoreError::InvalidRetry);
            }
            return Ok((self.commit(), self.retry.clone()));
        }
        let result = self.commit_synced_pending(cutoff, pending);
        if result.is_err() {
            self.faulted = true;
        }
        result
    }

    fn commit_synced_pending(
        &mut self,
        cutoff: DurableCutoff,
        pending: &[PendingRetryOutcome],
    ) -> Result<(ManifestCommit, RetryStateSnapshot), ManifestStoreError> {
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
        let retry = self.publish_retry_snapshot(retry_public, &candidate)?;
        let next = ManifestRecord {
            generation,
            registry: self.current.registry,
            cutoff,
            retry,
            sequence_floor: self.current.sequence_floor,
            catalog: self.current.catalog,
        };
        let committed = self.publish_and_adopt_manifest(next)?;
        self.retry = candidate;
        Ok((committed, self.retry.clone()))
    }

    /// Registers revision one and commits the complete resulting snapshot.
    ///
    /// # Errors
    ///
    /// Core refusal is non-mutating. Publication failure terminally faults the
    /// open authority and reports no commit.
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
    /// Core refusal is non-mutating. Publication failure terminally faults the
    /// open authority and reports no commit.
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
    /// Core refusal is non-mutating. Publication failure terminally faults the
    /// open authority and reports no commit.
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
        let registry_reference = match self.publish_registry_snapshot(registry_generation, &after) {
            Ok(reference) => reference,
            Err(error) => {
                self.faulted = true;
                return Err(error);
            }
        };
        let manifest_generation = self
            .current
            .generation
            .checked_add(1)
            .ok_or(ManifestStoreError::GenerationExhausted)?;
        let record = ManifestRecord {
            generation: manifest_generation,
            registry: registry_reference,
            cutoff: self.journal.durable_cutoff(),
            retry: self.current.retry,
            sequence_floor: self.current.sequence_floor,
            catalog: self.current.catalog,
        };
        let commit = match self.publish_and_adopt_manifest(record) {
            Ok(commit) => commit,
            Err(error) => {
                self.faulted = true;
                return Err(error);
            }
        };
        self.registry = candidate;
        Ok((output, commit))
    }

    fn publish_and_adopt_manifest(
        &mut self,
        record: ManifestRecord,
    ) -> Result<ManifestCommit, ManifestStoreError> {
        let slot = match self.publish_manifest(record) {
            Ok(slot) => slot,
            Err(error) => {
                self.faulted = true;
                return Err(error);
            }
        };
        let mut manifest_slots = self.manifest_slots;
        manifest_slots[slot] = Some(record);
        if let Err(error) =
            remove_unreferenced_catalog_slots(&self.directory_path, &self.directory, manifest_slots)
        {
            self.faulted = true;
            return Err(error);
        }
        if let Err(error) =
            remove_unreferenced_retry_slots(&self.directory_path, &self.directory, manifest_slots)
        {
            self.faulted = true;
            return Err(error);
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
        publish_reusable_slot(
            &self.directory_path,
            &self.directory,
            REGISTRY_STAGING_FILE_NAME,
            REGISTRY_SLOT_NAMES[usize::from(slot)],
            &bytes,
            MAX_REGISTRY_SNAPSHOT_BYTES,
            |candidate| {
                let decoded = decode_registry_snapshot_at_slot(candidate, slot)?;
                if decoded.reference != reference || decoded.registry.snapshot() != *snapshot {
                    return Err(ManifestStoreError::InvalidRegistry);
                }
                Ok(())
            },
        )?;
        Ok(reference)
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
        publish_reusable_slot(
            &self.directory_path,
            &self.directory,
            RETRY_STAGING_FILE_NAME,
            RETRY_SLOT_NAMES[usize::from(public.slot())],
            &bytes,
            MAX_RETRY_STATE_BYTES,
            |candidate| {
                let (decoded_reference, decoded) = decode_retry_state_at_slot(
                    candidate,
                    public.slot(),
                    snapshot.store_id(),
                    snapshot.options(),
                )
                .map_err(map_retry_codec)?;
                if decoded_reference != reference || decoded != *snapshot {
                    return Err(ManifestStoreError::InvalidRetry);
                }
                Ok(())
            },
        )?;
        Ok(reference)
    }

    fn select_catalog_candidate_slot(&self) -> Result<u8, ManifestStoreError> {
        let referenced = self.manifest_slots.map(|slot| {
            slot.and_then(|record| record.catalog.map(GenerationCatalogReference::slot))
        });
        (0_u8..3)
            .find(|candidate| !referenced.iter().flatten().any(|slot| slot == candidate))
            .ok_or(ManifestStoreError::InvalidGeneration)
    }

    fn publish_catalog_snapshot(
        &self,
        slot: u8,
        generation: u64,
        provisional: &GenerationCatalogSnapshot,
    ) -> Result<GenerationCatalogSnapshot, ManifestStoreError> {
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
        publish_reusable_slot(
            &self.directory_path,
            &self.directory,
            GENERATION_CATALOG_STAGING_FILE_NAME,
            CATALOG_SLOT_NAMES[usize::from(slot)],
            &bytes,
            MAX_GENERATION_CATALOG_BYTES,
            |candidate| {
                let decoded = decode_catalog(candidate, slot, snapshot.store_id())
                    .map_err(map_generation_codec)?;
                if decoded != snapshot {
                    return Err(ManifestStoreError::InvalidGeneration);
                }
                Ok(())
            },
        )?;
        Ok(snapshot)
    }

    fn publish_manifest(&self, record: ManifestRecord) -> Result<usize, ManifestStoreError> {
        let target = if record.generation == 1 {
            self.manifest_slots
                .iter()
                .position(Option::is_none)
                .ok_or(ManifestStoreError::InvalidManifest)?
        } else {
            1 - self.current_slot
        };
        let bytes = encode_manifest(record);
        publish_reusable_slot(
            &self.directory_path,
            &self.directory,
            MANIFEST_STAGING_FILE_NAME,
            MANIFEST_SLOT_NAMES[target],
            &bytes,
            MANIFEST_LEN,
            |candidate| {
                if decode_manifest(candidate, self.current.cutoff.journal().store_id())? != record {
                    return Err(ManifestStoreError::InvalidManifest);
                }
                Ok(())
            },
        )?;
        Ok(target)
    }

    fn commit(&self) -> ManifestCommit {
        self.current.commit()
    }

    fn ensure_usable(&self) -> Result<(), ManifestStoreError> {
        if self.faulted {
            Err(ManifestStoreError::Faulted)
        } else {
            Ok(())
        }
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
    staging: bool,
    rotation_staging: bool,
    registry_slots: usize,
    retry_slots: usize,
    manifest_slots: usize,
    catalog_slots: usize,
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

fn inspect_inventory(directory: &Path) -> Result<Inventory, ManifestStoreError> {
    let mut count = 0_usize;
    let mut staging = false;
    let mut store_format = false;
    let mut store_format_staging = false;
    let mut rotation_staging = false;
    let mut registry_slots = 0_usize;
    let mut retry_slots = 0_usize;
    let mut manifest_slots = 0_usize;
    let mut catalog_slots = 0_usize;
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
        } else if name == MANIFEST_STAGING_FILE_NAME
            || name == REGISTRY_STAGING_FILE_NAME
            || name == RETRY_STAGING_FILE_NAME
        {
            staging = true;
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
        staging,
        rotation_staging,
        registry_slots,
        retry_slots,
        manifest_slots,
        catalog_slots,
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
            {
                return Err(ManifestStoreError::InvalidManifest);
            }
            Ok(Some((newer_index, newer)))
        }
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
            Err(error) => return Err(manifest_io(ManifestIoOperation::Publish, &error)),
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
            Err(error) => return Err(manifest_io(ManifestIoOperation::Publish, &error)),
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
                manifest_io(ManifestIoOperation::OpenArtifact, &error)
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

fn publish_sealed_generation(
    directory_path: &Path,
    directory: &File,
    intent: RotationIntent,
    sequence_floor: u64,
    limits: ActiveJournalLimits,
    registry: &SeriesRegistry,
) -> Result<SealedGeneration, ManifestStoreError> {
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
                manifest_io(ManifestIoOperation::OpenArtifact, &error)
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
    let entry = SealedGeneration::new(
        intent.source_generation,
        sequence_floor,
        intent.sequence_cutoff,
        intent.source_end_offset,
        intent.registry_generation,
        intent.source_end_offset,
        checksum.finish(),
    );
    validate_sealed_journal(&staging_path, entry, intent.store_id, limits, registry)?;
    injected_rotation_fault(26, ManifestIoOperation::Read)?;
    std::fs::rename(&staging_path, &target_path)
        .map_err(|error| manifest_io(ManifestIoOperation::Publish, &error))?;
    injected_rotation_fault(27, ManifestIoOperation::Publish)?;
    directory
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))?;
    injected_rotation_fault(28, ManifestIoOperation::SyncDirectory)?;
    validate_sealed_journal(&target_path, entry, intent.store_id, limits, registry)?;
    injected_rotation_fault(29, ManifestIoOperation::Read)?;
    Ok(entry)
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
            Err(error) => return Err(manifest_io(ManifestIoOperation::Publish, &error)),
        }
    }
    directory
        .sync_all()
        .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))
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
            Err(error) => return Err(manifest_io(ManifestIoOperation::Publish, &error)),
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
                manifest_io(ManifestIoOperation::OpenArtifact, &error)
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
        _ => 0,
    };
    if code == 0 {
        return Ok(());
    }
    if take_publish_fault(code) {
        let operation = match point {
            PublicationPoint::Write => ManifestIoOperation::Write,
            PublicationPoint::SyncArtifact => ManifestIoOperation::SyncArtifact,
            PublicationPoint::Readback => ManifestIoOperation::Read,
            PublicationPoint::Publish => ManifestIoOperation::Publish,
            PublicationPoint::SyncDirectory => ManifestIoOperation::SyncDirectory,
        };
        let error = std::io::Error::other("injected manifest publication failure");
        return Err(manifest_io(operation, &error));
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
    if take_publish_fault(code) {
        let error = std::io::Error::other("injected rotation publication failure");
        return Err(manifest_io(operation, &error));
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
        || bytes[116..124].iter().any(|byte| *byte != 0)
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
    if matches!(error, ActiveJournalError::Io(_)) {
        ManifestStoreError::Active(error)
    } else {
        ManifestStoreError::UnsupportedStoreFormat
    }
}

fn manifest_io(operation: ManifestIoOperation, error: &std::io::Error) -> ManifestStoreError {
    ManifestStoreError::Io(ManifestIoEvidence {
        operation,
        kind: error.kind(),
        raw_os_error: error.raw_os_error(),
    })
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
                    Err(ManifestStoreError::InterruptedPublication)
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
    fn retry_transition_refusal_after_journal_sync_terminally_faults_store() {
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
            "a retained key cannot be advanced as fresh after journal sync"
        );
        assert_eq!(
            store.bind(second.admission().envelope().clone()),
            Err(ManifestStoreError::Faulted)
        );
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
                    Err(ManifestStoreError::InterruptedPublication)
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
                        | ManifestStoreError::InvalidRetry)
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
            (116, 1, 1),
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
