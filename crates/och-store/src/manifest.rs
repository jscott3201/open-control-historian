//! Bounded manifest-rooted active journal and canonical registry persistence.

use crate::active::{ActiveJournal, ActiveJournalConfig};
use crate::codec::{
    Cursor, Encoder, crc32c, decode_declaration, decode_declaration_evidence, encode_declaration,
    encode_declaration_evidence,
};
use crate::{
    ACTIVE_CHECKPOINT_FILE_NAME, ACTIVE_JOURNAL_FILE_NAME, ACTIVE_JOURNAL_GENERATION,
    ACTIVE_JOURNAL_HEADER_V2_VERSION, ActiveJournalError, ActiveJournalInspection,
    ActiveJournalLimits, ActiveJournalOpenMode, DurableCutoff, JournalV1Error, PreparedFrameV1,
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
#[cfg(test)]
use std::sync::atomic::{AtomicU8, Ordering};

/// Exact never-renamed store-level writer lock artifact.
pub const STORE_LOCK_FILE_NAME: &str = "store-v1.lock";
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
/// Hard maximum persisted registry series, including tombstones.
pub const MAX_PERSISTED_REGISTRY_SERIES: usize = 4_096;
/// Hard maximum persisted declaration revisions across one registry.
pub const MAX_PERSISTED_REGISTRY_REVISIONS: usize = 16_384;
/// Hard maximum bytes in one registry snapshot artifact.
pub const MAX_REGISTRY_SNAPSHOT_BYTES: usize = 64 * 1_024 * 1_024;

const MANIFEST_MAGIC: [u8; 8] = *b"OCHMAN01";
const MANIFEST_VERSION: u16 = 1;
const MANIFEST_LEN: usize = 128;
const MANIFEST_LEN_U16: u16 = 128;
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
const MAX_INVENTORY_ENTRIES: usize = 10;

#[cfg(test)]
static PUBLISH_FAULT: AtomicU8 = AtomicU8::new(0);

/// Explicit bounded canonical registry persistence and bootstrap input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryPersistenceOptions {
    limits: SeriesRegistryLimits,
    bootstrap: Option<SeriesRegistrySnapshot>,
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
        Ok(Self {
            limits,
            bootstrap: None,
        })
    }

    /// Supplies the explicit complete snapshot required by a nonempty
    /// pre-manifest store.
    ///
    /// # Errors
    ///
    /// Refuses a snapshot whose limits differ from the configured limits or
    /// exceed the hard persistence bounds.
    pub fn with_bootstrap_snapshot(
        mut self,
        snapshot: SeriesRegistrySnapshot,
    ) -> Result<Self, ManifestStoreError> {
        if snapshot.limits() != self.limits
            || snapshot.series().len() > MAX_PERSISTED_REGISTRY_SERIES
            || snapshot.declaration_revision_count() > MAX_PERSISTED_REGISTRY_REVISIONS
        {
            return Err(ManifestStoreError::InvalidOptions);
        }
        self.bootstrap = Some(snapshot);
        Ok(self)
    }

    /// Returns configured canonical registry limits.
    #[must_use]
    pub const fn limits(&self) -> SeriesRegistryLimits {
        self.limits
    }

    /// Returns the optional explicit pre-manifest bootstrap snapshot.
    #[must_use]
    pub const fn bootstrap_snapshot(&self) -> Option<&SeriesRegistrySnapshot> {
        self.bootstrap.as_ref()
    }
}

/// Validated manifest-rooted blocking store configuration.
pub struct ManifestStoreConfig {
    directory: PathBuf,
    store_id: StoreId,
    mode: ActiveJournalOpenMode,
    journal_limits: ActiveJournalLimits,
    registry: RegistryPersistenceOptions,
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
    ) -> Result<Self, ManifestStoreError> {
        ActiveJournalConfig::new(directory.clone(), store_id, mode, journal_limits)
            .map_err(ManifestStoreError::Active)?;
        if registry.limits.max_series() > MAX_PERSISTED_REGISTRY_SERIES
            || registry.limits.max_declaration_revisions() > MAX_PERSISTED_REGISTRY_REVISIONS
            || registry
                .bootstrap
                .as_ref()
                .is_some_and(|snapshot| snapshot.store_id() != store_id)
        {
            return Err(ManifestStoreError::InvalidOptions);
        }
        Ok(Self {
            directory,
            store_id,
            mode,
            journal_limits,
            registry,
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

/// Closed manifest, registry, bootstrap, publication, and lifecycle refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestStoreError {
    /// Configuration or a hard bound is invalid.
    InvalidOptions,
    /// Another process or handle retains the stable store lock.
    AlreadyOpen,
    /// Store inventory contains unknown, excessive, or unsafe evidence.
    InvalidInventory,
    /// A manifest slot is invalid, ambiguous, or inconsistent.
    InvalidManifest,
    /// A registry snapshot is invalid, ambiguous, or inconsistent.
    InvalidRegistry,
    /// The configured store identity differs from durable evidence.
    StoreMismatch,
    /// A nonempty pre-manifest journal lacks an explicit registry snapshot.
    BootstrapSnapshotRequired,
    /// Explicit bootstrap state cannot interpret every recovered declaration.
    BootstrapSnapshotMismatch,
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
            Self::AlreadyOpen => "manifest store is already open",
            Self::InvalidInventory => "invalid manifest store inventory",
            Self::InvalidManifest => "invalid manifest evidence",
            Self::InvalidRegistry => "invalid registry evidence",
            Self::StoreMismatch => "manifest store identity mismatch",
            Self::BootstrapSnapshotRequired => "registry bootstrap snapshot required",
            Self::BootstrapSnapshotMismatch => "registry bootstrap snapshot mismatch",
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
}

/// Sanitized bounded manifest-rooted store inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestStoreInspection {
    active: ActiveJournalInspection,
    committed: ManifestCommit,
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
}

/// Sole blocking owner of stable locking, active journal, registry, and manifests.
pub struct ManifestStore {
    directory_path: PathBuf,
    directory: File,
    _store_lock: File,
    journal: ActiveJournal,
    registry: SeriesRegistry,
    manifest_slots: [Option<ManifestRecord>; 2],
    current_slot: usize,
    current: ManifestRecord,
    faulted: bool,
}

impl ManifestStore {
    /// Creates, opens, or explicitly bootstraps one manifest-rooted store.
    ///
    /// Stable lock acquisition precedes manifest selection and active mutation.
    /// A nonempty pre-manifest store requires an exact caller-supplied snapshot.
    ///
    /// # Errors
    ///
    /// Returns a bounded path-free refusal for lock, inventory, bootstrap,
    /// format, identity, cutoff, registry, or I/O failure.
    pub fn open(config: ManifestStoreConfig) -> Result<Self, ManifestStoreError> {
        let directory = open_directory(&config.directory)?;
        let preflight = inspect_inventory(&config.directory)?;
        if preflight.staging {
            return Err(ManifestStoreError::InterruptedPublication);
        }
        let lock_path = config.directory.join(STORE_LOCK_FILE_NAME);
        let store_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| manifest_io(ManifestIoOperation::OpenArtifact, &error))?;
        lock_store(&store_lock)?;
        if !preflight.store_lock {
            directory
                .sync_all()
                .map_err(|error| manifest_io(ManifestIoOperation::SyncDirectory, &error))?;
        }
        let inventory = inspect_inventory(&config.directory)?;
        if inventory.staging {
            return Err(ManifestStoreError::InterruptedPublication);
        }
        let manifest_slots = read_manifest_slots(&config.directory, config.store_id)?;
        if let Some((current_slot, current)) = select_current_manifest(manifest_slots)? {
            if config.mode != ActiveJournalOpenMode::OpenExisting {
                return Err(ManifestStoreError::InvalidInventory);
            }
            Self::open_committed(
                config,
                directory,
                store_lock,
                manifest_slots,
                current_slot,
                current,
            )
        } else {
            Self::bootstrap(config, directory, store_lock, inventory, manifest_slots)
        }
    }

    fn open_committed(
        config: ManifestStoreConfig,
        directory: File,
        store_lock: File,
        manifest_slots: [Option<ManifestRecord>; 2],
        current_slot: usize,
        current: ManifestRecord,
    ) -> Result<Self, ManifestStoreError> {
        let registry =
            read_referenced_registry(&config.directory, current.registry, config.store_id)?;
        validate_registry_inventory(
            &config.directory,
            manifest_slots,
            current.registry,
            config.store_id,
        )?;
        let active_config = ActiveJournalConfig::new(
            config.directory.clone(),
            config.store_id,
            ActiveJournalOpenMode::OpenExisting,
            config.journal_limits,
        )
        .map_err(ManifestStoreError::Active)?
        .manifest_existing();
        let journal = ActiveJournal::open(active_config)?;
        if journal.header_version() != ACTIVE_JOURNAL_HEADER_V2_VERSION
            || journal.durable_cutoff() != current.cutoff
        {
            return Err(ManifestStoreError::InvalidManifest);
        }
        validate_recovered_declarations(&registry, journal.recovered_records())
            .map_err(|_| ManifestStoreError::InvalidRegistry)?;
        Ok(Self {
            directory_path: config.directory,
            directory,
            _store_lock: store_lock,
            journal,
            registry,
            manifest_slots,
            current_slot,
            current,
            faulted: false,
        })
    }

    fn bootstrap(
        config: ManifestStoreConfig,
        directory: File,
        store_lock: File,
        inventory: Inventory,
        mut manifest_slots: [Option<ManifestRecord>; 2],
    ) -> Result<Self, ManifestStoreError> {
        let active_config = ActiveJournalConfig::new(
            config.directory.clone(),
            config.store_id,
            config.mode,
            config.journal_limits,
        )
        .map_err(ManifestStoreError::Active)?;
        let active_config = match config.mode {
            ActiveJournalOpenMode::CreateNew => active_config.manifest_create(),
            ActiveJournalOpenMode::OpenExisting => active_config.manifest_bootstrap(),
        };
        let mut journal = ActiveJournal::open(active_config)?;
        let nonempty = !journal.recovered_records().is_empty();
        let registry = if nonempty {
            let snapshot = config
                .registry
                .bootstrap
                .as_ref()
                .ok_or(ManifestStoreError::BootstrapSnapshotRequired)?;
            let registry = restore_snapshot(snapshot)?;
            validate_recovered_declarations(&registry, journal.recovered_records())
                .map_err(|_| ManifestStoreError::BootstrapSnapshotMismatch)?;
            registry
        } else {
            if config
                .registry
                .bootstrap
                .as_ref()
                .is_some_and(|snapshot| !snapshot.series().is_empty())
            {
                return Err(ManifestStoreError::BootstrapSnapshotMismatch);
            }
            SeriesRegistry::new(config.store_id, config.registry.limits)
        };
        journal.upgrade_to_manifest_header()?;
        let mut store = Self {
            directory_path: config.directory,
            directory,
            _store_lock: store_lock,
            journal,
            registry,
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
            },
            faulted: false,
        };
        let registry_reference = if inventory.registry_slots == 0 {
            store.publish_registry_snapshot(1, &store.registry.snapshot())?
        } else {
            read_interrupted_genesis_registry(&store.directory_path, &store.registry.snapshot())?
        };
        let record = ManifestRecord {
            generation: 1,
            registry: registry_reference,
            cutoff: store.journal.durable_cutoff(),
        };
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

    /// Validates historical declaration authority and appends one frame.
    ///
    /// # Errors
    ///
    /// Unknown or altered historical declarations refuse before journal bytes.
    pub fn append(&mut self, frame: &PreparedFrameV1) -> Result<u64, ManifestStoreError> {
        self.ensure_usable()?;
        let declaration = frame.admission().declaration();
        if self
            .registry
            .resolve(declaration.series_id(), declaration.revision())
            != Some(declaration)
        {
            return Err(ManifestStoreError::HistoricalDeclarationMismatch);
        }
        self.journal
            .append(frame)
            .map_err(ManifestStoreError::Active)
    }

    /// Synchronizes the journal/checkpoint and commits their exact cutoff in a
    /// new manifest before returning.
    ///
    /// # Errors
    ///
    /// A publication failure returns no committed proof and terminally faults
    /// this open authority.
    pub fn sync_pending(&mut self) -> Result<ManifestCommit, ManifestStoreError> {
        self.ensure_usable()?;
        let cutoff = self.journal.sync_pending()?;
        if cutoff == self.current.cutoff {
            return Ok(self.commit());
        }
        let generation = self
            .current
            .generation
            .checked_add(1)
            .ok_or(ManifestStoreError::GenerationExhausted)?;
        let next = ManifestRecord {
            generation,
            registry: self.current.registry,
            cutoff,
        };
        self.publish_and_adopt_manifest(next)
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
        self.manifest_slots[slot] = Some(record);
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
        ManifestCommit {
            manifest_generation: self.current.generation,
            registry_generation: self.current.registry.generation,
            registry_slot: self.current.registry.slot,
            durable_cutoff: self.current.cutoff,
        }
    }

    fn ensure_usable(&self) -> Result<(), ManifestStoreError> {
        if self.faulted {
            Err(ManifestStoreError::Faulted)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy)]
struct Inventory {
    staging: bool,
    registry_slots: usize,
    store_lock: bool,
}

fn inspect_inventory(directory: &Path) -> Result<Inventory, ManifestStoreError> {
    let mut count = 0_usize;
    let mut staging = false;
    let mut registry_slots = 0_usize;
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
        if name == STORE_LOCK_FILE_NAME {
            store_lock = true;
        } else if name == MANIFEST_STAGING_FILE_NAME || name == REGISTRY_STAGING_FILE_NAME {
            staging = true;
        } else if REGISTRY_SLOT_NAMES.contains(&name) {
            registry_slots += 1;
        } else if name != STORE_LOCK_FILE_NAME
            && name != ACTIVE_JOURNAL_FILE_NAME
            && name != ACTIVE_CHECKPOINT_FILE_NAME
            && !MANIFEST_SLOT_NAMES.contains(&name)
        {
            return Err(ManifestStoreError::InvalidInventory);
        }
    }
    Ok(Inventory {
        staging,
        registry_slots,
        store_lock,
    })
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
            if newer.generation != older.generation.saturating_add(1) {
                return Err(ManifestStoreError::InvalidManifest);
            }
            Ok(Some((newer_index, newer)))
        }
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
        if !referenced && decoded.reference.generation > current.generation.saturating_add(1) {
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
            return Err(ManifestStoreError::BootstrapSnapshotMismatch);
        }
        found = Some(decoded.reference);
    }
    found.ok_or(ManifestStoreError::InvalidRegistry)
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
    Publish = 3,
    SyncDirectory = 4,
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
    let family = if staging_name == REGISTRY_STAGING_FILE_NAME {
        0_u8
    } else {
        4_u8
    };
    let code = family + point as u8;
    if PUBLISH_FAULT
        .compare_exchange(code, 0, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        let operation = match point {
            PublicationPoint::Write => ManifestIoOperation::Write,
            PublicationPoint::SyncArtifact => ManifestIoOperation::SyncArtifact,
            PublicationPoint::Publish => ManifestIoOperation::Publish,
            PublicationPoint::SyncDirectory => ManifestIoOperation::SyncDirectory,
        };
        let error = std::io::Error::other("injected manifest publication failure");
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

fn encode_manifest(record: ManifestRecord) -> [u8; MANIFEST_LEN] {
    let mut bytes = [0_u8; MANIFEST_LEN];
    bytes[..8].copy_from_slice(&MANIFEST_MAGIC);
    bytes[8..10].copy_from_slice(&MANIFEST_VERSION.to_be_bytes());
    bytes[10..12].copy_from_slice(&MANIFEST_LEN_U16.to_be_bytes());
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
    let checksum = crc32c(&bytes[..124]);
    bytes[124..128].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

fn decode_manifest(bytes: &[u8], store_id: StoreId) -> Result<ManifestRecord, ManifestStoreError> {
    if bytes.len() != MANIFEST_LEN
        || bytes[..8] != MANIFEST_MAGIC
        || u16::from_be_bytes(bytes[8..10].try_into().unwrap_or_default()) != MANIFEST_VERSION
        || u16::from_be_bytes(bytes[10..12].try_into().unwrap_or_default()) != MANIFEST_LEN_U16
        || bytes[69..72].iter().any(|byte| *byte != 0)
        || bytes[92..124].iter().any(|byte| *byte != 0)
        || crc32c(&bytes[..124])
            != u32::from_be_bytes(bytes[124..128].try_into().unwrap_or_default())
    {
        return Err(ManifestStoreError::InvalidManifest);
    }
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
        || journal_generation != ACTIVE_JOURNAL_GENERATION
        || checkpoint_generation == 0
        || registry.slot >= 3
        || registry.generation == 0
        || registry.length == 0
        || usize::try_from(registry.length)
            .map_or(true, |length| length > MAX_REGISTRY_SNAPSHOT_BYTES)
        || (append_sequence == 0) != (end_offset == crate::JOURNAL_V1_HEADER_LEN as u64)
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
    use crate::test_support;
    use std::fs;
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
        )
        .expect("fault store config")
    }

    #[test]
    fn every_registry_and_manifest_publication_boundary_fails_stop_without_false_success() {
        let cases = [
            (1, ManifestIoOperation::Write, false, false),
            (2, ManifestIoOperation::SyncArtifact, false, false),
            (3, ManifestIoOperation::Publish, false, false),
            (4, ManifestIoOperation::SyncDirectory, true, false),
            (5, ManifestIoOperation::Write, false, true),
            (6, ManifestIoOperation::SyncArtifact, false, true),
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
            PUBLISH_FAULT.store(code, Ordering::SeqCst);
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
    fn manifest_parser_refuses_hostile_version_bounds_reserved_scope_and_checksum() {
        let store_id = test_support::store_id(1);
        let record = ManifestRecord {
            generation: 1,
            registry: RegistryReference {
                slot: 0,
                generation: 1,
                length: 68,
                checksum: 7,
            },
            cutoff: genesis_placeholder(store_id),
        };
        let canonical = encode_manifest(record);
        assert_eq!(decode_manifest(&canonical, store_id), Ok(record));

        for offset in [9_usize, 69, 92, 127] {
            let mut hostile = canonical;
            hostile[offset] ^= 0xff;
            if offset != 127 {
                let checksum = crc32c(&hostile[..124]);
                hostile[124..].copy_from_slice(&checksum.to_be_bytes());
            }
            assert!(matches!(
                decode_manifest(&hostile, store_id),
                Err(ManifestStoreError::InvalidManifest)
            ));
        }
        for (range, value) in [
            (28..36, 0_u64),
            (72..80, 0_u64),
            (80..88, (MAX_REGISTRY_SNAPSHOT_BYTES as u64) + 1),
        ] {
            let mut hostile = canonical;
            hostile[range].copy_from_slice(&value.to_be_bytes());
            let checksum = crc32c(&hostile[..124]);
            hostile[124..].copy_from_slice(&checksum.to_be_bytes());
            assert!(matches!(
                decode_manifest(&hostile, store_id),
                Err(ManifestStoreError::InvalidManifest)
            ));
        }
        let mut slot = canonical;
        slot[68] = 3;
        let checksum = crc32c(&slot[..124]);
        slot[124..].copy_from_slice(&checksum.to_be_bytes());
        assert!(matches!(
            decode_manifest(&slot, store_id),
            Err(ManifestStoreError::InvalidManifest)
        ));
        assert!(matches!(
            decode_manifest(&canonical, test_support::store_id(2)),
            Err(ManifestStoreError::StoreMismatch)
        ));
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
