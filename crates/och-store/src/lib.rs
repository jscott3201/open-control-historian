#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Bounded Journal V1 framing and one blocking active-journal owner.
//!
//! The crate encodes already-authorized [`och_core::CanonicalAdmission`] values
//! and decodes hostile bytes into non-authorizing inspection records. Its
//! [`ActiveJournal`] is the sole synchronous owner of one generation's
//! mechanical active artifacts. [`ManifestStore`] composes it with stable
//! locking, bounded registry/retry/catalog snapshots, immutable raw-Journal
//! sealing, successor rotation, and a manifest-backed durable cutoff. The crate
//! owns no runtime scheduling, final segment encoding, or query behavior.

mod active;
mod codec;
mod decoded;
mod error;
mod generation;
mod manifest;
mod retry;

#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod test_support;

pub use active::{
    ACTIVE_CHECKPOINT_FILE_NAME, ACTIVE_JOURNAL_FILE_NAME, ACTIVE_JOURNAL_GENERATION,
    ActiveJournal, ActiveJournalConfig, ActiveJournalError, ActiveJournalInspection,
    ActiveJournalLimits, ActiveJournalOpenMode, DurableCutoff, JournalIdentity,
    MAX_ACTIVE_JOURNAL_BYTES, MAX_ACTIVE_JOURNAL_RECORDS, MAX_STORE_DIRECTORY_BYTES,
    RecoveredAdmissionV1, StoreIoEvidence, StoreIoOperation,
};

pub use codec::{
    AppendSequenceV1, DecodeLimitsV1, JournalHeaderV1, JournalHeaderV2, PrepareAdmissionError,
    PreparedAdmissionV1, PreparedFrameV1, admission_frame_len_v1, decode_admission_frame_v1,
    encode_admission_frame_v1, encode_decoded_admission_frame_v1,
};
pub use decoded::{DecodedAdmissionV1, DecodedDeclarationV1, DecodedObservationLineageV1};
pub use error::JournalV1Error;
pub use generation::{
    GENERATION_CATALOG_STAGING_FILE_NAME, GenerationCatalogReference, GenerationCatalogSnapshot,
    GenerationInventory, MAX_SEALED_GENERATIONS, ROTATION_INTENT_FILE_NAME,
    SEALED_JOURNAL_STAGING_FILE_NAME, SealedGeneration,
};
pub use manifest::{
    MANIFEST_SLOT_0_FILE_NAME, MANIFEST_SLOT_1_FILE_NAME, MANIFEST_STAGING_FILE_NAME,
    MAX_PERSISTED_REGISTRY_REVISIONS, MAX_PERSISTED_REGISTRY_SERIES, MAX_REGISTRY_SNAPSHOT_BYTES,
    ManifestCommit, ManifestIoEvidence, ManifestIoOperation, ManifestStore, ManifestStoreConfig,
    ManifestStoreError, ManifestStoreInspection, REGISTRY_SLOT_0_FILE_NAME,
    REGISTRY_SLOT_1_FILE_NAME, REGISTRY_SLOT_2_FILE_NAME, REGISTRY_STAGING_FILE_NAME,
    RETRY_SLOT_0_FILE_NAME, RETRY_SLOT_1_FILE_NAME, RETRY_SLOT_2_FILE_NAME,
    RETRY_STAGING_FILE_NAME, RegistryPersistenceOptions, STORE_LOCK_FILE_NAME,
};
pub use retry::{
    MAX_PERSISTED_RETRY_ENTRIES, MAX_RETRY_STATE_BYTES, PendingRetryOutcome, RetryGuardEntry,
    RetryOptionsError, RetryPersistenceOptions, RetryReplayOutcome, RetryStateMatch,
    RetryStateReference, RetryStateSnapshot,
};

/// Journal V1 format version written in premanifest headers and every admission frame.
pub const JOURNAL_V1_VERSION: u16 = 1;
/// Manifest-required active-header version; Journal V1 frame bytes remain version one.
pub const ACTIVE_JOURNAL_HEADER_V2_VERSION: u16 = 2;
/// Fixed Journal V1 file-header length in bytes.
pub const JOURNAL_V1_HEADER_LEN: usize = 28;
/// Fixed admission-frame prefix length before its variable payload.
pub const JOURNAL_V1_FRAME_PREFIX_LEN: usize = 20;
/// CRC-32C trailer length in bytes.
pub const JOURNAL_V1_FRAME_CRC_LEN: usize = 4;
/// Hard maximum encoded admission payload accepted by Journal V1.
pub const MAX_ADMISSION_PAYLOAD_V1: usize = 8 * 1_024 * 1_024;
/// Exact eight-byte Journal V1 file-header magic.
pub const JOURNAL_V1_HEADER_MAGIC: [u8; 8] = *b"OCHJNL01";
/// Exact four-byte Journal V1 admission-frame magic.
pub const JOURNAL_V1_FRAME_MAGIC: [u8; 4] = *b"OCHF";
