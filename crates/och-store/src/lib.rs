#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Bounded Journal V1 framing and one blocking active-journal owner.
//!
//! The crate encodes already-authorized [`och_core::CanonicalAdmission`] values
//! and decodes hostile bytes into non-authorizing inspection records. Its
//! [`ActiveJournal`] is the sole synchronous owner of the fixed pre-manifest
//! active artifacts and their mechanical durable high-water. The crate owns no
//! runtime, registry, manifest, rotation, or query behavior.

mod active;
mod codec;
mod decoded;
mod error;

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
    AppendSequenceV1, DecodeLimitsV1, JournalHeaderV1, PrepareAdmissionError, PreparedAdmissionV1,
    PreparedFrameV1, admission_frame_len_v1, decode_admission_frame_v1, encode_admission_frame_v1,
    encode_decoded_admission_frame_v1,
};
pub use decoded::{DecodedAdmissionV1, DecodedDeclarationV1, DecodedObservationLineageV1};
pub use error::JournalV1Error;

/// Journal V1 format version written in headers and admission frames.
pub const JOURNAL_V1_VERSION: u16 = 1;
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
