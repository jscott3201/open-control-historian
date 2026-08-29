#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Bounded, dependency-light Journal V1 semantic framing.
//!
//! The crate encodes already-authorized [`och_core::CanonicalAdmission`] values
//! and decodes hostile bytes into non-authorizing inspection records. It owns no
//! filesystem, writer, synchronization, persistence, durability, registry, or
//! runtime behavior.

mod codec;
mod decoded;
mod error;

pub use codec::{
    AppendSequenceV1, DecodeLimitsV1, JournalHeaderV1, decode_admission_frame_v1,
    encode_admission_frame_v1, encode_decoded_admission_frame_v1,
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
