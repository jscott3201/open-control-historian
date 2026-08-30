//! Manifest-bound bounded recovery evidence and Recovery State V1 bytes.

use crate::codec::crc32c;
use crate::{DurableCutoff, JournalIdentity};
use och_core::StoreId;

/// Exact first reusable Recovery State V1 slot.
pub const RECOVERY_SLOT_0_FILE_NAME: &str = "recovery-state-v1-slot-0.och";
/// Exact second reusable Recovery State V1 slot.
pub const RECOVERY_SLOT_1_FILE_NAME: &str = "recovery-state-v1-slot-1.och";
/// Exact third reusable Recovery State V1 slot.
pub const RECOVERY_SLOT_2_FILE_NAME: &str = "recovery-state-v1-slot-2.och";
/// Exact fixed Recovery State V1 staging artifact.
pub const RECOVERY_STAGING_FILE_NAME: &str = "recovery-state-v1.staging";
/// Exact fixed Recovery State V1 artifact length.
pub const RECOVERY_STATE_V1_LEN: usize = 96;

pub(crate) const RECOVERY_SLOT_NAMES: [&str; 3] = [
    RECOVERY_SLOT_0_FILE_NAME,
    RECOVERY_SLOT_1_FILE_NAME,
    RECOVERY_SLOT_2_FILE_NAME,
];
const RECOVERY_MAGIC: [u8; 8] = *b"OCHRCV01";
const RECOVERY_VERSION: u16 = 1;
const RECOVERY_STATE_V1_LEN_U16: u16 = 96;

/// Bounded sanitized startup/recovery classification.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryClassification {
    /// No recovery action was required for a successfully proven root.
    Clean,
    /// Bytes strictly after the newest proven manifest cutoff were recoverable.
    CommittedRootSuffix,
    /// A required manifest slot was absent, so newest authority was unprovable.
    MissingManifest,
    /// A manifest slot was present but corrupt or semantically invalid.
    CorruptManifest,
    /// A manifest or active format version is not supported by this writer.
    UnsupportedFormat,
    /// Registry authority was missing, corrupt, or mismatched.
    InvalidRegistry,
    /// Durable retry authority was missing, corrupt, or mismatched.
    InvalidRetry,
    /// Catalog, seal metadata, or generation authority was invalid.
    InvalidGeneration,
    /// Configured identity differs from durable evidence; stale restore remains closed.
    IdentityMismatchOrStaleRestore,
    /// Corruption intersects the manifest-committed active prefix.
    InteriorJournalCorruption,
    /// Publication or recognized inventory evidence is ambiguous.
    AmbiguousPublication,
    /// Filesystem I/O, permission, or path handling refused startup.
    IoOrPathRefusal,
    /// Configuration or another retained writer refused startup.
    OpenRefusal,
}

impl RecoveryClassification {
    const fn code(self) -> Option<u8> {
        match self {
            Self::CommittedRootSuffix => Some(1),
            _ => None,
        }
    }

    const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::CommittedRootSuffix),
            _ => None,
        }
    }
}

/// Bounded sanitized recovery action.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryAction {
    /// No durable artifact was changed.
    None,
    /// A proven active-journal suffix was removed without adoption.
    RemovedActiveSuffix,
}

impl RecoveryAction {
    const fn code(self) -> Option<u8> {
        match self {
            Self::RemovedActiveSuffix => Some(1),
            Self::None => None,
        }
    }

    const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::RemovedActiveSuffix),
            _ => None,
        }
    }
}

/// Immutable path/content-free evidence for the latest committed recovery.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryReport {
    classification: RecoveryClassification,
    action: RecoveryAction,
    source_manifest_generation: u64,
    cutoff: DurableCutoff,
    removed_bytes: u64,
    operation_count: u16,
}

impl RecoveryReport {
    pub(crate) const fn committed_suffix(
        source_manifest_generation: u64,
        cutoff: DurableCutoff,
        removed_bytes: u64,
        operation_count: u16,
    ) -> Self {
        Self {
            classification: RecoveryClassification::CommittedRootSuffix,
            action: RecoveryAction::RemovedActiveSuffix,
            source_manifest_generation,
            cutoff,
            removed_bytes,
            operation_count,
        }
    }

    /// Returns the closed recovery classification.
    #[must_use]
    pub const fn classification(self) -> RecoveryClassification {
        self.classification
    }

    /// Returns the closed action taken before this report was committed.
    #[must_use]
    pub const fn action(self) -> RecoveryAction {
        self.action
    }

    /// Returns the manifest generation that proved the repaired root.
    #[must_use]
    pub const fn source_manifest_generation(self) -> u64 {
        self.source_manifest_generation
    }

    /// Returns the exact root cutoff retained by recovery.
    #[must_use]
    pub const fn durable_cutoff(self) -> DurableCutoff {
        self.cutoff
    }

    /// Returns the exact number of bytes removed strictly after the cutoff.
    #[must_use]
    pub const fn removed_bytes(self) -> u64 {
        self.removed_bytes
    }

    /// Returns the bounded count of synchronized artifact operations.
    #[must_use]
    pub const fn operation_count(self) -> u16 {
        self.operation_count
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryArtifactReference {
    pub(crate) slot: u8,
    pub(crate) generation: u64,
    pub(crate) length: u64,
    pub(crate) checksum: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryState {
    pub(crate) store_id: StoreId,
    pub(crate) generation: u64,
    pub(crate) report: RecoveryReport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryCodecError {
    Invalid,
    StoreMismatch,
}

pub(crate) fn encode_recovery_state(state: RecoveryState) -> [u8; RECOVERY_STATE_V1_LEN] {
    let mut bytes = [0_u8; RECOVERY_STATE_V1_LEN];
    bytes[..8].copy_from_slice(&RECOVERY_MAGIC);
    bytes[8..10].copy_from_slice(&RECOVERY_VERSION.to_be_bytes());
    bytes[10..12].copy_from_slice(&RECOVERY_STATE_V1_LEN_U16.to_be_bytes());
    bytes[12..28].copy_from_slice(state.store_id.as_bytes());
    bytes[28..36].copy_from_slice(&state.generation.to_be_bytes());
    bytes[36..44].copy_from_slice(&state.report.source_manifest_generation.to_be_bytes());
    bytes[44..52].copy_from_slice(&state.report.cutoff.journal().generation().to_be_bytes());
    bytes[52..60].copy_from_slice(&state.report.cutoff.checkpoint_generation().to_be_bytes());
    bytes[60..68].copy_from_slice(&state.report.cutoff.append_sequence().to_be_bytes());
    bytes[68..76].copy_from_slice(&state.report.cutoff.end_offset().to_be_bytes());
    bytes[76..84].copy_from_slice(&state.report.removed_bytes.to_be_bytes());
    bytes[84] = state.report.classification.code().unwrap_or_default();
    bytes[85] = state.report.action.code().unwrap_or_default();
    bytes[86..88].copy_from_slice(&state.report.operation_count.to_be_bytes());
    let checksum = crc32c(&bytes[..92]);
    bytes[92..96].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

pub(crate) fn decode_recovery_state(
    bytes: &[u8],
    expected_store: StoreId,
) -> Result<RecoveryState, RecoveryCodecError> {
    if bytes.len() != RECOVERY_STATE_V1_LEN
        || bytes[..8] != RECOVERY_MAGIC
        || u16::from_be_bytes(bytes[8..10].try_into().unwrap_or_default()) != RECOVERY_VERSION
        || u16::from_be_bytes(bytes[10..12].try_into().unwrap_or_default())
            != RECOVERY_STATE_V1_LEN_U16
        || bytes[88..92].iter().any(|byte| *byte != 0)
        || crc32c(&bytes[..92]) != u32::from_be_bytes(bytes[92..96].try_into().unwrap_or_default())
    {
        return Err(RecoveryCodecError::Invalid);
    }
    let store_id = StoreId::from_bytes(bytes[12..28].try_into().unwrap_or_default())
        .map_err(|_| RecoveryCodecError::Invalid)?;
    if store_id != expected_store {
        return Err(RecoveryCodecError::StoreMismatch);
    }
    let generation = u64::from_be_bytes(bytes[28..36].try_into().unwrap_or_default());
    let source_manifest_generation =
        u64::from_be_bytes(bytes[36..44].try_into().unwrap_or_default());
    let journal_generation = u64::from_be_bytes(bytes[44..52].try_into().unwrap_or_default());
    let checkpoint_generation = u64::from_be_bytes(bytes[52..60].try_into().unwrap_or_default());
    let append_sequence = u64::from_be_bytes(bytes[60..68].try_into().unwrap_or_default());
    let end_offset = u64::from_be_bytes(bytes[68..76].try_into().unwrap_or_default());
    let removed_bytes = u64::from_be_bytes(bytes[76..84].try_into().unwrap_or_default());
    let classification =
        RecoveryClassification::from_code(bytes[84]).ok_or(RecoveryCodecError::Invalid)?;
    let action = RecoveryAction::from_code(bytes[85]).ok_or(RecoveryCodecError::Invalid)?;
    let operation_count = u16::from_be_bytes(bytes[86..88].try_into().unwrap_or_default());
    if generation == 0
        || source_manifest_generation == 0
        || journal_generation == 0
        || checkpoint_generation == 0
        || end_offset < crate::JOURNAL_V1_HEADER_LEN as u64
        || removed_bytes == 0
        || operation_count == 0
        || classification != RecoveryClassification::CommittedRootSuffix
        || action != RecoveryAction::RemovedActiveSuffix
    {
        return Err(RecoveryCodecError::Invalid);
    }
    let cutoff = DurableCutoff::from_recovery(
        JournalIdentity::from_recovery(store_id, journal_generation),
        checkpoint_generation,
        append_sequence,
        end_offset,
    );
    let state = RecoveryState {
        store_id,
        generation,
        report: RecoveryReport {
            classification,
            action,
            source_manifest_generation,
            cutoff,
            removed_bytes,
            operation_count,
        },
    };
    if encode_recovery_state(state) != bytes {
        return Err(RecoveryCodecError::Invalid);
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;

    fn state() -> RecoveryState {
        let store_id = test_support::store_id(1);
        RecoveryState {
            store_id,
            generation: 1,
            report: RecoveryReport::committed_suffix(
                7,
                DurableCutoff::from_manifest(store_id, 2, 4, 9, 700),
                31,
                2,
            ),
        }
    }

    #[test]
    fn recovery_state_v1_round_trips_and_refuses_hostile_fields() {
        let state = state();
        let canonical = encode_recovery_state(state);
        assert_eq!(decode_recovery_state(&canonical, state.store_id), Ok(state));
        assert!(decode_recovery_state(&canonical[..95], state.store_id).is_err());
        let mut trailing = canonical.to_vec();
        trailing.push(0);
        assert!(decode_recovery_state(&trailing, state.store_id).is_err());

        for (offset, length) in [(8_usize, 2_usize), (10, 2), (28, 8), (84, 1)] {
            let mut hostile = canonical;
            hostile[offset..offset + length].fill(0);
            let checksum = crc32c(&hostile[..92]);
            hostile[92..96].copy_from_slice(&checksum.to_be_bytes());
            assert!(decode_recovery_state(&hostile, state.store_id).is_err());
        }
        let mut reserved = canonical;
        reserved[88] = 1;
        let checksum = crc32c(&reserved[..92]);
        reserved[92..96].copy_from_slice(&checksum.to_be_bytes());
        assert!(decode_recovery_state(&reserved, state.store_id).is_err());
        let mut checksum = canonical;
        checksum[95] ^= 1;
        assert!(decode_recovery_state(&checksum, state.store_id).is_err());
        assert_eq!(
            decode_recovery_state(&canonical, test_support::store_id(2)),
            Err(RecoveryCodecError::StoreMismatch)
        );
    }
}
