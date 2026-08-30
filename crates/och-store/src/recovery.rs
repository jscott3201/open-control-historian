//! Fixed current-only Recovery State V1 evidence.

use crate::codec::crc32c;
use crate::{DurableCutoff, JOURNAL_V1_HEADER_LEN};
use och_core::StoreId;

/// Exact first reusable Recovery State V1 slot.
pub const RECOVERY_SLOT_0_FILE_NAME: &str = "recovery-state-v1-slot-0.och";
/// Exact second reusable Recovery State V1 slot.
pub const RECOVERY_SLOT_1_FILE_NAME: &str = "recovery-state-v1-slot-1.och";
/// Exact third reusable Recovery State V1 slot.
pub const RECOVERY_SLOT_2_FILE_NAME: &str = "recovery-state-v1-slot-2.och";
/// Exact fixed Recovery State V1 staging artifact.
pub const RECOVERY_STAGING_FILE_NAME: &str = "recovery-state-v1.staging";
/// Exact Recovery State V1 magic.
pub const RECOVERY_STATE_MAGIC: [u8; 8] = *b"OCHRCV01";
/// Current and sole Recovery State version.
pub const RECOVERY_STATE_VERSION: u16 = 1;
/// Exact Recovery State V1 artifact length.
pub const RECOVERY_STATE_LEN: usize = 128;

const RECOVERY_STATE_LEN_U16: u16 = 128;

pub(crate) const RECOVERY_SLOT_NAMES: [&str; 3] = [
    RECOVERY_SLOT_0_FILE_NAME,
    RECOVERY_SLOT_1_FILE_NAME,
    RECOVERY_SLOT_2_FILE_NAME,
];

/// Closed classification of the terminal suffix removed by current-V1 recovery.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryClassification {
    /// Fewer than the fixed Journal V1 frame-prefix bytes remained.
    ShortFramePrefix,
    /// Exactly one complete but structurally invalid frame prefix remained.
    InvalidFramePrefix,
    /// A valid prefix declared a frame extending beyond end of file.
    TruncatedDeclaredFrame,
    /// One complete frame ending exactly at EOF failed canonical decode.
    InvalidCompleteFrame,
}

impl RecoveryClassification {
    const fn tag(self) -> u8 {
        match self {
            Self::ShortFramePrefix => 1,
            Self::InvalidFramePrefix => 2,
            Self::TruncatedDeclaredFrame => 3,
            Self::InvalidCompleteFrame => 4,
        }
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::ShortFramePrefix),
            2 => Some(Self::InvalidFramePrefix),
            3 => Some(Self::TruncatedDeclaredFrame),
            4 => Some(Self::InvalidCompleteFrame),
            _ => None,
        }
    }
}

/// Closed action performed by current-V1 automatic recovery.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryAction {
    /// Remove only the proven suffix beyond the committed manifest cutoff.
    TruncateToCommittedRoot,
}

impl RecoveryAction {
    const fn tag(self) -> u8 {
        match self {
            Self::TruncateToCommittedRoot => 1,
        }
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::TruncateToCommittedRoot),
            _ => None,
        }
    }
}

/// Immutable path- and content-free evidence for the latest committed recovery event.
///
/// Presence means this event is retained by the current manifest. It does not
/// imply that recovery occurred during the current open.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryReport {
    store_id: StoreId,
    report_generation: u64,
    source_manifest_generation: u64,
    committing_manifest_generation: u64,
    source_manifest_checksum: u32,
    active_generation: u64,
    sequence_floor: u64,
    checkpoint_generation: u64,
    append_sequence: u64,
    committed_end_offset: u64,
    original_journal_length: u64,
    removed_bytes: u64,
    classification: RecoveryClassification,
    action: RecoveryAction,
}

impl RecoveryReport {
    /// Returns the positive Recovery State generation.
    #[must_use]
    pub const fn report_generation(self) -> u64 {
        self.report_generation
    }

    /// Returns the manifest generation whose cutoff was recovered.
    #[must_use]
    pub const fn source_manifest_generation(self) -> u64 {
        self.source_manifest_generation
    }

    /// Returns the manifest generation that committed this report.
    #[must_use]
    pub const fn committing_manifest_generation(self) -> u64 {
        self.committing_manifest_generation
    }

    /// Returns the active journal generation recovered by this event.
    #[must_use]
    pub const fn active_generation(self) -> u64 {
        self.active_generation
    }

    /// Returns the active generation's exclusive append-sequence floor.
    #[must_use]
    pub const fn active_sequence_floor(self) -> u64 {
        self.sequence_floor
    }

    /// Returns the checkpoint generation retained unchanged by recovery.
    #[must_use]
    pub const fn checkpoint_generation(self) -> u64 {
        self.checkpoint_generation
    }

    /// Returns the committed inclusive append-sequence cutoff.
    #[must_use]
    pub const fn append_sequence(self) -> u64 {
        self.append_sequence
    }

    /// Returns the committed active-journal end offset.
    #[must_use]
    pub const fn committed_end_offset(self) -> u64 {
        self.committed_end_offset
    }

    /// Returns the active-journal length before the suffix was removed.
    #[must_use]
    pub const fn original_journal_length(self) -> u64 {
        self.original_journal_length
    }

    /// Returns the positive number of bytes removed.
    #[must_use]
    pub const fn removed_bytes(self) -> u64 {
        self.removed_bytes
    }

    /// Returns the closed terminal-suffix classification.
    #[must_use]
    pub const fn classification(self) -> RecoveryClassification {
        self.classification
    }

    /// Returns the closed recovery action.
    #[must_use]
    pub const fn action(self) -> RecoveryAction {
        self.action
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store_id: StoreId,
        report_generation: u64,
        source_manifest_generation: u64,
        source_manifest_checksum: u32,
        cutoff: DurableCutoff,
        sequence_floor: u64,
        original_journal_length: u64,
        classification: RecoveryClassification,
    ) -> Result<Self, RecoveryCodecError> {
        let committing_manifest_generation = source_manifest_generation
            .checked_add(1)
            .ok_or(RecoveryCodecError::Invalid)?;
        let removed_bytes = original_journal_length
            .checked_sub(cutoff.end_offset())
            .filter(|removed| *removed > 0)
            .ok_or(RecoveryCodecError::Invalid)?;
        let report = Self {
            store_id,
            report_generation,
            source_manifest_generation,
            committing_manifest_generation,
            source_manifest_checksum,
            active_generation: cutoff.journal().generation(),
            sequence_floor,
            checkpoint_generation: cutoff.checkpoint_generation(),
            append_sequence: cutoff.append_sequence(),
            committed_end_offset: cutoff.end_offset(),
            original_journal_length,
            removed_bytes,
            classification,
            action: RecoveryAction::TruncateToCommittedRoot,
        };
        validate_report(report)?;
        Ok(report)
    }

    pub(crate) const fn store_id(self) -> StoreId {
        self.store_id
    }

    pub(crate) const fn source_manifest_checksum(self) -> u32 {
        self.source_manifest_checksum
    }

    pub(crate) const fn cutoff(self) -> DurableCutoff {
        DurableCutoff::from_manifest(
            self.store_id,
            self.active_generation,
            self.checkpoint_generation,
            self.append_sequence,
            self.committed_end_offset,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryReference {
    pub(crate) slot: u8,
    pub(crate) checksum: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryArtifact {
    pub(crate) reference: RecoveryReference,
    pub(crate) report: RecoveryReport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryCodecError {
    Invalid,
    StoreMismatch,
}

pub(crate) fn encode_recovery_state(
    report: RecoveryReport,
) -> Result<[u8; RECOVERY_STATE_LEN], RecoveryCodecError> {
    validate_report(report)?;
    let mut bytes = [0_u8; RECOVERY_STATE_LEN];
    bytes[..8].copy_from_slice(&RECOVERY_STATE_MAGIC);
    bytes[8..10].copy_from_slice(&RECOVERY_STATE_VERSION.to_be_bytes());
    bytes[10..12].copy_from_slice(&RECOVERY_STATE_LEN_U16.to_be_bytes());
    bytes[12..28].copy_from_slice(report.store_id.as_bytes());
    bytes[28..36].copy_from_slice(&report.report_generation.to_be_bytes());
    bytes[36..44].copy_from_slice(&report.source_manifest_generation.to_be_bytes());
    bytes[44..52].copy_from_slice(&report.committing_manifest_generation.to_be_bytes());
    bytes[52..56].copy_from_slice(&report.source_manifest_checksum.to_be_bytes());
    bytes[56..64].copy_from_slice(&report.active_generation.to_be_bytes());
    bytes[64..72].copy_from_slice(&report.sequence_floor.to_be_bytes());
    bytes[72..80].copy_from_slice(&report.checkpoint_generation.to_be_bytes());
    bytes[80..88].copy_from_slice(&report.append_sequence.to_be_bytes());
    bytes[88..96].copy_from_slice(&report.committed_end_offset.to_be_bytes());
    bytes[96..104].copy_from_slice(&report.original_journal_length.to_be_bytes());
    bytes[104..112].copy_from_slice(&report.removed_bytes.to_be_bytes());
    bytes[112] = report.classification.tag();
    bytes[113] = report.action.tag();
    let checksum = crc32c(&bytes[..124]);
    bytes[124..128].copy_from_slice(&checksum.to_be_bytes());
    Ok(bytes)
}

pub(crate) fn decode_recovery_state(
    bytes: &[u8],
    expected_store: StoreId,
) -> Result<RecoveryReport, RecoveryCodecError> {
    if bytes.len() != RECOVERY_STATE_LEN
        || bytes[..8] != RECOVERY_STATE_MAGIC
        || u16::from_be_bytes(bytes[8..10].try_into().unwrap_or_default()) != RECOVERY_STATE_VERSION
        || u16::from_be_bytes(bytes[10..12].try_into().unwrap_or_default())
            != RECOVERY_STATE_LEN_U16
        || bytes[114..124].iter().any(|byte| *byte != 0)
        || crc32c(&bytes[..124])
            != u32::from_be_bytes(bytes[124..128].try_into().unwrap_or_default())
    {
        return Err(RecoveryCodecError::Invalid);
    }
    let store_id = StoreId::from_bytes(bytes[12..28].try_into().unwrap_or_default())
        .map_err(|_| RecoveryCodecError::Invalid)?;
    if store_id != expected_store {
        return Err(RecoveryCodecError::StoreMismatch);
    }
    let report = RecoveryReport {
        store_id,
        report_generation: u64::from_be_bytes(bytes[28..36].try_into().unwrap_or_default()),
        source_manifest_generation: u64::from_be_bytes(
            bytes[36..44].try_into().unwrap_or_default(),
        ),
        committing_manifest_generation: u64::from_be_bytes(
            bytes[44..52].try_into().unwrap_or_default(),
        ),
        source_manifest_checksum: u32::from_be_bytes(bytes[52..56].try_into().unwrap_or_default()),
        active_generation: u64::from_be_bytes(bytes[56..64].try_into().unwrap_or_default()),
        sequence_floor: u64::from_be_bytes(bytes[64..72].try_into().unwrap_or_default()),
        checkpoint_generation: u64::from_be_bytes(bytes[72..80].try_into().unwrap_or_default()),
        append_sequence: u64::from_be_bytes(bytes[80..88].try_into().unwrap_or_default()),
        committed_end_offset: u64::from_be_bytes(bytes[88..96].try_into().unwrap_or_default()),
        original_journal_length: u64::from_be_bytes(bytes[96..104].try_into().unwrap_or_default()),
        removed_bytes: u64::from_be_bytes(bytes[104..112].try_into().unwrap_or_default()),
        classification: RecoveryClassification::from_tag(bytes[112])
            .ok_or(RecoveryCodecError::Invalid)?,
        action: RecoveryAction::from_tag(bytes[113]).ok_or(RecoveryCodecError::Invalid)?,
    };
    validate_report(report)?;
    if encode_recovery_state(report)?.as_slice() != bytes {
        return Err(RecoveryCodecError::Invalid);
    }
    Ok(report)
}

fn validate_report(report: RecoveryReport) -> Result<(), RecoveryCodecError> {
    if report.report_generation == 0
        || report.source_manifest_generation == 0
        || report.source_manifest_generation.checked_add(1)
            != Some(report.committing_manifest_generation)
        || report.active_generation == 0
        || report.checkpoint_generation == 0
        || report.append_sequence < report.sequence_floor
        || (report.append_sequence == report.sequence_floor)
            != (report.committed_end_offset == JOURNAL_V1_HEADER_LEN as u64)
        || report.committed_end_offset < JOURNAL_V1_HEADER_LEN as u64
        || report.original_journal_length <= report.committed_end_offset
        || report.original_journal_length > crate::MAX_ACTIVE_JOURNAL_BYTES
        || report
            .original_journal_length
            .checked_sub(report.committed_end_offset)
            != Some(report.removed_bytes)
        || report.removed_bytes == 0
        || report.action != RecoveryAction::TruncateToCommittedRoot
    {
        return Err(RecoveryCodecError::Invalid);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;

    fn report() -> RecoveryReport {
        RecoveryReport::new(
            test_support::store_id(1),
            1,
            7,
            0x1020_3040,
            DurableCutoff::from_manifest(test_support::store_id(1), 2, 3, 9, 400),
            5,
            423,
            RecoveryClassification::ShortFramePrefix,
        )
        .expect("valid report")
    }

    #[test]
    fn recovery_state_v1_refuses_hostile_fixed_fields_and_arithmetic() {
        let canonical = encode_recovery_state(report()).expect("canonical report");
        assert_eq!(
            decode_recovery_state(&canonical, test_support::store_id(1)),
            Ok(report())
        );
        let mut candidates = Vec::new();
        let mut wrong_magic = canonical;
        wrong_magic[0] ^= 1;
        candidates.push(wrong_magic.to_vec());
        for (range, replacement) in [
            (8..10, 2_u64),
            (10..12, 127),
            (28..36, 0),
            (36..44, 0),
            (44..52, 0),
            (56..64, 0),
            (72..80, 0),
            (104..112, 0),
        ] {
            let mut hostile = canonical;
            let width = range.end - range.start;
            hostile[range].copy_from_slice(&replacement.to_be_bytes()[8 - width..]);
            let checksum = crc32c(&hostile[..124]);
            hostile[124..].copy_from_slice(&checksum.to_be_bytes());
            candidates.push(hostile.to_vec());
        }
        for (offset, replacement) in [(112_usize, 0_u8), (113, 0), (114, 1)] {
            let mut hostile = canonical;
            hostile[offset] = replacement;
            let checksum = crc32c(&hostile[..124]);
            hostile[124..].copy_from_slice(&checksum.to_be_bytes());
            candidates.push(hostile.to_vec());
        }
        let mut bad_checksum = canonical.to_vec();
        bad_checksum[127] ^= 1;
        candidates.push(bad_checksum);
        candidates.push(canonical[..127].to_vec());
        let mut trailing = canonical.to_vec();
        trailing.push(0);
        candidates.push(trailing);
        for candidate in candidates {
            assert!(decode_recovery_state(&candidate, test_support::store_id(1)).is_err());
        }
        assert_eq!(
            decode_recovery_state(&canonical, test_support::store_id(2)),
            Err(RecoveryCodecError::StoreMismatch)
        );
    }
}
