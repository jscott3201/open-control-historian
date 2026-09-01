use super::schema::{FaultMode, PhaseId, PressureKind, RootClassification};
use crate::error::{EvidenceError, Result};
use std::collections::BTreeSet;
use std::num::NonZeroU32;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum Artifact {
    RootInventory,
    Marker,
    Manifest,
    ActiveJournal,
    Checkpoint,
    Registry,
    Retry,
    Recovery,
    Catalog,
    RawPair,
    SegmentPair,
    Intent,
    RawStaging,
    RawFinal,
    SegmentStaging,
    SegmentFinal,
    SuccessorJournal,
    SuccessorCheckpoint,
    CatalogStaging,
    CatalogFinal,
    ManifestStaging,
    ManifestFinal,
    StoreAuthority,
    StableLock,
}

impl Artifact {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::RootInventory => "ROOT_INVENTORY",
            Self::Marker => "MARKER",
            Self::Manifest => "SELECTED_MANIFEST",
            Self::ActiveJournal => "ACTIVE_JOURNAL_V1",
            Self::Checkpoint => "CHECKPOINT_V1",
            Self::Registry => "REGISTRY_V1",
            Self::Retry => "RETRY_V1",
            Self::Recovery => "RECOVERY_V1",
            Self::Catalog => "CATALOG_V2",
            Self::RawPair => "RETAINED_RAW_PAIR",
            Self::SegmentPair => "PUBLISHED_SEGMENT_PAIR",
            Self::Intent => "ROTATION_INTENT_V2",
            Self::RawStaging => "RAW_STAGING",
            Self::RawFinal => "RAW_FINAL",
            Self::SegmentStaging => "SEGMENT_STAGING",
            Self::SegmentFinal => "SEGMENT_FINAL",
            Self::SuccessorJournal => "SUCCESSOR_JOURNAL_V1",
            Self::SuccessorCheckpoint => "SUCCESSOR_CHECKPOINT_V1",
            Self::CatalogStaging => "CATALOG_V2_STAGING",
            Self::CatalogFinal => "CATALOG_V2_FINAL",
            Self::ManifestStaging => "MANIFEST_V2_STAGING",
            Self::ManifestFinal => "MANIFEST_V2_FINAL",
            Self::StoreAuthority => "STORE_AUTHORITY",
            Self::StableLock => "STABLE_LOCK",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum Operation {
    DirectoryOpen,
    DirectoryRead,
    FileOpen,
    MetadataRead,
    BoundedRead,
    CompleteValidation,
    RelationValidation,
    CreateNew,
    Write,
    Synchronize,
    Rename,
    Remove,
    Adopt,
    InspectionPublish,
    LockCreate,
    LockOpen,
    LockAcquire,
}

impl Operation {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::DirectoryOpen => "DIRECTORY_OPEN",
            Self::DirectoryRead => "DIRECTORY_READ",
            Self::FileOpen => "FILE_OPEN",
            Self::MetadataRead => "METADATA_READ",
            Self::BoundedRead => "BOUNDED_READ",
            Self::CompleteValidation => "COMPLETE_VALIDATION",
            Self::RelationValidation => "RELATION_VALIDATION",
            Self::CreateNew => "CREATE_NEW",
            Self::Write => "WRITE",
            Self::Synchronize => "SYNC_ALL",
            Self::Rename => "RENAME",
            Self::Remove => "REMOVE",
            Self::Adopt => "ADOPTION",
            Self::InspectionPublish => "INSPECTION_PUBLICATION",
            Self::LockCreate => "LOCK_CREATE",
            Self::LockOpen => "LOCK_OPEN",
            Self::LockAcquire => "LOCK_ACQUIRE",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CommitSide {
    Precommit,
    RenameBoundary,
    Postcommit,
}

impl CommitSide {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Precommit => "PRECOMMIT",
            Self::RenameBoundary => "RENAME_BOUNDARY",
            Self::Postcommit => "POSTCOMMIT",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum TerminalState {
    PriorRollback,
    CommittedConvergence,
    UnchangedRefusal,
    CompleteSuccess,
}

impl TerminalState {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::PriorRollback => "PRIOR_ROOT_ROLLBACK",
            Self::CommittedConvergence => "COMMITTED_ROOT_CONVERGENCE",
            Self::UnchangedRefusal => "UNCHANGED_REFUSAL",
            Self::CompleteSuccess => "COMPLETE_SUCCESS",
        }
    }
}

macro_rules! fault_registry {
    ($(
        $variant:ident => $literal:literal,
        $phase:ident, $artifact:ident, $operation:ident,
        $mutation:literal, $short:literal, $pressure:literal, $maximum:literal,
        $side:ident, $root:ident,
        next[$($next:ident),*], terminal[$($terminal:ident),*]
    );+ $(;)?) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub(super) enum FaultId { $($variant),+ }

        impl FaultId {
            pub(super) const ALL: &'static [Self] = &[$(Self::$variant),+];

            pub(super) const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $literal),+ }
            }

            pub(super) const fn source_symbol(self) -> &'static str {
                match self { $(Self::$variant => stringify!($variant)),+ }
            }

            pub(super) const fn source_invoke(self) -> super::SiteInvoke {
                match self { $(Self::$variant => $variant),+ }
            }

            pub(super) fn parse(value: &str) -> Result<Self> {
                match value {
                    $($literal => Ok(Self::$variant),)+
                    _ => Err(EvidenceError::InvalidHarness),
                }
            }

            pub(super) const fn descriptor(self) -> FaultDescriptor {
                match self {
                    $(Self::$variant => FaultDescriptor {
                        id: Self::$variant,
                        phase: PhaseId::$phase,
                        artifact: Artifact::$artifact,
                        operation: Operation::$operation,
                        mutation: $mutation,
                        short_write: $short,
                        pressure: $pressure,
                        maximum_occurrence: $maximum,
                        commit_side: CommitSide::$side,
                        expected_root: RootClassification::$root,
                        successors: &[$(Self::$next),*],
                        terminals: &[$(TerminalState::$terminal),*],
                    }),+
                }
            }

        }

        $(
            #[allow(non_snake_case)]
            fn $variant(
                io: &mut super::V2Io<'_>,
                selection: Option<FaultSelection>,
            ) -> Result<super::SiteResult> {
                io.execute_literal(FaultId::$variant, selection)
            }
        )+
    };
}

fault_registry! {
    P0DirectoryOpen => "V2IO-P0-INVENTORY-DIRECTORY-OPEN", Preflight, RootInventory, DirectoryOpen, false, false, false, 1, Precommit, Prior, next[P0DirectoryRead], terminal[UnchangedRefusal];
    P0DirectoryRead => "V2IO-P0-INVENTORY-DIRECTORY-READ", Preflight, RootInventory, DirectoryRead, false, false, false, 1, Precommit, Prior, next[P0ManifestOpen], terminal[UnchangedRefusal];
    P0ManifestOpen => "V2IO-P0-MANIFEST-OPEN", Preflight, Manifest, FileOpen, false, false, false, 1, Precommit, Prior, next[P0ManifestRead], terminal[UnchangedRefusal];
    P0ManifestRead => "V2IO-P0-MANIFEST-READ", Preflight, Manifest, BoundedRead, false, false, false, 1, Precommit, Prior, next[P0ManifestValidate], terminal[UnchangedRefusal];
    P0ManifestValidate => "V2IO-P0-MANIFEST-COMPLETE-VALIDATE", Preflight, Manifest, CompleteValidation, false, false, false, 1, Precommit, Prior, next[P0ActiveOpen], terminal[UnchangedRefusal];
    P0ActiveOpen => "V2IO-P0-ACTIVE-OPEN", Preflight, ActiveJournal, FileOpen, false, false, false, 1, Precommit, Prior, next[P0ActiveMetadata], terminal[UnchangedRefusal];
    P0ActiveMetadata => "V2IO-P0-ACTIVE-METADATA", Preflight, ActiveJournal, MetadataRead, false, false, false, 1, Precommit, Prior, next[P0ActiveRead], terminal[UnchangedRefusal];
    P0ActiveRead => "V2IO-P0-ACTIVE-READ", Preflight, ActiveJournal, BoundedRead, false, false, false, 4096, Precommit, Prior, next[P0ActiveRead, P0ActiveValidate], terminal[UnchangedRefusal];
    P0ActiveValidate => "V2IO-P0-ACTIVE-COMPLETE-VALIDATE", Preflight, ActiveJournal, CompleteValidation, false, false, false, 1, Precommit, Prior, next[P0CheckpointOpen], terminal[UnchangedRefusal];
    P0CheckpointOpen => "V2IO-P0-CHECKPOINT-OPEN", Preflight, Checkpoint, FileOpen, false, false, false, 1, Precommit, Prior, next[P0CheckpointRead], terminal[UnchangedRefusal];
    P0CheckpointRead => "V2IO-P0-CHECKPOINT-READ", Preflight, Checkpoint, BoundedRead, false, false, false, 1, Precommit, Prior, next[P0CheckpointValidate], terminal[UnchangedRefusal];
    P0CheckpointValidate => "V2IO-P0-CHECKPOINT-VALIDATE", Preflight, Checkpoint, CompleteValidation, false, false, false, 1, Precommit, Prior, next[P0RegistryOpen], terminal[UnchangedRefusal];
    P0RegistryOpen => "V2IO-P0-REGISTRY-OPEN", Preflight, Registry, FileOpen, false, false, false, 3, Precommit, Prior, next[P0RegistryRead], terminal[UnchangedRefusal];
    P0RegistryRead => "V2IO-P0-REGISTRY-READ", Preflight, Registry, BoundedRead, false, false, false, 3, Precommit, Prior, next[P0RegistryValidate], terminal[UnchangedRefusal];
    P0RegistryValidate => "V2IO-P0-REGISTRY-VALIDATE", Preflight, Registry, CompleteValidation, false, false, false, 3, Precommit, Prior, next[P0RetryOpen], terminal[UnchangedRefusal];
    P0RetryOpen => "V2IO-P0-RETRY-OPEN", Preflight, Retry, FileOpen, false, false, false, 3, Precommit, Prior, next[P0RetryRead], terminal[UnchangedRefusal];
    P0RetryRead => "V2IO-P0-RETRY-READ", Preflight, Retry, BoundedRead, false, false, false, 3, Precommit, Prior, next[P0RetryValidate], terminal[UnchangedRefusal];
    P0RetryValidate => "V2IO-P0-RETRY-VALIDATE", Preflight, Retry, CompleteValidation, false, false, false, 3, Precommit, Prior, next[P0RecoveryOpen], terminal[UnchangedRefusal];
    P0RecoveryOpen => "V2IO-P0-RECOVERY-OPEN", Preflight, Recovery, FileOpen, false, false, false, 3, Precommit, Prior, next[P0RecoveryRead], terminal[UnchangedRefusal];
    P0RecoveryRead => "V2IO-P0-RECOVERY-READ", Preflight, Recovery, BoundedRead, false, false, false, 3, Precommit, Prior, next[P0RecoveryValidate], terminal[UnchangedRefusal];
    P0RecoveryValidate => "V2IO-P0-RECOVERY-VALIDATE", Preflight, Recovery, CompleteValidation, false, false, false, 3, Precommit, Prior, next[P0CatalogOpen], terminal[UnchangedRefusal];
    P0CatalogOpen => "V2IO-P0-CATALOG-OPEN", Preflight, Catalog, FileOpen, false, false, false, 3, Precommit, Prior, next[P0CatalogRead], terminal[UnchangedRefusal];
    P0CatalogRead => "V2IO-P0-CATALOG-READ", Preflight, Catalog, BoundedRead, false, false, false, 3, Precommit, Prior, next[P0CatalogValidate], terminal[UnchangedRefusal];
    P0CatalogValidate => "V2IO-P0-CATALOG-VALIDATE", Preflight, Catalog, CompleteValidation, false, false, false, 3, Precommit, Prior, next[P0PairRawOpen], terminal[UnchangedRefusal];
    P0PairRawOpen => "V2IO-P0-PAIR-RAW-OPEN", Preflight, RawPair, FileOpen, false, false, false, 64, Precommit, Prior, next[P0PairRawRead], terminal[UnchangedRefusal];
    P0PairRawRead => "V2IO-P0-PAIR-RAW-READ", Preflight, RawPair, BoundedRead, false, false, false, 524_288, Precommit, Prior, next[P0PairRawRead, P0PairRawValidate], terminal[UnchangedRefusal];
    P0PairRawValidate => "V2IO-P0-PAIR-RAW-VALIDATE", Preflight, RawPair, CompleteValidation, false, false, false, 64, Precommit, Prior, next[P0PairSegmentOpen], terminal[UnchangedRefusal];
    P0PairSegmentOpen => "V2IO-P0-PAIR-SEGMENT-OPEN", Preflight, SegmentPair, FileOpen, false, false, false, 64, Precommit, Prior, next[P0PairSegmentRead], terminal[UnchangedRefusal];
    P0PairSegmentRead => "V2IO-P0-PAIR-SEGMENT-READ", Preflight, SegmentPair, BoundedRead, false, false, false, 622_976, Precommit, Prior, next[P0PairSegmentRead, P0PairSegmentValidate], terminal[UnchangedRefusal];
    P0PairSegmentValidate => "V2IO-P0-PAIR-SEGMENT-VALIDATE", Preflight, SegmentPair, CompleteValidation, false, false, false, 64, Precommit, Prior, next[P0PairRelation], terminal[UnchangedRefusal];
    P0PairRelation => "V2IO-P0-PAIR-RELATION-VALIDATE", Preflight, StoreAuthority, RelationValidation, false, false, false, 64, Precommit, Prior, next[P0PairRawOpen, P0CompleteRelation], terminal[UnchangedRefusal];
    P0CompleteRelation => "V2IO-P0-COMPLETE-RELATION-VALIDATE", Preflight, StoreAuthority, RelationValidation, false, false, false, 1, Precommit, Prior, next[P1IntentCreate], terminal[UnchangedRefusal];

    P1IntentCreate => "V2IO-P1-INTENT-CREATE-NEW", Intent, Intent, CreateNew, true, false, true, 1, Precommit, Prior, next[P1IntentWrite], terminal[PriorRollback];
    P1IntentWrite => "V2IO-P1-INTENT-WRITE", Intent, Intent, Write, true, true, true, 1, Precommit, Prior, next[P1IntentSync], terminal[PriorRollback];
    P1IntentSync => "V2IO-P1-INTENT-SYNC", Intent, Intent, Synchronize, true, false, true, 1, Precommit, Prior, next[P1IntentReadbackOpen], terminal[PriorRollback];
    P1IntentReadbackOpen => "V2IO-P1-INTENT-READBACK-OPEN", Intent, Intent, FileOpen, false, false, false, 1, Precommit, Prior, next[P1IntentReadback], terminal[PriorRollback];
    P1IntentReadback => "V2IO-P1-INTENT-READBACK", Intent, Intent, BoundedRead, false, false, false, 1, Precommit, Prior, next[P1IntentDecode], terminal[PriorRollback];
    P1IntentDecode => "V2IO-P1-INTENT-DECODE", Intent, Intent, CompleteValidation, false, false, false, 1, Precommit, Prior, next[P1DirectorySync], terminal[PriorRollback];
    P1DirectorySync => "V2IO-P1-INTENT-DIRECTORY-SYNC", Intent, RootInventory, Synchronize, true, false, true, 1, Precommit, Prior, next[P2SourceOpen], terminal[PriorRollback];

    P2SourceOpen => "V2IO-P2-RAW-SOURCE-OPEN", Raw, ActiveJournal, FileOpen, false, false, false, 1, Precommit, Prior, next[P2SourceRead], terminal[PriorRollback];
    P2SourceRead => "V2IO-P2-RAW-SOURCE-READ", Raw, ActiveJournal, BoundedRead, false, false, false, 4096, Precommit, Prior, next[P2SourceRead, P2StagingCreate], terminal[PriorRollback];
    P2StagingCreate => "V2IO-P2-RAW-STAGING-CREATE", Raw, RawStaging, CreateNew, true, false, true, 1, Precommit, Prior, next[P2StagingWrite], terminal[PriorRollback];
    P2StagingWrite => "V2IO-P2-RAW-STAGING-WRITE", Raw, RawStaging, Write, true, true, true, 4096, Precommit, Prior, next[P2SourceRead, P2StagingWrite, P2StagingSync], terminal[PriorRollback];
    P2StagingSync => "V2IO-P2-RAW-STAGING-SYNC", Raw, RawStaging, Synchronize, true, false, true, 1, Precommit, Prior, next[P2ReadbackOpen], terminal[PriorRollback];
    P2ReadbackOpen => "V2IO-P2-RAW-READBACK-OPEN", Raw, RawStaging, FileOpen, false, false, false, 1, Precommit, Prior, next[P2ReadbackRead], terminal[PriorRollback];
    P2ReadbackRead => "V2IO-P2-RAW-READBACK-READ", Raw, RawStaging, BoundedRead, false, false, false, 4096, Precommit, Prior, next[P2ReadbackRead, P2ReadbackValidate], terminal[PriorRollback];
    P2ReadbackValidate => "V2IO-P2-RAW-READBACK-VALIDATE", Raw, RawStaging, CompleteValidation, false, false, false, 1, Precommit, Prior, next[P2Rename], terminal[PriorRollback];
    P2Rename => "V2IO-P2-RAW-RENAME", Raw, RawFinal, Rename, true, false, true, 1, Precommit, Prior, next[P2DirectorySync], terminal[PriorRollback];
    P2DirectorySync => "V2IO-P2-RAW-DIRECTORY-SYNC", Raw, RootInventory, Synchronize, true, false, true, 1, Precommit, Prior, next[P2FinalOpen], terminal[PriorRollback];
    P2FinalOpen => "V2IO-P2-RAW-FINAL-OPEN", Raw, RawFinal, FileOpen, false, false, false, 1, Precommit, Prior, next[P2FinalReadValidate], terminal[PriorRollback];
    P2FinalReadValidate => "V2IO-P2-RAW-FINAL-READ-VALIDATE", Raw, RawFinal, CompleteValidation, false, false, false, 1, Precommit, Prior, next[P3SourceOpen], terminal[PriorRollback];

    P3SourceOpen => "V2IO-P3-SEGMENT-SOURCE-OPEN", Segment, RawFinal, FileOpen, false, false, false, 1, Precommit, Prior, next[P3SourceRead], terminal[PriorRollback];
    P3SourceRead => "V2IO-P3-SEGMENT-SOURCE-READ", Segment, RawFinal, BoundedRead, false, false, false, 4096, Precommit, Prior, next[P3SourceRead, P3StagingCreate], terminal[PriorRollback];
    P3StagingCreate => "V2IO-P3-SEGMENT-STAGING-CREATE", Segment, SegmentStaging, CreateNew, true, false, true, 1, Precommit, Prior, next[P3StagingWrite], terminal[PriorRollback];
    P3StagingWrite => "V2IO-P3-SEGMENT-STAGING-WRITE", Segment, SegmentStaging, Write, true, true, true, 8192, Precommit, Prior, next[P3SourceRead, P3StagingWrite, P3StagingSync], terminal[PriorRollback];
    P3StagingSync => "V2IO-P3-SEGMENT-STAGING-SYNC", Segment, SegmentStaging, Synchronize, true, false, true, 1, Precommit, Prior, next[P3ReadbackOpen], terminal[PriorRollback];
    P3ReadbackOpen => "V2IO-P3-SEGMENT-READBACK-OPEN", Segment, SegmentStaging, FileOpen, false, false, false, 1, Precommit, Prior, next[P3ReadbackRead], terminal[PriorRollback];
    P3ReadbackRead => "V2IO-P3-SEGMENT-READBACK-READ", Segment, SegmentStaging, BoundedRead, false, false, false, 8192, Precommit, Prior, next[P3ReadbackRead, P3HostileValidate], terminal[PriorRollback];
    P3HostileValidate => "V2IO-P3-SEGMENT-HOSTILE-VALIDATE", Segment, SegmentStaging, CompleteValidation, false, false, false, 1, Precommit, Prior, next[P3SourceLinkValidate], terminal[PriorRollback];
    P3SourceLinkValidate => "V2IO-P3-SEGMENT-SOURCE-LINK-VALIDATE", Segment, SegmentStaging, RelationValidation, false, false, false, 1, Precommit, Prior, next[P3Rename], terminal[PriorRollback];
    P3Rename => "V2IO-P3-SEGMENT-RENAME", Segment, SegmentFinal, Rename, true, false, true, 1, Precommit, Prior, next[P3DirectorySync], terminal[PriorRollback];
    P3DirectorySync => "V2IO-P3-SEGMENT-DIRECTORY-SYNC", Segment, RootInventory, Synchronize, true, false, true, 1, Precommit, Prior, next[P3FinalOpen], terminal[PriorRollback];
    P3FinalOpen => "V2IO-P3-SEGMENT-FINAL-OPEN", Segment, SegmentFinal, FileOpen, false, false, false, 1, Precommit, Prior, next[P3FinalReadValidate], terminal[PriorRollback];
    P3FinalReadValidate => "V2IO-P3-SEGMENT-FINAL-READ-VALIDATE", Segment, SegmentFinal, CompleteValidation, false, false, false, 1, Precommit, Prior, next[P4JournalCreate], terminal[PriorRollback];

    P4JournalCreate => "V2IO-P4-SUCCESSOR-JOURNAL-CREATE", Successor, SuccessorJournal, CreateNew, true, false, true, 1, Precommit, Prior, next[P4JournalWrite], terminal[PriorRollback];
    P4JournalWrite => "V2IO-P4-SUCCESSOR-JOURNAL-WRITE", Successor, SuccessorJournal, Write, true, true, true, 1, Precommit, Prior, next[P4JournalSync], terminal[PriorRollback];
    P4JournalSync => "V2IO-P4-SUCCESSOR-JOURNAL-SYNC", Successor, SuccessorJournal, Synchronize, true, false, true, 1, Precommit, Prior, next[P4CheckpointCreate], terminal[PriorRollback];
    P4CheckpointCreate => "V2IO-P4-SUCCESSOR-CHECKPOINT-CREATE", Successor, SuccessorCheckpoint, CreateNew, true, false, true, 1, Precommit, Prior, next[P4CheckpointWrite], terminal[PriorRollback];
    P4CheckpointWrite => "V2IO-P4-SUCCESSOR-CHECKPOINT-WRITE", Successor, SuccessorCheckpoint, Write, true, true, true, 1, Precommit, Prior, next[P4CheckpointSync], terminal[PriorRollback];
    P4CheckpointSync => "V2IO-P4-SUCCESSOR-CHECKPOINT-SYNC", Successor, SuccessorCheckpoint, Synchronize, true, false, true, 1, Precommit, Prior, next[P4DirectorySync], terminal[PriorRollback];
    P4DirectorySync => "V2IO-P4-SUCCESSOR-DIRECTORY-SYNC", Successor, RootInventory, Synchronize, true, false, true, 1, Precommit, Prior, next[P4JournalOpen], terminal[PriorRollback];
    P4JournalOpen => "V2IO-P4-SUCCESSOR-JOURNAL-OPEN", Successor, SuccessorJournal, FileOpen, false, false, false, 1, Precommit, Prior, next[P4JournalRead], terminal[PriorRollback];
    P4JournalRead => "V2IO-P4-SUCCESSOR-JOURNAL-READ", Successor, SuccessorJournal, BoundedRead, false, false, false, 1, Precommit, Prior, next[P4JournalValidate], terminal[PriorRollback];
    P4JournalValidate => "V2IO-P4-SUCCESSOR-JOURNAL-VALIDATE", Successor, SuccessorJournal, CompleteValidation, false, false, false, 1, Precommit, Prior, next[P4CheckpointOpen], terminal[PriorRollback];
    P4CheckpointOpen => "V2IO-P4-SUCCESSOR-CHECKPOINT-OPEN", Successor, SuccessorCheckpoint, FileOpen, false, false, false, 1, Precommit, Prior, next[P4CheckpointRead], terminal[PriorRollback];
    P4CheckpointRead => "V2IO-P4-SUCCESSOR-CHECKPOINT-READ", Successor, SuccessorCheckpoint, BoundedRead, false, false, false, 1, Precommit, Prior, next[P4CheckpointValidate], terminal[PriorRollback];
    P4CheckpointValidate => "V2IO-P4-SUCCESSOR-CHECKPOINT-VALIDATE", Successor, SuccessorCheckpoint, CompleteValidation, false, false, false, 1, Precommit, Prior, next[P4RelationValidate], terminal[PriorRollback];
    P4RelationValidate => "V2IO-P4-SUCCESSOR-RELATION-VALIDATE", Successor, StoreAuthority, RelationValidation, false, false, false, 1, Precommit, Prior, next[P5Create], terminal[PriorRollback];

    P5Create => "V2IO-P5-CATALOG-CREATE", Catalog, CatalogStaging, CreateNew, true, false, true, 1, Precommit, Prior, next[P5Write], terminal[PriorRollback];
    P5Write => "V2IO-P5-CATALOG-WRITE", Catalog, CatalogStaging, Write, true, true, true, 1, Precommit, Prior, next[P5Sync], terminal[PriorRollback];
    P5Sync => "V2IO-P5-CATALOG-SYNC", Catalog, CatalogStaging, Synchronize, true, false, true, 1, Precommit, Prior, next[P5ReadbackOpen], terminal[PriorRollback];
    P5ReadbackOpen => "V2IO-P5-CATALOG-READBACK-OPEN", Catalog, CatalogStaging, FileOpen, false, false, false, 1, Precommit, Prior, next[P5Readback], terminal[PriorRollback];
    P5Readback => "V2IO-P5-CATALOG-READBACK", Catalog, CatalogStaging, BoundedRead, false, false, false, 1, Precommit, Prior, next[P5ReadbackValidate], terminal[PriorRollback];
    P5ReadbackValidate => "V2IO-P5-CATALOG-READBACK-VALIDATE", Catalog, CatalogStaging, CompleteValidation, false, false, false, 1, Precommit, Prior, next[P5Rename], terminal[PriorRollback];
    P5Rename => "V2IO-P5-CATALOG-RENAME", Catalog, CatalogFinal, Rename, true, false, true, 1, Precommit, Prior, next[P5DirectorySync], terminal[PriorRollback];
    P5DirectorySync => "V2IO-P5-CATALOG-DIRECTORY-SYNC", Catalog, RootInventory, Synchronize, true, false, true, 1, Precommit, Prior, next[P5FinalOpen], terminal[PriorRollback];
    P5FinalOpen => "V2IO-P5-CATALOG-FINAL-OPEN", Catalog, CatalogFinal, FileOpen, false, false, false, 1, Precommit, Prior, next[P5FinalRelation], terminal[PriorRollback];
    P5FinalRelation => "V2IO-P5-CATALOG-FINAL-RELATION-VALIDATE", Catalog, CatalogFinal, RelationValidation, false, false, false, 1, Precommit, Prior, next[P6Create], terminal[PriorRollback];

    P6Create => "V2IO-P6-MANIFEST-CREATE", Manifest, ManifestStaging, CreateNew, true, false, true, 1, Precommit, Prior, next[P6Write], terminal[PriorRollback];
    P6Write => "V2IO-P6-MANIFEST-WRITE", Manifest, ManifestStaging, Write, true, true, true, 1, Precommit, Prior, next[P6Sync], terminal[PriorRollback];
    P6Sync => "V2IO-P6-MANIFEST-SYNC", Manifest, ManifestStaging, Synchronize, true, false, true, 1, Precommit, Prior, next[P6ReadbackOpen], terminal[PriorRollback];
    P6ReadbackOpen => "V2IO-P6-MANIFEST-READBACK-OPEN", Manifest, ManifestStaging, FileOpen, false, false, false, 1, Precommit, Prior, next[P6Readback], terminal[PriorRollback];
    P6Readback => "V2IO-P6-MANIFEST-READBACK", Manifest, ManifestStaging, BoundedRead, false, false, false, 1, Precommit, Prior, next[P6ReadbackValidate], terminal[PriorRollback];
    P6ReadbackValidate => "V2IO-P6-MANIFEST-READBACK-VALIDATE", Manifest, ManifestStaging, CompleteValidation, false, false, false, 1, Precommit, Prior, next[P6RenameCommit], terminal[PriorRollback];
    P6RenameCommit => "V2IO-P6-MANIFEST-RENAME-COMMIT", Manifest, ManifestFinal, Rename, true, false, true, 1, RenameBoundary, Committed, next[P6DirectorySync], terminal[PriorRollback, CommittedConvergence];
    P6DirectorySync => "V2IO-P6-MANIFEST-DIRECTORY-SYNC", Manifest, RootInventory, Synchronize, true, false, true, 1, Postcommit, Committed, next[P6FinalOpen], terminal[CommittedConvergence];
    P6FinalOpen => "V2IO-P6-MANIFEST-FINAL-OPEN", Manifest, ManifestFinal, FileOpen, false, false, false, 1, Postcommit, Committed, next[P6FinalRelation], terminal[CommittedConvergence];
    P6FinalRelation => "V2IO-P6-MANIFEST-COMMITTED-RELATION-VALIDATE", Manifest, ManifestFinal, RelationValidation, false, false, false, 1, Postcommit, Committed, next[P7Adopt], terminal[CommittedConvergence];

    P7Adopt => "V2IO-P7-ADOPT-SUCCESSOR", AdoptClean, StoreAuthority, Adopt, true, false, false, 1, Postcommit, Committed, next[P7Inspection], terminal[CommittedConvergence];
    P7Inspection => "V2IO-P7-ADOPT-INSPECTION-PUBLISH", AdoptClean, StoreAuthority, InspectionPublish, true, false, false, 1, Postcommit, Committed, next[P7StateValidate], terminal[CommittedConvergence];
    P7StateValidate => "V2IO-P7-ADOPT-STATE-VALIDATE", AdoptClean, StoreAuthority, RelationValidation, false, false, false, 1, Postcommit, Committed, next[P7PredecessorOpen], terminal[CommittedConvergence];
    P7PredecessorOpen => "V2IO-P7-CLEAN-PREDECESSOR-ACTIVE-OPEN", AdoptClean, ActiveJournal, FileOpen, false, false, false, 1, Postcommit, Committed, next[P7PredecessorValidate], terminal[CommittedConvergence];
    P7PredecessorValidate => "V2IO-P7-CLEAN-PREDECESSOR-ACTIVE-VALIDATE", AdoptClean, ActiveJournal, CompleteValidation, false, false, false, 1, Postcommit, Committed, next[P7PredecessorRemove], terminal[CommittedConvergence];
    P7PredecessorRemove => "V2IO-P7-CLEAN-PREDECESSOR-ACTIVE-REMOVE", AdoptClean, ActiveJournal, Remove, true, false, true, 1, Postcommit, Committed, next[P7PredecessorSync], terminal[CommittedConvergence];
    P7PredecessorSync => "V2IO-P7-CLEAN-PREDECESSOR-ACTIVE-DIRECTORY-SYNC", AdoptClean, RootInventory, Synchronize, true, false, true, 1, Postcommit, Committed, next[P7CheckpointOpen], terminal[CommittedConvergence];
    P7CheckpointOpen => "V2IO-P7-CLEAN-PREDECESSOR-CHECKPOINT-OPEN", AdoptClean, Checkpoint, FileOpen, false, false, false, 1, Postcommit, Committed, next[P7CheckpointValidate], terminal[CommittedConvergence];
    P7CheckpointValidate => "V2IO-P7-CLEAN-PREDECESSOR-CHECKPOINT-VALIDATE", AdoptClean, Checkpoint, CompleteValidation, false, false, false, 1, Postcommit, Committed, next[P7CheckpointRemove], terminal[CommittedConvergence];
    P7CheckpointRemove => "V2IO-P7-CLEAN-PREDECESSOR-CHECKPOINT-REMOVE", AdoptClean, Checkpoint, Remove, true, false, true, 1, Postcommit, Committed, next[P7CheckpointSync], terminal[CommittedConvergence];
    P7CheckpointSync => "V2IO-P7-CLEAN-PREDECESSOR-CHECKPOINT-DIRECTORY-SYNC", AdoptClean, RootInventory, Synchronize, true, false, true, 1, Postcommit, Committed, next[P7RawStagingOpen], terminal[CommittedConvergence];
    P7RawStagingOpen => "V2IO-P7-CLEAN-RAW-STAGING-OPEN-VALIDATE", AdoptClean, RawStaging, CompleteValidation, false, false, false, 1, Postcommit, Committed, next[P7RawStagingRemove, P7SegmentStagingOpen], terminal[CommittedConvergence];
    P7RawStagingRemove => "V2IO-P7-CLEAN-RAW-STAGING-REMOVE", AdoptClean, RawStaging, Remove, true, false, true, 1, Postcommit, Committed, next[P7RawStagingSync], terminal[CommittedConvergence];
    P7RawStagingSync => "V2IO-P7-CLEAN-RAW-STAGING-DIRECTORY-SYNC", AdoptClean, RootInventory, Synchronize, true, false, true, 1, Postcommit, Committed, next[P7SegmentStagingOpen], terminal[CommittedConvergence];
    P7SegmentStagingOpen => "V2IO-P7-CLEAN-SEGMENT-STAGING-OPEN-VALIDATE", AdoptClean, SegmentStaging, CompleteValidation, false, false, false, 1, Postcommit, Committed, next[P7SegmentStagingRemove, P7CatalogStagingOpen], terminal[CommittedConvergence];
    P7SegmentStagingRemove => "V2IO-P7-CLEAN-SEGMENT-STAGING-REMOVE", AdoptClean, SegmentStaging, Remove, true, false, true, 1, Postcommit, Committed, next[P7SegmentStagingSync], terminal[CommittedConvergence];
    P7SegmentStagingSync => "V2IO-P7-CLEAN-SEGMENT-STAGING-DIRECTORY-SYNC", AdoptClean, RootInventory, Synchronize, true, false, true, 1, Postcommit, Committed, next[P7CatalogStagingOpen], terminal[CommittedConvergence];
    P7CatalogStagingOpen => "V2IO-P7-CLEAN-CATALOG-STAGING-OPEN-VALIDATE", AdoptClean, CatalogStaging, CompleteValidation, false, false, false, 1, Postcommit, Committed, next[P7CatalogStagingRemove, P7ManifestStagingOpen], terminal[CommittedConvergence];
    P7CatalogStagingRemove => "V2IO-P7-CLEAN-CATALOG-STAGING-REMOVE", AdoptClean, CatalogStaging, Remove, true, false, true, 1, Postcommit, Committed, next[P7CatalogStagingSync], terminal[CommittedConvergence];
    P7CatalogStagingSync => "V2IO-P7-CLEAN-CATALOG-STAGING-DIRECTORY-SYNC", AdoptClean, RootInventory, Synchronize, true, false, true, 1, Postcommit, Committed, next[P7ManifestStagingOpen], terminal[CommittedConvergence];
    P7ManifestStagingOpen => "V2IO-P7-CLEAN-MANIFEST-STAGING-OPEN-VALIDATE", AdoptClean, ManifestStaging, CompleteValidation, false, false, false, 1, Postcommit, Committed, next[P7ManifestStagingRemove, P7InventoryRead], terminal[CommittedConvergence];
    P7ManifestStagingRemove => "V2IO-P7-CLEAN-MANIFEST-STAGING-REMOVE", AdoptClean, ManifestStaging, Remove, true, false, true, 1, Postcommit, Committed, next[P7ManifestStagingSync], terminal[CommittedConvergence];
    P7ManifestStagingSync => "V2IO-P7-CLEAN-MANIFEST-STAGING-DIRECTORY-SYNC", AdoptClean, RootInventory, Synchronize, true, false, true, 1, Postcommit, Committed, next[P7InventoryRead], terminal[CommittedConvergence];
    P7InventoryRead => "V2IO-P7-CLEAN-INVENTORY-READ", AdoptClean, RootInventory, DirectoryRead, false, false, false, 1, Postcommit, Committed, next[P7InventoryValidate], terminal[CommittedConvergence];
    P7InventoryValidate => "V2IO-P7-CLEAN-INVENTORY-VALIDATE", AdoptClean, RootInventory, CompleteValidation, false, false, false, 1, Postcommit, Committed, next[P7IntentRemove], terminal[CommittedConvergence];
    P7IntentRemove => "V2IO-P7-CLEAN-INTENT-LAST-REMOVE", AdoptClean, Intent, Remove, true, false, true, 1, Postcommit, Committed, next[P7FinalDirectorySync], terminal[CommittedConvergence];
    P7FinalDirectorySync => "V2IO-P7-CLEAN-FINAL-DIRECTORY-SYNC", AdoptClean, RootInventory, Synchronize, true, false, true, 1, Postcommit, Committed, next[], terminal[CompleteSuccess, CommittedConvergence];

    RbRawValidate => "V2IO-RB-RAW-DERIVATIVE-VALIDATE", Rollback, RawStaging, CompleteValidation, false, false, false, 2, Precommit, Prior, next[RbRawRemove, RbSegmentValidate], terminal[UnchangedRefusal];
    RbRawRemove => "V2IO-RB-RAW-DERIVATIVE-REMOVE", Rollback, RawStaging, Remove, true, false, true, 2, Precommit, Prior, next[RbRawSync], terminal[UnchangedRefusal];
    RbRawSync => "V2IO-RB-RAW-DERIVATIVE-DIRECTORY-SYNC", Rollback, RootInventory, Synchronize, true, false, true, 2, Precommit, Prior, next[RbRawValidate, RbSegmentValidate], terminal[UnchangedRefusal];
    RbSegmentValidate => "V2IO-RB-SEGMENT-DERIVATIVE-VALIDATE", Rollback, SegmentStaging, CompleteValidation, false, false, false, 2, Precommit, Prior, next[RbSegmentRemove, RbSuccessorValidate], terminal[UnchangedRefusal];
    RbSegmentRemove => "V2IO-RB-SEGMENT-DERIVATIVE-REMOVE", Rollback, SegmentStaging, Remove, true, false, true, 2, Precommit, Prior, next[RbSegmentSync], terminal[UnchangedRefusal];
    RbSegmentSync => "V2IO-RB-SEGMENT-DERIVATIVE-DIRECTORY-SYNC", Rollback, RootInventory, Synchronize, true, false, true, 2, Precommit, Prior, next[RbSegmentValidate, RbSuccessorValidate], terminal[UnchangedRefusal];
    RbSuccessorValidate => "V2IO-RB-SUCCESSOR-PAIR-VALIDATE", Rollback, SuccessorJournal, CompleteValidation, false, false, false, 2, Precommit, Prior, next[RbSuccessorRemove, RbCatalogValidate], terminal[UnchangedRefusal];
    RbSuccessorRemove => "V2IO-RB-SUCCESSOR-PAIR-REMOVE", Rollback, SuccessorJournal, Remove, true, false, true, 2, Precommit, Prior, next[RbSuccessorSync], terminal[UnchangedRefusal];
    RbSuccessorSync => "V2IO-RB-SUCCESSOR-PAIR-DIRECTORY-SYNC", Rollback, RootInventory, Synchronize, true, false, true, 2, Precommit, Prior, next[RbSuccessorValidate, RbCatalogValidate], terminal[UnchangedRefusal];
    RbCatalogValidate => "V2IO-RB-CATALOG-DERIVATIVE-VALIDATE", Rollback, CatalogStaging, CompleteValidation, false, false, false, 2, Precommit, Prior, next[RbCatalogRemove, RbManifestValidate], terminal[UnchangedRefusal];
    RbCatalogRemove => "V2IO-RB-CATALOG-DERIVATIVE-REMOVE", Rollback, CatalogStaging, Remove, true, false, true, 2, Precommit, Prior, next[RbCatalogSync], terminal[UnchangedRefusal];
    RbCatalogSync => "V2IO-RB-CATALOG-DERIVATIVE-DIRECTORY-SYNC", Rollback, RootInventory, Synchronize, true, false, true, 2, Precommit, Prior, next[RbCatalogValidate, RbManifestValidate], terminal[UnchangedRefusal];
    RbManifestValidate => "V2IO-RB-MANIFEST-DERIVATIVE-VALIDATE", Rollback, ManifestStaging, CompleteValidation, false, false, false, 2, Precommit, Prior, next[RbManifestRemove, RbInventoryRead], terminal[UnchangedRefusal];
    RbManifestRemove => "V2IO-RB-MANIFEST-DERIVATIVE-REMOVE", Rollback, ManifestStaging, Remove, true, false, true, 2, Precommit, Prior, next[RbManifestSync], terminal[UnchangedRefusal];
    RbManifestSync => "V2IO-RB-MANIFEST-DERIVATIVE-DIRECTORY-SYNC", Rollback, RootInventory, Synchronize, true, false, true, 2, Precommit, Prior, next[RbManifestValidate, RbInventoryRead], terminal[UnchangedRefusal];
    RbInventoryRead => "V2IO-RB-PRIOR-INVENTORY-READ", Rollback, RootInventory, DirectoryRead, false, false, false, 1, Precommit, Prior, next[RbInventoryValidate], terminal[UnchangedRefusal];
    RbInventoryValidate => "V2IO-RB-PRIOR-INVENTORY-VALIDATE", Rollback, RootInventory, CompleteValidation, false, false, false, 1, Precommit, Prior, next[RbIntentRemove], terminal[UnchangedRefusal];
    RbIntentRemove => "V2IO-RB-INTENT-LAST-REMOVE", Rollback, Intent, Remove, true, false, true, 1, Precommit, Prior, next[RbFinalSync], terminal[UnchangedRefusal];
    RbFinalSync => "V2IO-RB-FINAL-DIRECTORY-SYNC", Rollback, RootInventory, Synchronize, true, false, true, 1, Precommit, Prior, next[], terminal[PriorRollback, UnchangedRefusal];

    OpenDirectoryOpen => "V2IO-OPEN-DIRECTORY-OPEN", EagerOpen, RootInventory, DirectoryOpen, false, false, false, 1, Precommit, Prior, next[OpenDirectoryRead], terminal[UnchangedRefusal];
    OpenDirectoryRead => "V2IO-OPEN-DIRECTORY-READ", EagerOpen, RootInventory, DirectoryRead, false, false, false, 1, Precommit, Prior, next[OpenMarkerOpen], terminal[UnchangedRefusal];
    OpenMarkerOpen => "V2IO-OPEN-MARKER-OPEN", EagerOpen, Marker, FileOpen, false, false, false, 1, Precommit, Prior, next[OpenMarkerMetadata], terminal[UnchangedRefusal];
    OpenMarkerMetadata => "V2IO-OPEN-MARKER-METADATA", EagerOpen, Marker, MetadataRead, false, false, false, 1, Precommit, Prior, next[OpenMarkerRead], terminal[UnchangedRefusal];
    OpenMarkerRead => "V2IO-OPEN-MARKER-READ", EagerOpen, Marker, BoundedRead, false, false, false, 1, Precommit, Prior, next[OpenMarkerValidate], terminal[UnchangedRefusal];
    OpenMarkerValidate => "V2IO-OPEN-MARKER-VALIDATE", EagerOpen, Marker, CompleteValidation, false, false, false, 1, Precommit, Prior, next[OpenManifestOpen], terminal[UnchangedRefusal];
    OpenManifestOpen => "V2IO-OPEN-MANIFEST-PAIR-OPEN", EagerOpen, Manifest, FileOpen, false, false, false, 2, Precommit, Prior, next[OpenManifestRead], terminal[UnchangedRefusal];
    OpenManifestRead => "V2IO-OPEN-MANIFEST-PAIR-READ", EagerOpen, Manifest, BoundedRead, false, false, false, 2, Precommit, Prior, next[OpenManifestValidate], terminal[UnchangedRefusal];
    OpenManifestValidate => "V2IO-OPEN-MANIFEST-PAIR-VALIDATE", EagerOpen, Manifest, CompleteValidation, false, false, false, 2, Precommit, Prior, next[OpenAuthorityFamilies], terminal[UnchangedRefusal];
    OpenAuthorityFamilies => "V2IO-OPEN-REGISTRY-RETRY-RECOVERY-VALIDATE", EagerOpen, Registry, CompleteValidation, false, false, false, 9, Precommit, Prior, next[OpenActiveValidate], terminal[UnchangedRefusal];
    OpenActiveValidate => "V2IO-OPEN-ACTIVE-CHECKPOINT-VALIDATE", EagerOpen, ActiveJournal, CompleteValidation, false, false, false, 2, Precommit, Prior, next[OpenCatalogValidate], terminal[UnchangedRefusal];
    OpenCatalogValidate => "V2IO-OPEN-CATALOG-VALIDATE", EagerOpen, Catalog, CompleteValidation, false, false, false, 3, Precommit, Prior, next[OpenIntentStagingValidate, OpenPairRawOpen], terminal[UnchangedRefusal];
    OpenIntentStagingValidate => "V2IO-OPEN-INTENT-STAGING-VALIDATE", EagerOpen, Intent, CompleteValidation, false, false, false, 5, Precommit, Prior, next[OpenConvergenceRemove, OpenPairRawOpen], terminal[UnchangedRefusal];
    OpenPairRawOpen => "V2IO-OPEN-PAIR-RAW-OPEN", EagerOpen, RawPair, FileOpen, false, false, false, 64, Precommit, Prior, next[OpenPairRawMetadata], terminal[UnchangedRefusal];
    OpenPairRawMetadata => "V2IO-OPEN-PAIR-RAW-METADATA", EagerOpen, RawPair, MetadataRead, false, false, false, 64, Precommit, Prior, next[OpenPairRawRead], terminal[UnchangedRefusal];
    OpenPairRawRead => "V2IO-OPEN-PAIR-RAW-READ", EagerOpen, RawPair, BoundedRead, false, false, false, 524_288, Precommit, Prior, next[OpenPairRawRead, OpenPairRawValidate], terminal[UnchangedRefusal];
    OpenPairRawValidate => "V2IO-OPEN-PAIR-RAW-VALIDATE", EagerOpen, RawPair, CompleteValidation, false, false, false, 64, Precommit, Prior, next[OpenPairSegmentOpen], terminal[UnchangedRefusal];
    OpenPairSegmentOpen => "V2IO-OPEN-PAIR-SEGMENT-OPEN", EagerOpen, SegmentPair, FileOpen, false, false, false, 64, Precommit, Prior, next[OpenPairSegmentMetadata], terminal[UnchangedRefusal];
    OpenPairSegmentMetadata => "V2IO-OPEN-PAIR-SEGMENT-METADATA", EagerOpen, SegmentPair, MetadataRead, false, false, false, 64, Precommit, Prior, next[OpenPairSegmentRead], terminal[UnchangedRefusal];
    OpenPairSegmentRead => "V2IO-OPEN-PAIR-SEGMENT-READ", EagerOpen, SegmentPair, BoundedRead, false, false, false, 622_976, Precommit, Prior, next[OpenPairSegmentRead, OpenPairSegmentValidate], terminal[UnchangedRefusal];
    OpenPairSegmentValidate => "V2IO-OPEN-PAIR-SEGMENT-VALIDATE", EagerOpen, SegmentPair, CompleteValidation, false, false, false, 64, Precommit, Prior, next[OpenPairRelation], terminal[UnchangedRefusal];
    OpenPairRelation => "V2IO-OPEN-PAIR-RELATION-VALIDATE", EagerOpen, StoreAuthority, RelationValidation, false, false, false, 64, Precommit, Prior, next[OpenPairRawOpen, OpenLockCreate], terminal[UnchangedRefusal];
    OpenLockCreate => "V2IO-OPEN-STABLE-LOCK-CREATE", EagerOpen, StableLock, LockCreate, true, false, true, 1, Postcommit, Committed, next[OpenLockOpen], terminal[UnchangedRefusal];
    OpenLockOpen => "V2IO-OPEN-STABLE-LOCK-OPEN", EagerOpen, StableLock, LockOpen, false, false, false, 1, Postcommit, Committed, next[OpenLockAcquire], terminal[UnchangedRefusal];
    OpenLockAcquire => "V2IO-OPEN-STABLE-LOCK-ACQUIRE", EagerOpen, StableLock, LockAcquire, false, false, false, 1, Postcommit, Committed, next[OpenFinalRelation], terminal[UnchangedRefusal];
    OpenConvergenceRemove => "V2IO-OPEN-CONVERGENCE-REMOVE", EagerOpen, Intent, Remove, true, false, true, 12, Postcommit, Committed, next[OpenConvergenceSync], terminal[UnchangedRefusal];
    OpenConvergenceSync => "V2IO-OPEN-CONVERGENCE-DIRECTORY-SYNC", EagerOpen, RootInventory, Synchronize, true, false, true, 12, Postcommit, Committed, next[OpenIntentStagingValidate, OpenPairRawOpen], terminal[UnchangedRefusal];
    OpenFinalRelation => "V2IO-OPEN-FINAL-RELATION-VALIDATE", EagerOpen, StoreAuthority, RelationValidation, false, false, false, 1, Postcommit, Committed, next[OpenAdopt], terminal[UnchangedRefusal];
    OpenAdopt => "V2IO-OPEN-WRITABLE-HANDLE-ADOPT", EagerOpen, StoreAuthority, Adopt, true, false, false, 1, Postcommit, Committed, next[], terminal[CompleteSuccess]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FaultDescriptor {
    pub(super) id: FaultId,
    pub(super) phase: PhaseId,
    pub(super) artifact: Artifact,
    pub(super) operation: Operation,
    pub(super) mutation: bool,
    pub(super) short_write: bool,
    pub(super) pressure: bool,
    pub(super) maximum_occurrence: u32,
    pub(super) commit_side: CommitSide,
    pub(super) expected_root: RootClassification,
    pub(super) successors: &'static [FaultId],
    pub(super) terminals: &'static [TerminalState],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FaultSelection {
    pub(super) id: FaultId,
    pub(super) occurrence: NonZeroU32,
    pub(super) mode: FaultMode,
    pub(super) pressure: PressureKind,
}

impl FaultSelection {
    pub(super) fn new(
        id: FaultId,
        occurrence: NonZeroU32,
        mode: FaultMode,
        pressure: PressureKind,
    ) -> Result<Self> {
        let descriptor = id.descriptor();
        if occurrence.get() > descriptor.maximum_occurrence
            || mode == FaultMode::None
            || (mode == FaultMode::ShortPartialWrite && !descriptor.short_write)
            || (pressure != PressureKind::None && !descriptor.pressure)
            || (pressure != PressureKind::None
                && !matches!(
                    mode,
                    FaultMode::PreOperationError | FaultMode::ShortPartialWrite
                ))
        {
            return Err(EvidenceError::InvalidHarness);
        }
        Ok(Self {
            id,
            occurrence,
            mode,
            pressure,
        })
    }
}

pub(super) fn validate_registry() -> Result<()> {
    let all = FaultId::ALL.iter().copied().collect::<BTreeSet<_>>();
    if all.len() != FaultId::ALL.len() || FaultId::ALL.len() < 150 {
        return Err(EvidenceError::InvalidHarness);
    }
    let mut literals = BTreeSet::new();
    let mut reachable = BTreeSet::new();
    let mut frontier = vec![
        FaultId::P0DirectoryOpen,
        FaultId::OpenDirectoryOpen,
        FaultId::RbRawValidate,
    ];
    while let Some(id) = frontier.pop() {
        if reachable.insert(id) {
            frontier.extend_from_slice(id.descriptor().successors);
        }
    }
    for id in FaultId::ALL {
        let descriptor = id.descriptor();
        if !literals.insert(id.as_str())
            || FaultId::parse(id.as_str())? != *id
            || descriptor.id != *id
            || descriptor.maximum_occurrence == 0
            || descriptor
                .successors
                .iter()
                .any(|successor| !all.contains(successor))
            || (descriptor.successors.is_empty() && descriptor.terminals.is_empty())
            || descriptor.terminals.iter().collect::<BTreeSet<_>>().len()
                != descriptor.terminals.len()
            || (descriptor.short_write && descriptor.operation != Operation::Write)
            || (descriptor.pressure && !descriptor.mutation)
            || (descriptor.commit_side == CommitSide::Postcommit
                && descriptor.expected_root != RootClassification::Committed)
        {
            return Err(EvidenceError::InvalidHarness);
        }
    }
    if reachable != all {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(())
}

pub(super) fn applicability_rows() -> Result<Vec<FaultSelection>> {
    let mut count = 0_usize;
    for id in FaultId::ALL {
        let descriptor = id.descriptor();
        count = count.checked_add(2).ok_or(EvidenceError::Bounds)?;
        if descriptor.short_write {
            count = count.checked_add(1).ok_or(EvidenceError::Bounds)?;
        }
        if descriptor.pressure {
            count = count.checked_add(2).ok_or(EvidenceError::Bounds)?;
            if descriptor.short_write {
                count = count.checked_add(2).ok_or(EvidenceError::Bounds)?;
            }
        }
    }
    let mut rows = Vec::new();
    rows.try_reserve_exact(count)
        .map_err(|_| EvidenceError::Bounds)?;
    for id in FaultId::ALL {
        let occurrence = NonZeroU32::MIN;
        let descriptor = id.descriptor();
        rows.push(FaultSelection::new(
            *id,
            occurrence,
            FaultMode::PreOperationError,
            PressureKind::None,
        )?);
        rows.push(FaultSelection::new(
            *id,
            occurrence,
            FaultMode::ChildCrashAfterSuccess,
            PressureKind::None,
        )?);
        if descriptor.short_write {
            rows.push(FaultSelection::new(
                *id,
                occurrence,
                FaultMode::ShortPartialWrite,
                PressureKind::None,
            )?);
        }
        if descriptor.pressure {
            for pressure in [PressureKind::StorageFull, PressureKind::QuotaExceeded] {
                rows.push(FaultSelection::new(
                    *id,
                    occurrence,
                    FaultMode::PreOperationError,
                    pressure,
                )?);
                if descriptor.short_write {
                    rows.push(FaultSelection::new(
                        *id,
                        occurrence,
                        FaultMode::ShortPartialWrite,
                        pressure,
                    )?);
                }
            }
        }
    }
    if rows.len() != count {
        return Err(EvidenceError::InvalidHarness);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_closed_reachable_and_has_explicit_terminals() {
        validate_registry().expect("closed literal fault registry");
        assert!(FaultId::parse("V2IO-*").is_err());
        assert!(FaultId::parse("V2IO-P8-DYNAMIC").is_err());
    }

    #[test]
    fn every_literal_row_has_all_applicable_executor_targets() {
        let rows = applicability_rows().expect("precomputed applicability matrix");
        for id in FaultId::ALL {
            assert!(
                rows.iter()
                    .any(|row| row.id == *id && row.mode == FaultMode::PreOperationError)
            );
            assert!(
                rows.iter()
                    .any(|row| row.id == *id && row.mode == FaultMode::ChildCrashAfterSuccess)
            );
            let descriptor = id.descriptor();
            assert_eq!(
                rows.iter()
                    .any(|row| row.id == *id && row.mode == FaultMode::ShortPartialWrite),
                descriptor.short_write
            );
            for pressure in [PressureKind::StorageFull, PressureKind::QuotaExceeded] {
                assert_eq!(
                    rows.iter()
                        .any(|row| row.id == *id && row.pressure == pressure),
                    descriptor.pressure
                );
            }
        }
    }

    #[test]
    fn illegal_applicability_is_rejected() {
        assert!(
            FaultSelection::new(
                FaultId::P0DirectoryOpen,
                NonZeroU32::MIN,
                FaultMode::ShortPartialWrite,
                PressureKind::None,
            )
            .is_err()
        );
        assert!(
            FaultSelection::new(
                FaultId::P0DirectoryOpen,
                NonZeroU32::MIN,
                FaultMode::PreOperationError,
                PressureKind::StorageFull,
            )
            .is_err()
        );
    }
}
