//! Sanitized failures from canonical model construction and validation.

use core::fmt;

/// A closed, sanitized model-construction or validation failure.
///
/// Variants identify the violated contract without retaining or displaying the
/// caller's input. This makes errors safe to log even when input is hostile.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ModelError {
    /// UUID text, version, or variant is invalid.
    InvalidIdentity,
    /// Exact text exceeds its Unicode-scalar bound.
    InvalidExactText,
    /// A portable printable-ASCII token is empty, invalid, or too long.
    InvalidPortableToken,
    /// A content format is empty, invalid, or too long.
    InvalidContentFormat,
    /// A retry key is empty, invalid, or too long.
    InvalidRetryKey,
    /// A canonical unsigned decimal is malformed or outside `u128`.
    InvalidCanonicalDecimal,
    /// A timestamp nanosecond fraction is outside the normalized range.
    InvalidNanosecond,
    /// A timestamp cannot be represented as an exact integer millisecond.
    InexactUnixMilliseconds,
    /// A timestamp-to-millisecond conversion overflowed `i64`.
    UnixMillisecondsOverflow,
    /// A time interval is empty or reversed.
    EmptyTimeInterval,
    /// A gap's producer-sequence interval is empty or reversed.
    EmptyGap,
    /// Native status contains more than the permitted number of tokens.
    TooManyNativeStatusTokens,
    /// An observed envelope exceeds its observation bound.
    TooManyObservations,
    /// An observed envelope exceeds its gap bound.
    TooManyGaps,
    /// Observed evidence has neither an observation nor a gap.
    EmptyObservedEvidence,
    /// Two observations in one envelope have the same observation identity.
    DuplicateObservationId,
    /// Only some observations in an envelope have producer positions.
    MixedProducerPositions,
    /// Observation producer positions are not strictly increasing.
    MisorderedProducerPositions,
    /// Gaps are not ordered by epoch and starting sequence.
    MisorderedGaps,
    /// Two gaps overlap within one producer epoch.
    OverlappingGaps,
    /// An observation producer position falls within a declared gap.
    ObservationInsideGap,
    /// An interval-mode observation lacks interval metadata.
    MissingObservationInterval,
    /// A non-interval-mode observation contains interval metadata.
    UnexpectedObservationInterval,
    /// No-change evidence was supplied for a mode other than change-only.
    InvalidNoChangeMode,
    /// A declaration reference is empty, contains a control, or exceeds its bound.
    InvalidDeclarationReference,
    /// Declaration revision zero is reserved and therefore invalid.
    InvalidDeclarationRevision,
    /// A new series would exceed the registry's total-series bound.
    RegistrySeriesCapacityExceeded,
    /// A new declaration would exceed the registry's retained-revision bound.
    RegistryRevisionCapacityExceeded,
    /// The series identity is already registered with different declaration input.
    SeriesAlreadyRegistered,
    /// The requested series identity is not registered.
    SeriesNotFound,
    /// The requested series is terminally retired.
    SeriesRetired,
    /// The expected declaration revision is not the active revision.
    StaleDeclarationRevision,
    /// A requested revision does not change the declaration payload.
    DeclarationUnchanged,
    /// Envelope series metadata differs from the active declaration.
    SeriesMetadataMismatch,
    /// An observation value does not match the active declared value family.
    ObservationValueFamilyMismatch,
    /// Source schema version zero is reserved and invalid.
    InvalidSourceSchemaVersion,
    /// Capture completion precedes capture start.
    CaptureRunTimeOrder,
    /// Endpoint evidence does not link to its system evidence.
    SourceEndpointSystemMismatch,
    /// Capture-run evidence does not link to its endpoint evidence.
    CaptureRunEndpointMismatch,
    /// Snapshot evidence does not link to its capture-run evidence.
    SourceSnapshotRunMismatch,
    /// Canonical source admission requires a projection-bearing declaration.
    SourceProjectionRequired,
    /// Source interval classification differs from canonical envelope evidence.
    SourceIntervalMismatch,
    /// Capture lifecycle source fields differ from the declaration binding.
    SourceLifecycleBindingMismatch,
    /// Admission retry series or producer scope differs from the bound envelope.
    AdmissionRetryScopeMismatch,
    /// Source observation contexts exceed their fixed bound.
    TooManySourceObservationContexts,
    /// Source observation context count differs from canonical observations.
    SourceObservationCountMismatch,
    /// Source observation context names a different canonical observation.
    SourceObservationAssociationMismatch,
    /// Source gap contexts exceed their fixed bound.
    TooManySourceGapContexts,
    /// Source gap context count differs from canonical gaps.
    SourceGapCountMismatch,
    /// Source record ordinals are not strictly increasing.
    MisorderedSourceRecordOrdinals,
    /// Two source or capture roles reuse one evidence identity.
    DuplicateSourceEvidenceId,
    /// A raw record does not link to the retained capture snapshot.
    SourceRawSnapshotMismatch,
    /// A normalized record does not link to its paired raw record.
    SourceNormalizedRawMismatch,
    /// A normalized record does not link to its paired source observation.
    SourceNormalizedObservationMismatch,
    /// Raw-record idempotency content differs from the raw artifact content.
    SourceRawIdempotencyMismatch,
    /// Observation interpretation context differs from the bound declaration.
    SourceInterpretationMismatch,
    /// Source gap range or order differs from canonical gap evidence.
    SourceGapMismatch,
}

impl fmt::Display for ModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidIdentity => "invalid canonical UUIDv7 identity",
            Self::InvalidExactText => "exact text exceeds its bound",
            Self::InvalidPortableToken => "invalid bounded printable-ASCII token",
            Self::InvalidContentFormat => "invalid bounded lowercase content format",
            Self::InvalidRetryKey => "invalid bounded retry key",
            Self::InvalidCanonicalDecimal => "invalid canonical unsigned decimal",
            Self::InvalidNanosecond => "invalid normalized nanosecond fraction",
            Self::InexactUnixMilliseconds => "timestamp is not an exact Unix millisecond",
            Self::UnixMillisecondsOverflow => "Unix millisecond conversion overflowed",
            Self::EmptyTimeInterval => "time interval must be non-empty and half-open",
            Self::EmptyGap => "gap must be non-empty and half-open",
            Self::TooManyNativeStatusTokens => "native status token bound exceeded",
            Self::TooManyObservations => "observation bound exceeded",
            Self::TooManyGaps => "gap bound exceeded",
            Self::EmptyObservedEvidence => "observed evidence is empty",
            Self::DuplicateObservationId => "duplicate observation identity",
            Self::MixedProducerPositions => "mixed producer-position presence",
            Self::MisorderedProducerPositions => "producer positions are misordered",
            Self::MisorderedGaps => "gaps are misordered",
            Self::OverlappingGaps => "gaps overlap",
            Self::ObservationInsideGap => "observation position falls inside a gap",
            Self::MissingObservationInterval => "interval-mode observation lacks an interval",
            Self::UnexpectedObservationInterval => "non-interval-mode observation has an interval",
            Self::InvalidNoChangeMode => "no-change evidence requires change-only mode",
            Self::InvalidDeclarationReference => "invalid bounded declaration reference",
            Self::InvalidDeclarationRevision => "invalid declaration revision",
            Self::RegistrySeriesCapacityExceeded => "series registry capacity exceeded",
            Self::RegistryRevisionCapacityExceeded => "declaration revision capacity exceeded",
            Self::SeriesAlreadyRegistered => "series identity is already registered",
            Self::SeriesNotFound => "series identity is not registered",
            Self::SeriesRetired => "series is terminally retired",
            Self::StaleDeclarationRevision => "declaration revision is stale",
            Self::DeclarationUnchanged => "declaration payload is unchanged",
            Self::SeriesMetadataMismatch => "envelope series metadata mismatch",
            Self::ObservationValueFamilyMismatch => "observation value family mismatch",
            Self::InvalidSourceSchemaVersion => "invalid source schema version",
            Self::CaptureRunTimeOrder => "capture completion precedes capture start",
            Self::SourceEndpointSystemMismatch => "source endpoint system link mismatch",
            Self::CaptureRunEndpointMismatch => "capture run endpoint link mismatch",
            Self::SourceSnapshotRunMismatch => "source snapshot run link mismatch",
            Self::SourceProjectionRequired => "source projection is required",
            Self::SourceIntervalMismatch => "source interval classification mismatch",
            Self::SourceLifecycleBindingMismatch => "source lifecycle binding mismatch",
            Self::AdmissionRetryScopeMismatch => "admission retry scope mismatch",
            Self::TooManySourceObservationContexts => "source observation context bound exceeded",
            Self::SourceObservationCountMismatch => "source observation count mismatch",
            Self::SourceObservationAssociationMismatch => "source observation association mismatch",
            Self::TooManySourceGapContexts => "source gap context bound exceeded",
            Self::SourceGapCountMismatch => "source gap count mismatch",
            Self::MisorderedSourceRecordOrdinals => "source record ordinals are misordered",
            Self::DuplicateSourceEvidenceId => "duplicate source evidence identity",
            Self::SourceRawSnapshotMismatch => "source raw record snapshot link mismatch",
            Self::SourceNormalizedRawMismatch => "normalized record raw link mismatch",
            Self::SourceNormalizedObservationMismatch => {
                "normalized record observation link mismatch"
            }
            Self::SourceRawIdempotencyMismatch => "source raw idempotency content mismatch",
            Self::SourceInterpretationMismatch => "source interpretation context mismatch",
            Self::SourceGapMismatch => "source gap evidence mismatch",
        })
    }
}

impl std::error::Error for ModelError {}
