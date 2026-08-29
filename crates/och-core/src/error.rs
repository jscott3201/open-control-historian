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
        })
    }
}

impl std::error::Error for ModelError {}
