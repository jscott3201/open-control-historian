//! Exact observation values and externally supplied content identity.

use crate::bounded::{ContentFormat, ExactText, StateClass, StateMember, UnavailableReason};
use crate::position::parse_canonical_u128;
use crate::{ArtifactId, ModelError};
use core::fmt;
use core::str::FromStr;

/// The exact IEEE 754 binary64 bit pattern of a real value.
///
/// Equality, hashing, and order use the underlying `u64` bits. NaN payloads and
/// signed zero therefore remain distinct; this type deliberately has no
/// arithmetic or numerical-order operations.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RealBits(u64);

impl RealBits {
    /// Retains the exact bit pattern of a floating-point value.
    #[must_use]
    pub const fn from_f64(value: f64) -> Self {
        Self(value.to_bits())
    }

    /// Reconstructs a floating-point value with exactly the retained bits.
    #[must_use]
    pub const fn to_f64(self) -> f64 {
        f64::from_bits(self.0)
    }

    /// Constructs directly from IEEE 754 binary64 bits.
    #[must_use]
    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    /// Returns the exact IEEE 754 binary64 bits.
    #[must_use]
    pub const fn to_bits(self) -> u64 {
        self.0
    }
}

/// A canonical externally supplied content version over the full `u128` range.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContentVersion(u128);

impl ContentVersion {
    /// Constructs a version from its exact supported decimal value.
    #[must_use]
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    /// Returns the exact version value.
    #[must_use]
    pub const fn get(self) -> u128 {
        self.0
    }

    /// Parses canonical unsigned decimal text.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidCanonicalDecimal`] for signs, whitespace,
    /// non-digits, leading zeros except `0`, or overflow beyond `u128`.
    pub fn parse(text: &str) -> Result<Self, ModelError> {
        parse_canonical_u128(text).map(Self)
    }
}

impl From<u128> for ContentVersion {
    fn from(value: u128) -> Self {
        Self::new(value)
    }
}

impl FromStr for ContentVersion {
    type Err = ModelError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::parse(text)
    }
}

impl fmt::Display for ContentVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Immutable externally supplied content identity.
///
/// The model retains a lowercase format token, canonical version, and exact
/// SHA-256 digest bytes. It neither hashes content nor defines content bytes or a
/// wire representation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ContentIdentity {
    format: ContentFormat,
    version: ContentVersion,
    sha256: [u8; 32],
}

impl ContentIdentity {
    /// Constructs an externally computed immutable content identity.
    #[must_use]
    pub const fn new(format: ContentFormat, version: ContentVersion, sha256: [u8; 32]) -> Self {
        Self {
            format,
            version,
            sha256,
        }
    }

    /// Returns the externally defined format.
    #[must_use]
    pub const fn format(&self) -> &ContentFormat {
        &self.format
    }

    /// Returns the canonical content version.
    #[must_use]
    pub const fn version(&self) -> ContentVersion {
        self.version
    }

    /// Returns the exact SHA-256 digest bytes without recomputing them.
    #[must_use]
    pub const fn sha256(&self) -> &[u8; 32] {
        &self.sha256
    }
}

/// A nominal artifact reference qualified by immutable content identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ArtifactReference {
    artifact_id: ArtifactId,
    content: ContentIdentity,
}

impl ArtifactReference {
    /// Constructs an artifact reference without fetching or hashing content.
    #[must_use]
    pub const fn new(artifact_id: ArtifactId, content: ContentIdentity) -> Self {
        Self {
            artifact_id,
            content,
        }
    }

    /// Returns the nominal artifact identity.
    #[must_use]
    pub const fn artifact_id(&self) -> ArtifactId {
        self.artifact_id
    }

    /// Returns the externally supplied immutable content identity.
    #[must_use]
    pub const fn content(&self) -> &ContentIdentity {
        &self.content
    }
}

/// A state value with an explicit vocabulary/class and member.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StateValue {
    class: StateClass,
    member: StateMember,
}

impl StateValue {
    /// Constructs an exact state value without interpreting either token.
    #[must_use]
    pub const fn new(class: StateClass, member: StateMember) -> Self {
        Self { class, member }
    }

    /// Returns the explicit state class.
    #[must_use]
    pub const fn class(&self) -> &StateClass {
        &self.class
    }

    /// Returns the exact member within the class.
    #[must_use]
    pub const fn member(&self) -> &StateMember {
        &self.member
    }
}

/// Explicit unavailable content with an optional bounded opaque reason.
///
/// Unavailability is a value. It is not absence of an observation, bad quality,
/// a producer-sequence gap, or no-change evidence.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Unavailable {
    reason: Option<UnavailableReason>,
}

impl Unavailable {
    /// Constructs unavailable content with an optional opaque reason.
    #[must_use]
    pub const fn new(reason: Option<UnavailableReason>) -> Self {
        Self { reason }
    }

    /// Constructs unavailable content without a reason.
    #[must_use]
    pub const fn without_reason() -> Self {
        Self { reason: None }
    }

    /// Returns the optional opaque reason.
    #[must_use]
    pub const fn reason(&self) -> Option<&UnavailableReason> {
        self.reason.as_ref()
    }
}

/// An exact canonical observation value.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ExactValue {
    /// Exact IEEE 754 binary64 bits.
    Real(RealBits),
    /// Full signed 64-bit integer.
    Signed(i64),
    /// Full unsigned 64-bit integer.
    Unsigned(u64),
    /// Boolean value.
    Boolean(bool),
    /// Exact state class and member.
    State(StateValue),
    /// Exact unnormalized bounded text.
    Text(ExactText),
    /// Nominal artifact and immutable content identity.
    Artifact(ArtifactReference),
    /// Explicit unavailable content.
    Unavailable(Unavailable),
}
