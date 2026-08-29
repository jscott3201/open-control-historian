//! Closed normalized quality and independent opaque native status.

use crate::{ModelError, NativeStatusToken};

/// Maximum ordered tokens retained in [`NativeStatus`].
pub const MAX_NATIVE_STATUS_TOKENS: usize = 16;

/// The closed normalized quality level.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QualityLevel {
    /// The quality is unknown.
    Unknown,
    /// The value is known to be good.
    Good,
    /// The value has uncertain quality.
    Uncertain,
    /// The value is known to be bad.
    Bad,
    /// Quality has deliberately not been evaluated.
    NotEvaluated,
}

/// Independent normalized quality flags.
///
/// Flags do not imply or rewrite the [`QualityLevel`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct QualityFlags(u8);

impl QualityFlags {
    const STALE: u8 = 1 << 0;
    const INVALID: u8 = 1 << 1;
    const SUBSTITUTED: u8 = 1 << 2;
    const OVERRIDDEN: u8 = 1 << 3;
    const OUT_OF_SERVICE: u8 = 1 << 4;
    const COMMUNICATION_FAILURE: u8 = 1 << 5;

    /// Constructs an empty set of independent flags.
    #[must_use]
    pub const fn none() -> Self {
        Self(0)
    }

    /// Returns flags with stale evidence set to `enabled`.
    #[must_use]
    pub const fn with_stale(mut self, enabled: bool) -> Self {
        self.set(Self::STALE, enabled);
        self
    }

    /// Returns flags with invalid evidence set to `enabled`.
    #[must_use]
    pub const fn with_invalid(mut self, enabled: bool) -> Self {
        self.set(Self::INVALID, enabled);
        self
    }

    /// Returns flags with substituted evidence set to `enabled`.
    #[must_use]
    pub const fn with_substituted(mut self, enabled: bool) -> Self {
        self.set(Self::SUBSTITUTED, enabled);
        self
    }

    /// Returns flags with overridden evidence set to `enabled`.
    #[must_use]
    pub const fn with_overridden(mut self, enabled: bool) -> Self {
        self.set(Self::OVERRIDDEN, enabled);
        self
    }

    /// Returns flags with out-of-service evidence set to `enabled`.
    #[must_use]
    pub const fn with_out_of_service(mut self, enabled: bool) -> Self {
        self.set(Self::OUT_OF_SERVICE, enabled);
        self
    }

    /// Returns flags with communication-failure evidence set to `enabled`.
    #[must_use]
    pub const fn with_communication_failure(mut self, enabled: bool) -> Self {
        self.set(Self::COMMUNICATION_FAILURE, enabled);
        self
    }

    const fn set(&mut self, flag: u8, enabled: bool) {
        if enabled {
            self.0 |= flag;
        } else {
            self.0 &= !flag;
        }
    }

    /// Reports stale evidence.
    #[must_use]
    pub const fn stale(self) -> bool {
        self.0 & Self::STALE != 0
    }

    /// Reports invalid evidence.
    #[must_use]
    pub const fn invalid(self) -> bool {
        self.0 & Self::INVALID != 0
    }

    /// Reports substituted evidence.
    #[must_use]
    pub const fn substituted(self) -> bool {
        self.0 & Self::SUBSTITUTED != 0
    }

    /// Reports overridden evidence.
    #[must_use]
    pub const fn overridden(self) -> bool {
        self.0 & Self::OVERRIDDEN != 0
    }

    /// Reports out-of-service evidence.
    #[must_use]
    pub const fn out_of_service(self) -> bool {
        self.0 & Self::OUT_OF_SERVICE != 0
    }

    /// Reports communication-failure evidence.
    #[must_use]
    pub const fn communication_failure(self) -> bool {
        self.0 & Self::COMMUNICATION_FAILURE != 0
    }
}

/// Normalized quality as one closed level plus independent flags.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Quality {
    level: QualityLevel,
    flags: QualityFlags,
}

impl Quality {
    /// Constructs normalized quality without deriving one component from another.
    #[must_use]
    pub const fn new(level: QualityLevel, flags: QualityFlags) -> Self {
        Self { level, flags }
    }

    /// Returns the normalized level.
    #[must_use]
    pub const fn level(self) -> QualityLevel {
        self.level
    }

    /// Returns all independent flags.
    #[must_use]
    pub const fn flags(self) -> QualityFlags {
        self.flags
    }
}

/// Absent or ordered opaque producer-native status.
///
/// Unknown and duplicate tokens are retained exactly. Native status is not
/// interpreted as normalized [`Quality`].
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct NativeStatus(Vec<NativeStatusToken>);

impl NativeStatus {
    /// Constructs ordered native status from at most 16 tokens.
    ///
    /// An empty vector represents absent native status. The length is rejected
    /// before any model-owned secondary allocation.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::TooManyNativeStatusTokens`] above 16 tokens.
    pub fn new(tokens: Vec<NativeStatusToken>) -> Result<Self, ModelError> {
        if tokens.len() > MAX_NATIVE_STATUS_TOKENS {
            return Err(ModelError::TooManyNativeStatusTokens);
        }
        Ok(Self(tokens))
    }

    /// Constructs absent native status.
    #[must_use]
    pub const fn absent() -> Self {
        Self(Vec::new())
    }

    /// Returns the ordered opaque tokens.
    #[must_use]
    pub fn tokens(&self) -> &[NativeStatusToken] {
        &self.0
    }

    /// Reports whether native status is absent.
    #[must_use]
    pub fn is_absent(&self) -> bool {
        self.0.is_empty()
    }
}
