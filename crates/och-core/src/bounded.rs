//! Bounded exact text and portable token primitives.

use crate::ModelError;
use core::fmt;

/// Maximum Unicode scalar values in [`ExactText`].
pub const MAX_TEXT_SCALARS: usize = 4_096;
/// Maximum bytes in portable status, state, and reason tokens.
pub const MAX_PORTABLE_TOKEN_BYTES: usize = 256;
/// Maximum bytes in a content format token.
pub const MAX_CONTENT_FORMAT_BYTES: usize = 64;
/// Maximum bytes in a retry key.
pub const MAX_RETRY_KEY_BYTES: usize = 128;

fn validate_printable_ascii(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.bytes().all(|byte| (b' '..=b'~').contains(&byte))
}

macro_rules! portable_token {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        ///
        /// Values are preserved exactly and contain 1–256 printable ASCII bytes.
        #[derive(Clone, Debug, Eq, Hash, PartialEq)]
        pub struct $name(String);

        impl $name {
            /// Validates and retains a portable token without normalization.
            ///
            /// # Errors
            ///
            /// Returns [`ModelError::InvalidPortableToken`] when the value is
            /// empty, longer than 256 bytes, non-ASCII, or contains a control.
            pub fn new(value: String) -> Result<Self, ModelError> {
                if !validate_printable_ascii(&value, MAX_PORTABLE_TOKEN_BYTES) {
                    return Err(ModelError::InvalidPortableToken);
                }
                Ok(Self(value))
            }

            /// Borrows the exact token.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes this token and returns its exact text.
            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = ModelError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

portable_token!(StateClass, "A state's explicit vocabulary or class token.");
portable_token!(StateMember, "A member token within a state class.");
portable_token!(
    NativeStatusToken,
    "One opaque producer-native status token whose meaning is not normalized."
);
portable_token!(
    UnavailableReason,
    "An optional opaque reason attached only to an unavailable value."
);

/// Exact, unnormalized text containing at most 4,096 Unicode scalar values.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExactText(String);

impl ExactText {
    /// Retains text exactly after checking its Unicode-scalar bound.
    ///
    /// Empty text is a valid exact value. No Unicode normalization is applied.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidExactText`] when more than 4,096 Unicode
    /// scalar values are present.
    pub fn new(value: String) -> Result<Self, ModelError> {
        if value.chars().take(MAX_TEXT_SCALARS + 1).count() > MAX_TEXT_SCALARS {
            return Err(ModelError::InvalidExactText);
        }
        Ok(Self(value))
    }

    /// Borrows the exact, unnormalized text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes this value and returns its exact text.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl TryFrom<String> for ExactText {
    type Error = ModelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for ExactText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A lowercase, bounded token naming an externally defined content format.
///
/// The accepted grammar is 1–64 printable non-space ASCII bytes with no
/// uppercase letters. Punctuation is retained so external format vocabularies
/// can use values such as `application/octet-stream`; this crate does not assign
/// meaning to any format.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ContentFormat(String);

impl ContentFormat {
    /// Validates and retains a lowercase content format token.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidContentFormat`] when the token is empty,
    /// exceeds 64 bytes, contains non-ASCII/control/space bytes, or has an ASCII
    /// uppercase letter.
    pub fn new(value: String) -> Result<Self, ModelError> {
        let valid = !value.is_empty()
            && value.len() <= MAX_CONTENT_FORMAT_BYTES
            && value
                .bytes()
                .all(|byte| (b'!'..=b'~').contains(&byte) && !byte.is_ascii_uppercase());
        if !valid {
            return Err(ModelError::InvalidContentFormat);
        }
        Ok(Self(value))
    }

    /// Borrows the exact format token.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes this token and returns its exact text.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl TryFrom<String> for ContentFormat {
    type Error = ModelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for ContentFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A caller-supplied retry key containing 1–128 printable ASCII bytes.
///
/// The key is opaque and is never derived, hashed, or interpreted by the model.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct RetryKey(String);

impl RetryKey {
    /// Validates and retains an opaque retry key.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidRetryKey`] when the key is empty, exceeds
    /// 128 bytes, is non-ASCII, or contains a control byte.
    pub fn new(value: String) -> Result<Self, ModelError> {
        if !validate_printable_ascii(&value, MAX_RETRY_KEY_BYTES) {
            return Err(ModelError::InvalidRetryKey);
        }
        Ok(Self(value))
    }

    /// Borrows the exact key for explicit comparison or transport by its owner.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the key and returns its exact text.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl TryFrom<String> for RetryKey {
    type Error = ModelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Debug for RetryKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetryKey([REDACTED])")
    }
}
