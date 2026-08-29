//! Explicit producer epoch and sequence ordering.

use crate::ModelError;
use core::fmt;
use core::str::FromStr;

pub(crate) fn parse_canonical_u128(text: &str) -> Result<u128, ModelError> {
    let bytes = text.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 39
        || (bytes.len() > 1 && bytes[0] == b'0')
        || !bytes.iter().all(u8::is_ascii_digit)
    {
        return Err(ModelError::InvalidCanonicalDecimal);
    }

    let mut value = 0_u128;
    for digit in bytes {
        value = value
            .checked_mul(10)
            .and_then(|current| current.checked_add(u128::from(digit - b'0')))
            .ok_or(ModelError::InvalidCanonicalDecimal)?;
    }
    Ok(value)
}

macro_rules! producer_number {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        ///
        /// The complete `u128` range is supported. Text uses canonical unsigned
        /// decimal: no sign, whitespace, or leading zero except the value `0`.
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u128);

        impl $name {
            /// Constructs the number from its exact numeric value.
            #[must_use]
            pub const fn new(value: u128) -> Self {
                Self(value)
            }

            /// Returns the exact numeric value.
            #[must_use]
            pub const fn get(self) -> u128 {
                self.0
            }

            /// Parses canonical unsigned decimal text.
            ///
            /// # Errors
            ///
            /// Returns [`ModelError::InvalidCanonicalDecimal`] for non-canonical
            /// text or a value outside the full `u128` range.
            pub fn parse(text: &str) -> Result<Self, ModelError> {
                parse_canonical_u128(text).map(Self)
            }
        }

        impl From<u128> for $name {
            fn from(value: u128) -> Self {
                Self::new(value)
            }
        }

        impl FromStr for $name {
            type Err = ModelError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                Self::parse(text)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

producer_number!(
    ProducerEpoch,
    "A producer-defined epoch that separates sequence number domains."
);
producer_number!(
    ProducerSequence,
    "A producer-defined sequence number within one epoch."
);

/// The independent authority for producer order.
///
/// UUID order and timestamps never replace this pair. Numeric order compares
/// epoch first and sequence second.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProducerPosition {
    epoch: ProducerEpoch,
    sequence: ProducerSequence,
}

impl ProducerPosition {
    /// Constructs an explicit producer position.
    #[must_use]
    pub const fn new(epoch: ProducerEpoch, sequence: ProducerSequence) -> Self {
        Self { epoch, sequence }
    }

    /// Returns the producer epoch.
    #[must_use]
    pub const fn epoch(self) -> ProducerEpoch {
        self.epoch
    }

    /// Returns the sequence within the epoch.
    #[must_use]
    pub const fn sequence(self) -> ProducerSequence {
        self.sequence
    }
}
