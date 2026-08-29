//! Nominal, parse-only RFC 9562 `UUIDv7` identities.
//!
//! Identity families intentionally do not compare or convert across nominal
//! boundaries:
//!
//! ```compile_fail
//! use och_core::{ProducerId, SeriesId};
//!
//! let series: SeriesId = "01941f29-7c00-7000-8000-000000000001".parse().unwrap();
//! let _producer: ProducerId = series;
//! ```

use crate::ModelError;
use core::fmt;
use core::str::FromStr;

fn parse_uuid_v7(text: &str) -> Result<[u8; 16], ModelError> {
    let source = text.as_bytes();
    if source.len() != 36
        || source[8] != b'-'
        || source[13] != b'-'
        || source[18] != b'-'
        || source[23] != b'-'
    {
        return Err(ModelError::InvalidIdentity);
    }

    let mut bytes = [0_u8; 16];
    let mut source_index = 0;
    for destination in &mut bytes {
        while matches!(source_index, 8 | 13 | 18 | 23) {
            source_index += 1;
        }
        let high = decode_lower_hex(source[source_index])?;
        let low = decode_lower_hex(source[source_index + 1])?;
        *destination = (high << 4) | low;
        source_index += 2;
    }
    validate_uuid_v7(bytes)
}

fn decode_lower_hex(value: u8) -> Result<u8, ModelError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(ModelError::InvalidIdentity),
    }
}

fn validate_uuid_v7(bytes: [u8; 16]) -> Result<[u8; 16], ModelError> {
    if bytes[6] >> 4 != 7 || bytes[8] & 0b1100_0000 != 0b1000_0000 {
        return Err(ModelError::InvalidIdentity);
    }
    Ok(bytes)
}

fn display_uuid(bytes: &[u8; 16], formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            formatter.write_str("-")?;
        }
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

macro_rules! identity_type {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        ///
        /// The byte order is available only as a final deterministic identity
        /// tie-breaker. It does not establish producer order or freshness.
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 16]);

        impl $name {
            /// Parses canonical lowercase, hyphenated RFC 9562 `UUIDv7` text.
            ///
            /// # Errors
            ///
            /// Returns [`ModelError::InvalidIdentity`] for non-canonical text,
            /// a version other than 7, or a non-RFC variant.
            pub fn parse(text: &str) -> Result<Self, ModelError> {
                parse_uuid_v7(text).map(Self)
            }

            /// Validates and constructs an identity from network-order UUID bytes.
            ///
            /// # Errors
            ///
            /// Returns [`ModelError::InvalidIdentity`] unless the bytes carry
            /// UUID version 7 and the RFC variant.
            pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, ModelError> {
                validate_uuid_v7(bytes).map(Self)
            }

            /// Returns the UUID bytes in canonical network order.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }

            /// Consumes the identity and returns canonical network-order bytes.
            #[must_use]
            pub const fn into_bytes(self) -> [u8; 16] {
                self.0
            }
        }

        impl FromStr for $name {
            type Err = ModelError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                Self::parse(text)
            }
        }

        impl TryFrom<[u8; 16]> for $name {
            type Error = ModelError;

            fn try_from(bytes: [u8; 16]) -> Result<Self, Self::Error> {
                Self::from_bytes(bytes)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                display_uuid(&self.0, formatter)
            }
        }
    };
}

identity_type!(
    SeriesId,
    "The nominal identity of one immutable collection-mode series."
);
identity_type!(
    ProducerId,
    "The nominal identity of an observation producer."
);
identity_type!(ObservationId, "The nominal identity of one observation.");
identity_type!(ArtifactId, "The nominal identity of an external artifact.");
