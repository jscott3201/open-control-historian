//! Normalized timestamps and non-empty half-open time intervals.

use crate::ModelError;

/// A normalized signed Unix timestamp with nanosecond precision.
///
/// `unix_seconds` is the floor relative to the Unix epoch and `nanosecond` is
/// always in `0..1_000_000_000`. Consequently `-1 ms` is represented as second
/// `-1` plus `999_000_000` ns, not as a negative fractional field.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Timestamp {
    unix_seconds: i64,
    nanosecond: u32,
}

impl Timestamp {
    /// Nanoseconds in one normalized second.
    pub const NANOS_PER_SECOND: u32 = 1_000_000_000;

    /// Constructs a normalized timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidNanosecond`] when `nanosecond` is not less
    /// than one billion.
    pub const fn new(unix_seconds: i64, nanosecond: u32) -> Result<Self, ModelError> {
        if nanosecond >= Self::NANOS_PER_SECOND {
            return Err(ModelError::InvalidNanosecond);
        }
        Ok(Self {
            unix_seconds,
            nanosecond,
        })
    }

    /// Converts any signed Unix millisecond exactly using Euclidean arithmetic.
    #[must_use]
    pub fn from_unix_milliseconds(milliseconds: i64) -> Self {
        let unix_seconds = milliseconds.div_euclid(1_000);
        let millisecond_fraction = milliseconds.rem_euclid(1_000);
        let nanosecond = u32::try_from(millisecond_fraction).unwrap_or_default() * 1_000_000;
        Self {
            unix_seconds,
            nanosecond,
        }
    }

    /// Converts to signed Unix milliseconds without losing precision.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InexactUnixMilliseconds`] when the timestamp has
    /// sub-millisecond precision, or [`ModelError::UnixMillisecondsOverflow`]
    /// when the exact result is outside `i64`.
    pub fn to_unix_milliseconds(self) -> Result<i64, ModelError> {
        if !self.nanosecond.is_multiple_of(1_000_000) {
            return Err(ModelError::InexactUnixMilliseconds);
        }
        let milliseconds =
            i128::from(self.unix_seconds) * 1_000 + i128::from(self.nanosecond / 1_000_000);
        i64::try_from(milliseconds).map_err(|_| ModelError::UnixMillisecondsOverflow)
    }

    /// Returns the signed floor Unix second.
    #[must_use]
    pub const fn unix_seconds(self) -> i64 {
        self.unix_seconds
    }

    /// Returns the normalized nanosecond fraction.
    #[must_use]
    pub const fn nanosecond(self) -> u32 {
        self.nanosecond
    }
}

/// The source, receive, and policy-effective times of an observation.
///
/// No chronological relationship is required: source time can be unavailable,
/// and effective time is policy evidence rather than a derived freshness claim.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ObservationTimes {
    source: Option<Timestamp>,
    receive: Timestamp,
    effective: Timestamp,
}

impl ObservationTimes {
    /// Constructs observation times without imposing chronology.
    #[must_use]
    pub const fn new(source: Option<Timestamp>, receive: Timestamp, effective: Timestamp) -> Self {
        Self {
            source,
            receive,
            effective,
        }
    }

    /// Returns the optional producer source time.
    #[must_use]
    pub const fn source(self) -> Option<Timestamp> {
        self.source
    }

    /// Returns the receive time.
    #[must_use]
    pub const fn receive(self) -> Timestamp {
        self.receive
    }

    /// Returns the policy-effective time.
    #[must_use]
    pub const fn effective(self) -> Timestamp {
        self.effective
    }
}

/// A non-empty half-open time interval `[start, end)`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TimeInterval {
    start: Timestamp,
    end: Timestamp,
}

impl TimeInterval {
    /// Constructs a half-open interval when `start < end`.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::EmptyTimeInterval`] for an empty or reversed range.
    pub fn new(start: Timestamp, end: Timestamp) -> Result<Self, ModelError> {
        if start >= end {
            return Err(ModelError::EmptyTimeInterval);
        }
        Ok(Self { start, end })
    }

    /// Returns the inclusive start.
    #[must_use]
    pub const fn start(self) -> Timestamp {
        self.start
    }

    /// Returns the exclusive end.
    #[must_use]
    pub const fn end(self) -> Timestamp {
        self.end
    }

    /// Reports whether a timestamp lies in `[start, end)`.
    #[must_use]
    pub fn contains(self, timestamp: Timestamp) -> bool {
        self.start <= timestamp && timestamp < self.end
    }
}
