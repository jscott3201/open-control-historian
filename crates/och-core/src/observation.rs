//! Series metadata, exact observations, and the deterministic raw order key.

use crate::{
    ExactValue, NativeStatus, ObservationId, ObservationTimes, ProducerId, ProducerPosition,
    Quality, SeriesId, TimeInterval, Timestamp,
};

/// The closed collection semantics of an immutable series.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CollectionMode {
    /// Independent samples; no hold or value between samples is inferred.
    Sampled,
    /// Changes with optional explicit no-change intervals; no carry is inferred
    /// beyond the interval explicitly represented by that evidence.
    ChangeOnly,
    /// Exact cumulative readings; no delta or reset is inferred.
    Cumulative,
    /// Values covering explicit non-empty half-open time intervals.
    Interval,
    /// Occurrence evidence; no hold after an event is inferred.
    Event,
}

/// Immutable scope and collection semantics for one series.
///
/// A change to collection mode requires a new or explicitly reviewed series
/// identity; this type intentionally provides no mutation operation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SeriesMetadata {
    series_id: SeriesId,
    producer_id: ProducerId,
    collection_mode: CollectionMode,
}

impl SeriesMetadata {
    /// Constructs immutable series metadata.
    #[must_use]
    pub const fn new(
        series_id: SeriesId,
        producer_id: ProducerId,
        collection_mode: CollectionMode,
    ) -> Self {
        Self {
            series_id,
            producer_id,
            collection_mode,
        }
    }

    /// Returns the nominal series identity.
    #[must_use]
    pub const fn series_id(&self) -> SeriesId {
        self.series_id
    }

    /// Returns the producer identity in this series scope.
    #[must_use]
    pub const fn producer_id(&self) -> ProducerId {
        self.producer_id
    }

    /// Returns immutable collection semantics.
    #[must_use]
    pub const fn collection_mode(&self) -> CollectionMode {
        self.collection_mode
    }
}

/// One exact canonical observation.
///
/// Interval metadata is validated atomically against the series collection mode
/// by [`crate::CollectionEnvelope`]. Producer position remains optional because
/// some producers cannot supply ordering evidence.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Observation {
    id: ObservationId,
    value: ExactValue,
    times: ObservationTimes,
    quality: Quality,
    native_status: NativeStatus,
    producer_position: Option<ProducerPosition>,
    interval: Option<TimeInterval>,
}

impl Observation {
    /// Constructs an observation for later atomic envelope validation.
    #[must_use]
    pub const fn new(
        observation_id: ObservationId,
        value: ExactValue,
        times: ObservationTimes,
        quality: Quality,
        native_status: NativeStatus,
        producer_position: Option<ProducerPosition>,
        interval: Option<TimeInterval>,
    ) -> Self {
        Self {
            id: observation_id,
            value,
            times,
            quality,
            native_status,
            producer_position,
            interval,
        }
    }

    /// Returns the nominal observation identity.
    #[must_use]
    pub const fn observation_id(&self) -> ObservationId {
        self.id
    }

    /// Returns the exact value.
    #[must_use]
    pub const fn value(&self) -> &ExactValue {
        &self.value
    }

    /// Returns all source, receive, and effective times.
    #[must_use]
    pub const fn times(&self) -> ObservationTimes {
        self.times
    }

    /// Returns normalized quality.
    #[must_use]
    pub const fn quality(&self) -> Quality {
        self.quality
    }

    /// Returns independent opaque native status.
    #[must_use]
    pub const fn native_status(&self) -> &NativeStatus {
        &self.native_status
    }

    /// Returns explicit producer order when supplied.
    #[must_use]
    pub const fn producer_position(&self) -> Option<ProducerPosition> {
        self.producer_position
    }

    /// Returns interval metadata when supplied.
    #[must_use]
    pub const fn interval(&self) -> Option<TimeInterval> {
        self.interval
    }

    /// Returns the exact deterministic raw observation order key.
    ///
    /// Only effective time, receive time, then observation ID participate.
    /// Source time and producer position are deliberately excluded.
    #[must_use]
    pub const fn raw_order_key(&self) -> RawObservationOrderKey {
        RawObservationOrderKey {
            effective: self.times.effective(),
            receive: self.times.receive(),
            observation_id: self.id,
        }
    }
}

/// The deterministic raw observation order tuple.
///
/// Derived ordering is exactly `(effective time, receive time, observation ID)`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RawObservationOrderKey {
    effective: Timestamp,
    receive: Timestamp,
    observation_id: ObservationId,
}

impl RawObservationOrderKey {
    /// Returns the first order component: effective time.
    #[must_use]
    pub const fn effective(self) -> Timestamp {
        self.effective
    }

    /// Returns the second order component: receive time.
    #[must_use]
    pub const fn receive(self) -> Timestamp {
        self.receive
    }

    /// Returns the final deterministic identity tie-breaker.
    #[must_use]
    pub const fn observation_id(self) -> ObservationId {
        self.observation_id
    }
}
