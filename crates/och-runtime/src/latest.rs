use crate::ingress::IngressShared;
use och_core::{Observation, ProducerPosition, SeriesId, SeriesMetadata, StoreId};
use std::error::Error;
use std::fmt;
use std::sync::Arc;

/// Maximum nominal series retained by one runtime's volatile latest registry.
///
/// This fixed bound is independent of the ingress command capacity. No entry is
/// evicted to admit another series.
pub const MAX_PUBLISHED_SERIES: usize = 16;

/// One exact positioned observation published for a nominal series.
///
/// This is immutable observation evidence, not a current or held value. In
/// particular, collection mode does not add freshness, interpolation, delta,
/// reset, or interval-extension semantics.
#[derive(Clone, Eq, PartialEq)]
pub struct PublishedObservation {
    series: SeriesMetadata,
    observation: Observation,
    position: ProducerPosition,
}

impl PublishedObservation {
    pub(crate) fn new(
        series: SeriesMetadata,
        observation: Observation,
        position: ProducerPosition,
    ) -> Self {
        Self {
            series,
            observation,
            position,
        }
    }

    /// Returns the exact immutable series metadata bound by first publication.
    #[must_use]
    pub const fn series_metadata(&self) -> &SeriesMetadata {
        &self.series
    }

    /// Returns the exact published observation.
    #[must_use]
    pub const fn observation(&self) -> &Observation {
        &self.observation
    }

    /// Returns the explicit producer position that authorized publication.
    #[must_use]
    pub const fn producer_position(&self) -> ProducerPosition {
        self.position
    }
}

impl fmt::Debug for PublishedObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublishedObservation")
            .finish_non_exhaustive()
    }
}

/// An immutable point-in-time view of one runtime's volatile latest registry.
///
/// The immutable store scope makes even empty or retained-old snapshots
/// self-identifying. Cloning a snapshot is cheap and never changes its entries. A
/// caller may retain old snapshots after later publication or failure; that
/// caller-owned volatile memory is not runtime history, durability, or restart
/// recovery. Entry order is unspecified and conveys no arrival or latest-order
/// authority.
#[derive(Clone, Eq, PartialEq)]
pub struct LatestSnapshot {
    store_id: StoreId,
    entries: Arc<[PublishedObservation]>,
}

impl LatestSnapshot {
    pub(crate) fn empty(store_id: StoreId) -> Self {
        Self {
            store_id,
            entries: Arc::from([]),
        }
    }

    pub(crate) fn from_entries(store_id: StoreId, entries: Vec<PublishedObservation>) -> Self {
        Self {
            store_id,
            entries: Arc::from(entries.into_boxed_slice()),
        }
    }

    pub(crate) fn shares_entries_with(&self, other: &Self) -> bool {
        self.store_id == other.store_id && Arc::ptr_eq(&self.entries, &other.entries)
    }

    /// Returns the immutable store scope of this snapshot.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns the number of retained nominal series.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Reports whether the snapshot contains no published observations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the published observation for `series_id`, when retained.
    #[must_use]
    pub fn get(&self, series_id: &SeriesId) -> Option<&PublishedObservation> {
        self.entries
            .iter()
            .find(|entry| entry.series_metadata().series_id() == *series_id)
    }

    /// Returns all bounded entries in unspecified enumeration order.
    #[must_use]
    pub fn as_slice(&self) -> &[PublishedObservation] {
        &self.entries
    }

    /// Iterates over bounded entries in unspecified enumeration order.
    pub fn iter(&self) -> std::slice::Iter<'_, PublishedObservation> {
        self.entries.iter()
    }
}

impl fmt::Debug for LatestSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LatestSnapshot")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<'a> IntoIterator for &'a LatestSnapshot {
    type Item = &'a PublishedObservation;
    type IntoIter = std::slice::Iter<'a, PublishedObservation>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A cloneable synchronous reader for one runtime's volatile latest registry.
///
/// This handle contains no executor or public Tokio primitive and never keeps the
/// writer task alive. After graceful shutdown it can return the sealed final
/// snapshot. After abnormal stop, future capture attempts return
/// [`LatestReadError`], while snapshots acquired earlier remain usable.
#[derive(Clone)]
pub struct LatestReadHandle {
    shared: Arc<IngressShared>,
}

impl LatestReadHandle {
    pub(crate) fn new(shared: Arc<IngressShared>) -> Self {
        Self { shared }
    }

    /// Returns the immutable store scope of snapshots captured by this handle.
    #[must_use]
    pub fn store_id(&self) -> StoreId {
        self.shared.store_id()
    }

    /// Captures the complete current immutable snapshot synchronously.
    ///
    /// A reader racing an advancing publication receives either the complete old
    /// snapshot or the complete new snapshot. Enumeration order has no semantic
    /// meaning.
    ///
    /// # Errors
    ///
    /// Returns one sanitized [`LatestReadError`] after any abnormal writer or
    /// synchronization failure.
    pub fn snapshot(&self) -> Result<LatestSnapshot, LatestReadError> {
        self.shared.latest_snapshot()
    }
}

impl fmt::Debug for LatestReadHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LatestReadHandle")
            .finish_non_exhaustive()
    }
}

/// Sanitized failure to capture a runtime's latest snapshot.
///
/// The error deliberately carries no lock, task, queue, series, observation, or
/// failure payload details.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct LatestReadError {
    _private: (),
}

impl LatestReadError {
    pub(crate) const fn unavailable() -> Self {
        Self { _private: () }
    }
}

impl fmt::Debug for LatestReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LatestReadError")
    }
}

impl fmt::Display for LatestReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("latest observation snapshot is unavailable")
    }
}

impl Error for LatestReadError {}

pub(crate) struct LatestState {
    current: Option<LatestSnapshot>,
    sealed: bool,
}

impl LatestState {
    pub(crate) fn new(store_id: StoreId) -> Self {
        Self {
            current: Some(LatestSnapshot::empty(store_id)),
            sealed: false,
        }
    }

    pub(crate) fn snapshot(&self) -> Option<LatestSnapshot> {
        self.current.clone()
    }

    pub(crate) fn plan(
        &self,
        candidate: Option<PublishedObservation>,
    ) -> Result<PublicationPlan, PublicationFault> {
        let Some(current) = self.current.as_ref().filter(|_| !self.sealed) else {
            return Err(PublicationFault);
        };
        let Some(candidate) = candidate else {
            return Ok(PublicationPlan::NoChange);
        };
        let series_id = candidate.series_metadata().series_id();

        if let Some((index, published)) = current
            .as_slice()
            .iter()
            .enumerate()
            .find(|(_, entry)| entry.series_metadata().series_id() == series_id)
        {
            if published.series_metadata() != candidate.series_metadata() {
                return Err(PublicationFault);
            }
            return match candidate
                .producer_position()
                .cmp(&published.producer_position())
            {
                std::cmp::Ordering::Greater => {
                    Ok(PublicationPlan::Advance(Box::new(PlannedAdvance {
                        base: current.clone(),
                        index: Some(index),
                        candidate,
                    })))
                }
                std::cmp::Ordering::Less => Ok(PublicationPlan::NoChange),
                std::cmp::Ordering::Equal if candidate.observation() == published.observation() => {
                    Ok(PublicationPlan::NoChange)
                }
                std::cmp::Ordering::Equal => Err(PublicationFault),
            };
        }

        if current.len() >= MAX_PUBLISHED_SERIES {
            return Err(PublicationFault);
        }
        Ok(PublicationPlan::Advance(Box::new(PlannedAdvance {
            base: current.clone(),
            index: None,
            candidate,
        })))
    }

    pub(crate) fn can_complete(&self, preparation: &PreparedPublication) -> bool {
        if self.sealed || self.current.is_none() {
            return false;
        }
        match preparation {
            PreparedPublication::NoChange => true,
            PreparedPublication::Advance { base, .. } => self
                .current
                .as_ref()
                .is_some_and(|current| current.shares_entries_with(base)),
        }
    }

    pub(crate) fn commit(&mut self, preparation: PreparedPublication) {
        if let PreparedPublication::Advance { next, .. } = preparation {
            self.current = Some(next);
        }
    }

    pub(crate) fn seal(&mut self) -> bool {
        if self.current.is_none() || self.sealed {
            return false;
        }
        self.sealed = true;
        true
    }

    pub(crate) fn make_unavailable(&mut self) {
        self.current = None;
        self.sealed = true;
    }
}

pub(crate) enum PublicationPlan {
    NoChange,
    Advance(Box<PlannedAdvance>),
}

pub(crate) struct PlannedAdvance {
    base: LatestSnapshot,
    index: Option<usize>,
    candidate: PublishedObservation,
}

impl PublicationPlan {
    pub(crate) fn stage(self) -> PreparedPublication {
        match self {
            Self::NoChange => PreparedPublication::NoChange,
            Self::Advance(advance) => {
                let PlannedAdvance {
                    base,
                    index,
                    candidate,
                } = *advance;
                let mut entries = base.as_slice().to_vec();
                if let Some(index) = index {
                    entries[index] = candidate;
                } else {
                    entries.push(candidate);
                }
                let next = LatestSnapshot::from_entries(base.store_id(), entries);
                PreparedPublication::Advance { base, next }
            }
        }
    }
}

pub(crate) enum PreparedPublication {
    NoChange,
    Advance {
        base: LatestSnapshot,
        next: LatestSnapshot,
    },
}

pub(crate) struct PublicationFault;
