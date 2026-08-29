//! Atomic bounded collection evidence and its cross-item validation.

use crate::{
    CollectionMode, ModelError, Observation, ProducerEpoch, ProducerSequence, SeriesMetadata,
    TimeInterval,
};
use std::collections::HashSet;

/// Maximum observations accepted in one atomic envelope.
pub const MAX_OBSERVATIONS: usize = 256;
/// Maximum gaps accepted in one atomic envelope.
pub const MAX_GAPS: usize = 64;

/// A closed, sanitized producer-sequence gap reason.
///
/// No variant claims time completeness, no-change, or value unavailability.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum GapReason {
    /// The producer did not provide a more specific sanitized reason.
    Unknown,
    /// A producer restart made a sequence range unavailable.
    ProducerRestart,
    /// A bounded producer buffer discarded a sequence range.
    BufferOverflow,
    /// Communication lost a sequence range.
    CommunicationFailure,
    /// Source-side acquisition did not retain a sequence range.
    SourceDataLoss,
    /// An authorized administrative action excluded a sequence range.
    AdministrativeExclusion,
}

/// A non-empty half-open producer-sequence range within one epoch.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Gap {
    epoch: ProducerEpoch,
    start: ProducerSequence,
    end: ProducerSequence,
    reason: GapReason,
}

impl Gap {
    /// Constructs `[start, end)` within one producer epoch.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::EmptyGap`] when `start >= end`.
    pub fn new(
        epoch: ProducerEpoch,
        start: ProducerSequence,
        end: ProducerSequence,
        reason: GapReason,
    ) -> Result<Self, ModelError> {
        if start >= end {
            return Err(ModelError::EmptyGap);
        }
        Ok(Self {
            epoch,
            start,
            end,
            reason,
        })
    }

    /// Returns the one producer epoch containing the range.
    #[must_use]
    pub const fn epoch(&self) -> ProducerEpoch {
        self.epoch
    }

    /// Returns the inclusive starting sequence.
    #[must_use]
    pub const fn start(&self) -> ProducerSequence {
        self.start
    }

    /// Returns the exclusive ending sequence.
    #[must_use]
    pub const fn end(&self) -> ProducerSequence {
        self.end
    }

    /// Returns the closed sanitized reason.
    #[must_use]
    pub const fn reason(&self) -> GapReason {
        self.reason
    }

    /// Reports whether a sequence in this gap's epoch lies in `[start, end)`.
    #[must_use]
    pub fn contains(&self, epoch: ProducerEpoch, sequence: ProducerSequence) -> bool {
        self.epoch == epoch && self.start <= sequence && sequence < self.end
    }
}

/// Explicit no-change evidence over a non-empty half-open time interval.
///
/// This evidence is valid only for [`CollectionMode::ChangeOnly`] and contains
/// no observations or producer-sequence gaps.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct NoChange {
    interval: TimeInterval,
}

impl NoChange {
    /// Wraps an already validated non-empty half-open time interval.
    #[must_use]
    pub const fn new(interval: TimeInterval) -> Self {
        Self { interval }
    }

    /// Returns the exact no-change interval.
    #[must_use]
    pub const fn interval(self) -> TimeInterval {
        self.interval
    }
}

/// The closed kind of evidence held by a [`CollectionEnvelope`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EvidenceKind {
    /// One or more observations or gaps.
    Observed,
    /// One explicit change-only no-change interval.
    NoChange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Evidence {
    Observed {
        observations: Vec<Observation>,
        gaps: Vec<Gap>,
    },
    NoChange(NoChange),
}

/// One bounded, atomically validated collection envelope.
///
/// Private evidence fields prevent construction that bypasses cross-item or
/// collection-mode invariants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionEnvelope {
    series: SeriesMetadata,
    evidence: Evidence,
}

impl CollectionEnvelope {
    /// Validates and constructs observed evidence.
    ///
    /// Validation rejects bounds before secondary model allocations, then checks
    /// mode-specific interval metadata, duplicate IDs, all-or-none producer
    /// positions, strictly increasing positions, ordered non-overlapping gaps,
    /// and positioned observations inside gaps.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`ModelError`] for the first violated invariant.
    pub fn observed(
        series: SeriesMetadata,
        observations: Vec<Observation>,
        gaps: Vec<Gap>,
    ) -> Result<Self, ModelError> {
        validate_observed(series.collection_mode(), &observations, &gaps)?;
        Ok(Self {
            series,
            evidence: Evidence::Observed { observations, gaps },
        })
    }

    /// Constructs explicit no-change evidence for a change-only series.
    ///
    /// The representation structurally contains no observations or gaps.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidNoChangeMode`] unless the series is
    /// [`CollectionMode::ChangeOnly`].
    pub fn no_change(series: SeriesMetadata, no_change: NoChange) -> Result<Self, ModelError> {
        if series.collection_mode() != CollectionMode::ChangeOnly {
            return Err(ModelError::InvalidNoChangeMode);
        }
        Ok(Self {
            series,
            evidence: Evidence::NoChange(no_change),
        })
    }

    /// Returns immutable series scope and collection semantics.
    #[must_use]
    pub const fn series(&self) -> &SeriesMetadata {
        &self.series
    }

    /// Returns the closed evidence kind.
    #[must_use]
    pub const fn evidence_kind(&self) -> EvidenceKind {
        match self.evidence {
            Evidence::Observed { .. } => EvidenceKind::Observed,
            Evidence::NoChange(_) => EvidenceKind::NoChange,
        }
    }

    /// Returns observations, or an empty slice for no-change evidence.
    #[must_use]
    pub fn observations(&self) -> &[Observation] {
        match &self.evidence {
            Evidence::Observed { observations, .. } => observations,
            Evidence::NoChange(_) => &[],
        }
    }

    /// Returns gaps, or an empty slice for no-change evidence.
    #[must_use]
    pub fn gaps(&self) -> &[Gap] {
        match &self.evidence {
            Evidence::Observed { gaps, .. } => gaps,
            Evidence::NoChange(_) => &[],
        }
    }

    /// Returns the explicit no-change evidence when present.
    #[must_use]
    pub const fn no_change_evidence(&self) -> Option<NoChange> {
        match self.evidence {
            Evidence::Observed { .. } => None,
            Evidence::NoChange(no_change) => Some(no_change),
        }
    }
}

fn validate_observed(
    mode: CollectionMode,
    observations: &[Observation],
    gaps: &[Gap],
) -> Result<(), ModelError> {
    if observations.len() > MAX_OBSERVATIONS {
        return Err(ModelError::TooManyObservations);
    }
    if gaps.len() > MAX_GAPS {
        return Err(ModelError::TooManyGaps);
    }
    if observations.is_empty() && gaps.is_empty() {
        return Err(ModelError::EmptyObservedEvidence);
    }

    let mut identities = HashSet::with_capacity(observations.len());
    for observation in observations {
        match (mode, observation.interval()) {
            (CollectionMode::Interval, None) => {
                return Err(ModelError::MissingObservationInterval);
            }
            (CollectionMode::Interval, Some(_)) | (_, None) => {}
            (_, Some(_)) => return Err(ModelError::UnexpectedObservationInterval),
        }
        if !identities.insert(observation.observation_id()) {
            return Err(ModelError::DuplicateObservationId);
        }
    }

    let first_position_presence = observations
        .first()
        .map(|observation| observation.producer_position().is_some());
    if first_position_presence.is_some_and(|presence| {
        observations
            .iter()
            .any(|observation| observation.producer_position().is_some() != presence)
    }) {
        return Err(ModelError::MixedProducerPositions);
    }

    if first_position_presence == Some(true) {
        for pair in observations.windows(2) {
            let (Some(previous), Some(current)) =
                (pair[0].producer_position(), pair[1].producer_position())
            else {
                return Err(ModelError::MixedProducerPositions);
            };
            if previous >= current {
                return Err(ModelError::MisorderedProducerPositions);
            }
        }
    }

    for pair in gaps.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if current.epoch() < previous.epoch()
            || (current.epoch() == previous.epoch() && current.start() < previous.start())
        {
            return Err(ModelError::MisorderedGaps);
        }
        if current.epoch() == previous.epoch() && current.start() < previous.end() {
            return Err(ModelError::OverlappingGaps);
        }
    }

    if first_position_presence == Some(true) {
        validate_observations_outside_gaps(observations, gaps)?;
    }
    Ok(())
}

fn validate_observations_outside_gaps(
    observations: &[Observation],
    gaps: &[Gap],
) -> Result<(), ModelError> {
    let mut gap_index = 0;
    for observation in observations {
        let Some(position) = observation.producer_position() else {
            return Err(ModelError::MixedProducerPositions);
        };
        while let Some(gap) = gaps.get(gap_index) {
            if gap.epoch() < position.epoch()
                || (gap.epoch() == position.epoch() && gap.end() <= position.sequence())
            {
                gap_index += 1;
            } else {
                break;
            }
        }
        if gaps
            .get(gap_index)
            .is_some_and(|gap| gap.contains(position.epoch(), position.sequence()))
        {
            return Err(ModelError::ObservationInsideGap);
        }
    }
    Ok(())
}
