use och_core::{
    CaptureLifecycle, CollectionEnvelope, DeclarationEvidence, DeclarationRevision, EvidenceId,
    NormalizedRecordEvidence, ObservationId, RawRecordEvidence, RetryQualification, SeriesBinding,
    SeriesDeclarationPayload, SeriesId, SourceBatchMetadata, SourceGapEvidence, SourceIntervalKind,
    SourceObservationEvidence, StoreId,
};

/// Non-authorizing decoded declaration snapshot retained by Journal V1.
///
/// This mirror cannot be converted into a registry-issued declaration or bind
/// new evidence. It exists only for byte inspection and deterministic re-encode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedDeclarationV1 {
    pub(crate) store_id: StoreId,
    pub(crate) series_id: SeriesId,
    pub(crate) revision: DeclarationRevision,
    pub(crate) previous_revision: Option<DeclarationRevision>,
    pub(crate) binding: SeriesBinding,
    pub(crate) payload: SeriesDeclarationPayload,
    pub(crate) evidence: DeclarationEvidence,
}

impl DecodedDeclarationV1 {
    /// Returns the encoded store authority.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns the stable encoded series identity.
    #[must_use]
    pub const fn series_id(&self) -> SeriesId {
        self.series_id
    }

    /// Returns the encoded declaration revision.
    #[must_use]
    pub const fn revision(&self) -> DeclarationRevision {
        self.revision
    }

    /// Returns the encoded predecessor revision.
    #[must_use]
    pub const fn previous_revision(&self) -> Option<DeclarationRevision> {
        self.previous_revision
    }

    /// Returns the immutable encoded logical-point binding.
    #[must_use]
    pub const fn binding(&self) -> &SeriesBinding {
        &self.binding
    }

    /// Returns the encoded revisionable declaration payload.
    #[must_use]
    pub const fn payload(&self) -> &SeriesDeclarationPayload {
        &self.payload
    }

    /// Returns the encoded declaration-transition evidence.
    #[must_use]
    pub const fn evidence(&self) -> &DeclarationEvidence {
        &self.evidence
    }
}

/// Non-authorizing decoded source lineage for one canonical observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedObservationLineageV1 {
    pub(crate) ordinal: u8,
    pub(crate) canonical_observation_id: ObservationId,
    pub(crate) observation: SourceObservationEvidence,
    pub(crate) raw: RawRecordEvidence,
    pub(crate) normalized: NormalizedRecordEvidence,
}

impl DecodedObservationLineageV1 {
    /// Returns the original source-record ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> u8 {
        self.ordinal
    }

    /// Returns the explicitly associated canonical observation identity.
    #[must_use]
    pub const fn canonical_observation_id(&self) -> ObservationId {
        self.canonical_observation_id
    }

    /// Returns the retained source-observation evidence.
    #[must_use]
    pub const fn observation(&self) -> &SourceObservationEvidence {
        &self.observation
    }

    /// Returns the retained raw-record evidence.
    #[must_use]
    pub const fn raw(&self) -> &RawRecordEvidence {
        &self.raw
    }

    /// Returns the retained normalized-record evidence.
    #[must_use]
    pub const fn normalized(&self) -> &NormalizedRecordEvidence {
        &self.normalized
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DecodedEvidenceV1 {
    Observed {
        observations: Box<[DecodedObservationLineageV1]>,
        gaps: Box<[SourceGapEvidence]>,
    },
    NoChange,
}

/// Structurally complete non-authorizing decoded Journal V1 admission.
///
/// The type deliberately has no conversion to [`och_core::CanonicalAdmission`]
/// and cannot be submitted to `och-runtime`. Re-authorization requires the
/// independent live registry path owned by `och-core`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedAdmissionV1 {
    pub(crate) append_sequence: u64,
    pub(crate) store_id: StoreId,
    pub(crate) declaration: DecodedDeclarationV1,
    pub(crate) envelope: CollectionEnvelope,
    pub(crate) retry: RetryQualification,
    pub(crate) batch: SourceBatchMetadata,
    pub(crate) lifecycle: CaptureLifecycle,
    pub(crate) evidence: DecodedEvidenceV1,
}

impl DecodedAdmissionV1 {
    /// Returns the exact positive append sequence carried by the frame.
    #[must_use]
    pub const fn append_sequence(&self) -> u64 {
        self.append_sequence
    }

    /// Returns the exact encoded store scope.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns the non-authorizing decoded declaration snapshot.
    #[must_use]
    pub const fn declaration(&self) -> &DecodedDeclarationV1 {
        &self.declaration
    }

    /// Returns the decoded atomically validated envelope.
    #[must_use]
    pub const fn envelope(&self) -> &CollectionEnvelope {
        &self.envelope
    }

    /// Returns the decoded request-scoped retry qualification.
    #[must_use]
    pub const fn retry(&self) -> &RetryQualification {
        &self.retry
    }

    /// Returns the decoded source-batch metadata.
    #[must_use]
    pub const fn batch(&self) -> &SourceBatchMetadata {
        &self.batch
    }

    /// Returns the decoded capture lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> &CaptureLifecycle {
        &self.lifecycle
    }

    /// Returns the decoded closed observed/no-change kind.
    #[must_use]
    pub const fn evidence_kind(&self) -> SourceIntervalKind {
        match &self.evidence {
            DecodedEvidenceV1::Observed { .. } => SourceIntervalKind::Observed,
            DecodedEvidenceV1::NoChange => SourceIntervalKind::NoChange,
        }
    }

    /// Returns decoded ordered source-observation lineages.
    #[must_use]
    pub fn observations(&self) -> &[DecodedObservationLineageV1] {
        match &self.evidence {
            DecodedEvidenceV1::Observed { observations, .. } => observations,
            DecodedEvidenceV1::NoChange => &[],
        }
    }

    /// Returns decoded ordered source-gap evidence.
    #[must_use]
    pub fn gaps(&self) -> &[SourceGapEvidence] {
        match &self.evidence {
            DecodedEvidenceV1::Observed { gaps, .. } => gaps,
            DecodedEvidenceV1::NoChange => &[],
        }
    }

    pub(crate) fn evidence(&self) -> &DecodedEvidenceV1 {
        &self.evidence
    }
}

pub(crate) fn all_evidence_ids(admission: &DecodedAdmissionV1) -> Vec<EvidenceId> {
    let lifecycle = admission.lifecycle();
    let mut ids = vec![
        lifecycle.system().evidence_id(),
        lifecycle.endpoint().evidence_id(),
        lifecycle.run().evidence_id(),
        lifecycle.snapshot().evidence_id(),
    ];
    for lineage in admission.observations() {
        ids.extend([
            lineage.observation().evidence_id(),
            lineage.raw().evidence_id(),
            lineage.normalized().evidence_id(),
        ]);
    }
    ids
}
