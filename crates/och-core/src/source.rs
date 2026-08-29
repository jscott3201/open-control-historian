//! Bounded source/capture provenance and declaration-authorized canonical admission.
//!
//! These contracts preserve source evidence for future durable encoding without
//! defining serialization, persistence, adapter behavior, or retry policy.

use crate::collection::{MAX_GAPS, MAX_OBSERVATIONS};
use crate::compact::compact_vec;
use crate::{
    ArtifactReference, CollectionEnvelope, ContentIdentity, DeclarationReference,
    DeclaredCollectionEnvelope, EvidenceId, EvidenceKind, ModelError, ProducerEpoch,
    ProducerSequence, QuantityEvidence, RetryKey, RetryQualification, SeriesDeclaration,
    SourceProjection, SourceReference, StoreId, Timestamp, UnitEvidence,
};
use core::fmt;
use std::collections::HashSet;

/// Maximum source observation contexts in one canonical admission.
pub const MAX_SOURCE_OBSERVATION_CONTEXTS: usize = MAX_OBSERVATIONS;
/// Maximum source gap contexts in one canonical admission.
pub const MAX_SOURCE_GAP_CONTEXTS: usize = MAX_GAPS;

/// A bounded nominal source-schema identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SourceSchemaIdentity(DeclarationReference);

impl SourceSchemaIdentity {
    /// Validates and retains an exact bounded schema identity.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidDeclarationReference`] under the shared
    /// bounded-reference grammar.
    pub fn new(value: String) -> Result<Self, ModelError> {
        DeclarationReference::new(value).map(Self)
    }

    /// Borrows the exact schema identity.
    #[must_use]
    pub const fn as_reference(&self) -> &DeclarationReference {
        &self.0
    }

    /// Borrows the exact schema identity text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for SourceSchemaIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A non-zero bounded source-schema version.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SourceSchemaVersion(u128);

impl SourceSchemaVersion {
    /// Constructs a non-zero source-schema version.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidSourceSchemaVersion`] for zero.
    pub const fn new(value: u128) -> Result<Self, ModelError> {
        if value == 0 {
            return Err(ModelError::InvalidSourceSchemaVersion);
        }
        Ok(Self(value))
    }

    /// Returns the exact version value.
    #[must_use]
    pub const fn get(self) -> u128 {
        self.0
    }
}

impl fmt::Display for SourceSchemaVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The source batch's closed interval classification.
///
/// This classification contains no timestamp payload. Canonical no-change time
/// evidence remains owned by [`crate::NoChange`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SourceIntervalKind {
    /// The batch carries observation and/or gap evidence.
    Observed,
    /// The batch carries explicit no-change evidence.
    NoChange,
}

/// Exact source schema and interval classification retained for one admission.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceBatchMetadata {
    schema: SourceSchemaIdentity,
    version: SourceSchemaVersion,
    interval: SourceIntervalKind,
}

impl SourceBatchMetadata {
    /// Constructs exact source batch metadata.
    #[must_use]
    pub const fn new(
        schema: SourceSchemaIdentity,
        version: SourceSchemaVersion,
        interval: SourceIntervalKind,
    ) -> Self {
        Self {
            schema,
            version,
            interval,
        }
    }

    /// Returns the exact source-schema identity.
    #[must_use]
    pub const fn schema(&self) -> &SourceSchemaIdentity {
        &self.schema
    }

    /// Returns the exact source-schema version.
    #[must_use]
    pub const fn version(&self) -> SourceSchemaVersion {
        self.version
    }

    /// Returns the closed source interval classification.
    #[must_use]
    pub const fn interval(&self) -> SourceIntervalKind {
        self.interval
    }
}

/// Source-system capture evidence.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceSystemEvidence {
    evidence_id: EvidenceId,
    provider: DeclarationReference,
    projection: SourceProjection,
}

impl SourceSystemEvidence {
    /// Constructs exact source-system evidence.
    #[must_use]
    pub const fn new(
        evidence_id: EvidenceId,
        provider: DeclarationReference,
        projection: SourceProjection,
    ) -> Self {
        Self {
            evidence_id,
            provider,
            projection,
        }
    }

    /// Returns the shared source evidence identity.
    #[must_use]
    pub const fn evidence_id(&self) -> EvidenceId {
        self.evidence_id
    }

    /// Returns the exact provider reference.
    #[must_use]
    pub const fn provider(&self) -> &DeclarationReference {
        &self.provider
    }

    /// Returns the exact source projection reference.
    #[must_use]
    pub const fn projection(&self) -> &SourceProjection {
        &self.projection
    }
}

/// Source-endpoint capture evidence linked to one source system.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceEndpointEvidence {
    evidence_id: EvidenceId,
    system_id: EvidenceId,
    locator: DeclarationReference,
}

impl SourceEndpointEvidence {
    /// Constructs exact source-endpoint evidence.
    #[must_use]
    pub const fn new(
        evidence_id: EvidenceId,
        system_id: EvidenceId,
        locator: DeclarationReference,
    ) -> Self {
        Self {
            evidence_id,
            system_id,
            locator,
        }
    }

    /// Returns this endpoint's evidence identity.
    #[must_use]
    pub const fn evidence_id(&self) -> EvidenceId {
        self.evidence_id
    }

    /// Returns the linked source-system evidence identity.
    #[must_use]
    pub const fn system_id(&self) -> EvidenceId {
        self.system_id
    }

    /// Returns the exact provider-scoped source locator.
    #[must_use]
    pub const fn locator(&self) -> &DeclarationReference {
        &self.locator
    }
}

/// Capture-run evidence linked to one source endpoint.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CaptureRunEvidence {
    evidence_id: EvidenceId,
    endpoint_id: EvidenceId,
    started_at: Timestamp,
    completed_at: Option<Timestamp>,
}

impl CaptureRunEvidence {
    /// Constructs capture-run evidence with optional completion.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::CaptureRunTimeOrder`] when completion precedes
    /// start. Equal timestamps are accepted.
    pub fn new(
        evidence_id: EvidenceId,
        endpoint_id: EvidenceId,
        started_at: Timestamp,
        completed_at: Option<Timestamp>,
    ) -> Result<Self, ModelError> {
        if completed_at.is_some_and(|completed| completed < started_at) {
            return Err(ModelError::CaptureRunTimeOrder);
        }
        Ok(Self {
            evidence_id,
            endpoint_id,
            started_at,
            completed_at,
        })
    }

    /// Returns this capture run's evidence identity.
    #[must_use]
    pub const fn evidence_id(&self) -> EvidenceId {
        self.evidence_id
    }

    /// Returns the linked endpoint evidence identity.
    #[must_use]
    pub const fn endpoint_id(&self) -> EvidenceId {
        self.endpoint_id
    }

    /// Returns the exact capture start time.
    #[must_use]
    pub const fn started_at(&self) -> Timestamp {
        self.started_at
    }

    /// Returns optional exact capture completion time.
    #[must_use]
    pub const fn completed_at(&self) -> Option<Timestamp> {
        self.completed_at
    }
}

/// Source snapshot evidence linked to one capture run.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceSnapshotEvidence {
    evidence_id: EvidenceId,
    run_id: EvidenceId,
    artifact: ArtifactReference,
}

impl SourceSnapshotEvidence {
    /// Constructs exact source snapshot evidence.
    #[must_use]
    pub const fn new(
        evidence_id: EvidenceId,
        run_id: EvidenceId,
        artifact: ArtifactReference,
    ) -> Self {
        Self {
            evidence_id,
            run_id,
            artifact,
        }
    }

    /// Returns this snapshot's evidence identity.
    #[must_use]
    pub const fn evidence_id(&self) -> EvidenceId {
        self.evidence_id
    }

    /// Returns the linked capture-run evidence identity.
    #[must_use]
    pub const fn run_id(&self) -> EvidenceId {
        self.run_id
    }

    /// Returns the exact snapshot artifact and content identity.
    #[must_use]
    pub const fn artifact(&self) -> &ArtifactReference {
        &self.artifact
    }
}

/// One exact linked system → endpoint → run → snapshot capture lifecycle.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CaptureLifecycle {
    system: SourceSystemEvidence,
    endpoint: SourceEndpointEvidence,
    run: CaptureRunEvidence,
    snapshot: SourceSnapshotEvidence,
}

impl CaptureLifecycle {
    /// Validates and constructs a complete capture lifecycle.
    ///
    /// # Errors
    ///
    /// Returns a sanitized link-mismatch error unless endpoint links to system,
    /// run links to endpoint, and snapshot links to run.
    pub fn new(
        system: SourceSystemEvidence,
        endpoint: SourceEndpointEvidence,
        run: CaptureRunEvidence,
        snapshot: SourceSnapshotEvidence,
    ) -> Result<Self, ModelError> {
        if endpoint.system_id != system.evidence_id {
            return Err(ModelError::SourceEndpointSystemMismatch);
        }
        if run.endpoint_id != endpoint.evidence_id {
            return Err(ModelError::CaptureRunEndpointMismatch);
        }
        if snapshot.run_id != run.evidence_id {
            return Err(ModelError::SourceSnapshotRunMismatch);
        }
        Ok(Self {
            system,
            endpoint,
            run,
            snapshot,
        })
    }

    /// Returns the source-system evidence.
    #[must_use]
    pub const fn system(&self) -> &SourceSystemEvidence {
        &self.system
    }

    /// Returns the source-endpoint evidence.
    #[must_use]
    pub const fn endpoint(&self) -> &SourceEndpointEvidence {
        &self.endpoint
    }

    /// Returns the capture-run evidence.
    #[must_use]
    pub const fn run(&self) -> &CaptureRunEvidence {
        &self.run
    }

    /// Returns the source-snapshot evidence.
    #[must_use]
    pub const fn snapshot(&self) -> &SourceSnapshotEvidence {
        &self.snapshot
    }
}

/// Optional source-owned idempotency evidence.
///
/// It is retained independently and is never converted to or compared with the
/// request-scoped [`RetryQualification`].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceIdempotency {
    key: RetryKey,
    content: ContentIdentity,
}

impl SourceIdempotency {
    /// Constructs exact source-owned idempotency evidence.
    #[must_use]
    pub const fn new(key: RetryKey, content: ContentIdentity) -> Self {
        Self { key, content }
    }

    /// Returns the opaque source idempotency key.
    #[must_use]
    pub const fn key(&self) -> &RetryKey {
        &self.key
    }

    /// Returns the exact source idempotency content identity.
    #[must_use]
    pub const fn content(&self) -> &ContentIdentity {
        &self.content
    }
}

/// Closed source transport-delivery evidence.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SourceTransport {
    /// The source classified this transport delivery as new.
    New,
    /// The source classified this transport delivery as redelivered.
    Redelivered,
}

/// Transient interpretation context validated against the exact declaration.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceInterpretation {
    source: SourceReference,
    application: Option<DeclarationReference>,
    quantity: QuantityEvidence,
    unit: UnitEvidence,
}

impl SourceInterpretation {
    /// Constructs exact source observation interpretation context.
    #[must_use]
    pub const fn new(
        source: SourceReference,
        application: Option<DeclarationReference>,
        quantity: QuantityEvidence,
        unit: UnitEvidence,
    ) -> Self {
        Self {
            source,
            application,
            quantity,
            unit,
        }
    }

    /// Returns the exact provider/projection/locator source tuple.
    #[must_use]
    pub const fn source(&self) -> &SourceReference {
        &self.source
    }

    /// Returns the optional application reference.
    #[must_use]
    pub const fn application(&self) -> Option<&DeclarationReference> {
        self.application.as_ref()
    }

    /// Returns exact quantity evidence.
    #[must_use]
    pub const fn quantity(&self) -> &QuantityEvidence {
        &self.quantity
    }

    /// Returns exact unit evidence.
    #[must_use]
    pub const fn unit(&self) -> &UnitEvidence {
        &self.unit
    }
}

/// Source observation identity, artifact, transport, and idempotency evidence.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceObservationEvidence {
    evidence_id: EvidenceId,
    provenance_artifact: Option<ArtifactReference>,
    transport: SourceTransport,
    idempotency: Option<SourceIdempotency>,
}

impl SourceObservationEvidence {
    /// Constructs exact source observation evidence.
    #[must_use]
    pub const fn new(
        evidence_id: EvidenceId,
        provenance_artifact: Option<ArtifactReference>,
        transport: SourceTransport,
        idempotency: Option<SourceIdempotency>,
    ) -> Self {
        Self {
            evidence_id,
            provenance_artifact,
            transport,
            idempotency,
        }
    }

    /// Returns the source observation evidence identity.
    #[must_use]
    pub const fn evidence_id(&self) -> EvidenceId {
        self.evidence_id
    }

    /// Returns optional distinct source-observation provenance content.
    #[must_use]
    pub const fn provenance_artifact(&self) -> Option<&ArtifactReference> {
        self.provenance_artifact.as_ref()
    }

    /// Returns source transport-delivery evidence.
    #[must_use]
    pub const fn transport(&self) -> SourceTransport {
        self.transport
    }

    /// Returns optional source-observation idempotency evidence.
    #[must_use]
    pub const fn idempotency(&self) -> Option<&SourceIdempotency> {
        self.idempotency.as_ref()
    }
}

/// Raw source record evidence linked to the shared capture snapshot.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RawRecordEvidence {
    evidence_id: EvidenceId,
    snapshot_id: EvidenceId,
    artifact: ArtifactReference,
    idempotency: Option<SourceIdempotency>,
}

impl RawRecordEvidence {
    /// Constructs exact raw source record evidence.
    #[must_use]
    pub const fn new(
        evidence_id: EvidenceId,
        snapshot_id: EvidenceId,
        artifact: ArtifactReference,
        idempotency: Option<SourceIdempotency>,
    ) -> Self {
        Self {
            evidence_id,
            snapshot_id,
            artifact,
            idempotency,
        }
    }

    /// Returns the raw record evidence identity.
    #[must_use]
    pub const fn evidence_id(&self) -> EvidenceId {
        self.evidence_id
    }

    /// Returns the linked capture-snapshot evidence identity.
    #[must_use]
    pub const fn snapshot_id(&self) -> EvidenceId {
        self.snapshot_id
    }

    /// Returns the exact raw artifact and content identity.
    #[must_use]
    pub const fn artifact(&self) -> &ArtifactReference {
        &self.artifact
    }

    /// Returns optional raw-record idempotency evidence.
    #[must_use]
    pub const fn idempotency(&self) -> Option<&SourceIdempotency> {
        self.idempotency.as_ref()
    }
}

/// Normalized source record evidence linked to raw and observation evidence.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NormalizedRecordEvidence {
    evidence_id: EvidenceId,
    raw_record_id: EvidenceId,
    content: ContentIdentity,
    observation_evidence_id: EvidenceId,
}

impl NormalizedRecordEvidence {
    /// Constructs exact normalized record evidence.
    #[must_use]
    pub const fn new(
        evidence_id: EvidenceId,
        raw_record_id: EvidenceId,
        content: ContentIdentity,
        observation_evidence_id: EvidenceId,
    ) -> Self {
        Self {
            evidence_id,
            raw_record_id,
            content,
            observation_evidence_id,
        }
    }

    /// Returns the normalized record evidence identity.
    #[must_use]
    pub const fn evidence_id(&self) -> EvidenceId {
        self.evidence_id
    }

    /// Returns the linked raw record evidence identity.
    #[must_use]
    pub const fn raw_record_id(&self) -> EvidenceId {
        self.raw_record_id
    }

    /// Returns exact normalized content identity.
    #[must_use]
    pub const fn content(&self) -> &ContentIdentity {
        &self.content
    }

    /// Returns the linked source observation evidence identity.
    #[must_use]
    pub const fn observation_evidence_id(&self) -> EvidenceId {
        self.observation_evidence_id
    }
}

/// One source observation's transient context and linked record pair.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceObservationContext {
    ordinal: u8,
    interpretation: SourceInterpretation,
    observation: SourceObservationEvidence,
    raw: RawRecordEvidence,
    normalized: NormalizedRecordEvidence,
}

impl SourceObservationContext {
    /// Constructs one exact ordered source observation context.
    #[must_use]
    pub const fn new(
        ordinal: u8,
        interpretation: SourceInterpretation,
        observation: SourceObservationEvidence,
        raw: RawRecordEvidence,
        normalized: NormalizedRecordEvidence,
    ) -> Self {
        Self {
            ordinal,
            interpretation,
            observation,
            raw,
            normalized,
        }
    }

    /// Returns the original source-batch record ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> u8 {
        self.ordinal
    }

    /// Returns the transient interpretation context.
    #[must_use]
    pub const fn interpretation(&self) -> &SourceInterpretation {
        &self.interpretation
    }

    /// Returns source observation evidence.
    #[must_use]
    pub const fn observation(&self) -> &SourceObservationEvidence {
        &self.observation
    }

    /// Returns linked raw record evidence.
    #[must_use]
    pub const fn raw(&self) -> &RawRecordEvidence {
        &self.raw
    }

    /// Returns linked normalized record evidence.
    #[must_use]
    pub const fn normalized(&self) -> &NormalizedRecordEvidence {
        &self.normalized
    }
}

/// Retained source observation lineage after declaration context validation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceObservationLineage {
    ordinal: u8,
    observation: SourceObservationEvidence,
    raw: RawRecordEvidence,
    normalized: NormalizedRecordEvidence,
}

impl SourceObservationLineage {
    /// Returns the original source-batch record ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> u8 {
        self.ordinal
    }

    /// Returns retained source observation evidence.
    #[must_use]
    pub const fn observation(&self) -> &SourceObservationEvidence {
        &self.observation
    }

    /// Returns retained raw record evidence.
    #[must_use]
    pub const fn raw(&self) -> &RawRecordEvidence {
        &self.raw
    }

    /// Returns retained normalized record evidence.
    #[must_use]
    pub const fn normalized(&self) -> &NormalizedRecordEvidence {
        &self.normalized
    }
}

/// A closed source-side reason retained alongside one canonical gap.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SourceGapReason {
    /// Communication failed while acquiring the range.
    CommunicationFailure,
    /// The source was unavailable for the range.
    SourceUnavailable,
    /// A producer reset affected the range.
    ProducerReset,
    /// Source policy filtered the range.
    Filtered,
    /// The source supplied no more specific reason.
    Unknown,
}

/// Source gap context that exactly mirrors one canonical producer range.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceGapEvidence {
    epoch: ProducerEpoch,
    start: ProducerSequence,
    end: ProducerSequence,
    reason: SourceGapReason,
}

impl SourceGapEvidence {
    /// Constructs one non-empty half-open source gap range.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::EmptyGap`] when `start >= end`.
    pub fn new(
        epoch: ProducerEpoch,
        start: ProducerSequence,
        end: ProducerSequence,
        reason: SourceGapReason,
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

    /// Returns the producer epoch.
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

    /// Returns the exact closed source-side reason.
    #[must_use]
    pub const fn reason(&self) -> SourceGapReason {
        self.reason
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AdmissionEvidence {
    Observed {
        observations: Vec<SourceObservationLineage>,
        gaps: Vec<SourceGapEvidence>,
    },
    NoChange,
}

/// Final bounded declaration-authorized canonical admission evidence.
///
/// This is the exact native input boundary for future M02 journal encoding. It
/// owns a consumed registry-issued declaration binding, request retry scope,
/// source schema, capture lifecycle, and observed or no-change source evidence.
/// It defines no bytes, durability, persistence, or adapter behavior.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalAdmission {
    store_id: StoreId,
    declaration: SeriesDeclaration,
    envelope: CollectionEnvelope,
    retry: RetryQualification,
    batch: SourceBatchMetadata,
    lifecycle: CaptureLifecycle,
    evidence: AdmissionEvidence,
}

impl CanonicalAdmission {
    /// Validates and constructs observed source admission evidence.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`ModelError`] for source classification, scope,
    /// lifecycle, count, ordering, identity, link, interpretation, idempotency,
    /// or gap mismatch. Validation completes before input vectors are compacted.
    pub fn observed(
        declared: DeclaredCollectionEnvelope,
        retry: RetryQualification,
        batch: SourceBatchMetadata,
        lifecycle: CaptureLifecycle,
        observations: Vec<SourceObservationContext>,
        gaps: Vec<SourceGapEvidence>,
    ) -> Result<Self, ModelError> {
        validate_common(
            &declared,
            &retry,
            &batch,
            &lifecycle,
            SourceIntervalKind::Observed,
        )?;
        let envelope = declared.envelope();
        if observations.len() > MAX_SOURCE_OBSERVATION_CONTEXTS {
            return Err(ModelError::TooManySourceObservationContexts);
        }
        if observations.len() != envelope.observations().len() {
            return Err(ModelError::SourceObservationCountMismatch);
        }
        if gaps.len() > MAX_SOURCE_GAP_CONTEXTS {
            return Err(ModelError::TooManySourceGapContexts);
        }
        if gaps.len() != envelope.gaps().len() {
            return Err(ModelError::SourceGapCountMismatch);
        }

        let mut evidence_ids = lifecycle_evidence_ids(&lifecycle)?;
        validate_observations(
            declared.declaration(),
            &lifecycle,
            &observations,
            &mut evidence_ids,
        )?;
        validate_gaps(envelope, &gaps)?;

        let retained = observations
            .into_iter()
            .map(|context| SourceObservationLineage {
                ordinal: context.ordinal,
                observation: context.observation,
                raw: context.raw,
                normalized: context.normalized,
            })
            .collect();
        let (store_id, declaration, envelope) = declared.into_parts();
        Ok(Self {
            store_id,
            declaration,
            envelope,
            retry,
            batch,
            lifecycle,
            evidence: AdmissionEvidence::Observed {
                observations: compact_vec(retained),
                gaps: compact_vec(gaps),
            },
        })
    }

    /// Validates and constructs source no-change admission evidence.
    ///
    /// The representation structurally retains no observations, gaps, or record
    /// lineages. Canonical no-change time evidence remains in the bound envelope.
    ///
    /// # Errors
    ///
    /// Returns a sanitized [`ModelError`] for classification, retry scope,
    /// lifecycle binding, or duplicate lifecycle evidence identities.
    pub fn no_change(
        declared: DeclaredCollectionEnvelope,
        retry: RetryQualification,
        batch: SourceBatchMetadata,
        lifecycle: CaptureLifecycle,
    ) -> Result<Self, ModelError> {
        validate_common(
            &declared,
            &retry,
            &batch,
            &lifecycle,
            SourceIntervalKind::NoChange,
        )?;
        let _evidence_ids = lifecycle_evidence_ids(&lifecycle)?;
        let (store_id, declaration, envelope) = declared.into_parts();
        Ok(Self {
            store_id,
            declaration,
            envelope,
            retry,
            batch,
            lifecycle,
            evidence: AdmissionEvidence::NoChange,
        })
    }

    /// Returns the store authority that issued the declaration binding.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns the exact governing declaration snapshot.
    #[must_use]
    pub const fn declaration(&self) -> &SeriesDeclaration {
        &self.declaration
    }

    /// Returns the original atomically validated canonical envelope.
    #[must_use]
    pub const fn envelope(&self) -> &CollectionEnvelope {
        &self.envelope
    }

    /// Returns exact request-scoped retry qualification.
    #[must_use]
    pub const fn retry(&self) -> &RetryQualification {
        &self.retry
    }

    /// Returns exact retained source batch metadata.
    #[must_use]
    pub const fn batch(&self) -> &SourceBatchMetadata {
        &self.batch
    }

    /// Returns the complete retained capture lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> &CaptureLifecycle {
        &self.lifecycle
    }

    /// Returns the closed observed/no-change evidence kind.
    #[must_use]
    pub const fn evidence_kind(&self) -> SourceIntervalKind {
        match &self.evidence {
            AdmissionEvidence::Observed { .. } => SourceIntervalKind::Observed,
            AdmissionEvidence::NoChange => SourceIntervalKind::NoChange,
        }
    }

    /// Returns retained ordered source observation lineages, or an empty slice
    /// for no-change evidence.
    #[must_use]
    pub fn observations(&self) -> &[SourceObservationLineage] {
        match &self.evidence {
            AdmissionEvidence::Observed { observations, .. } => observations,
            AdmissionEvidence::NoChange => &[],
        }
    }

    /// Returns retained ordered source gap contexts, or an empty slice for
    /// no-change evidence.
    #[must_use]
    pub fn gaps(&self) -> &[SourceGapEvidence] {
        match &self.evidence {
            AdmissionEvidence::Observed { gaps, .. } => gaps,
            AdmissionEvidence::NoChange => &[],
        }
    }
}

fn validate_common(
    declared: &DeclaredCollectionEnvelope,
    retry: &RetryQualification,
    batch: &SourceBatchMetadata,
    lifecycle: &CaptureLifecycle,
    expected_interval: SourceIntervalKind,
) -> Result<(), ModelError> {
    let declaration = declared.declaration();
    let envelope = declared.envelope();
    if retry.series_id() != envelope.series().series_id()
        || retry.producer_id() != envelope.series().producer_id()
    {
        return Err(ModelError::AdmissionRetryScopeMismatch);
    }
    let envelope_interval = match envelope.evidence_kind() {
        EvidenceKind::Observed => SourceIntervalKind::Observed,
        EvidenceKind::NoChange => SourceIntervalKind::NoChange,
    };
    if batch.interval != expected_interval || envelope_interval != expected_interval {
        return Err(ModelError::SourceIntervalMismatch);
    }

    let source = declaration.binding().source();
    let projection = source
        .projection()
        .ok_or(ModelError::SourceProjectionRequired)?;
    if &lifecycle.system.provider != source.provider()
        || &lifecycle.system.projection != projection
        || &lifecycle.endpoint.locator != source.locator()
    {
        return Err(ModelError::SourceLifecycleBindingMismatch);
    }
    Ok(())
}

fn lifecycle_evidence_ids(lifecycle: &CaptureLifecycle) -> Result<HashSet<EvidenceId>, ModelError> {
    let mut identities = HashSet::with_capacity(4);
    for evidence_id in [
        lifecycle.system.evidence_id,
        lifecycle.endpoint.evidence_id,
        lifecycle.run.evidence_id,
        lifecycle.snapshot.evidence_id,
    ] {
        if !identities.insert(evidence_id) {
            return Err(ModelError::DuplicateSourceEvidenceId);
        }
    }
    Ok(identities)
}

fn validate_observations(
    declaration: &SeriesDeclaration,
    lifecycle: &CaptureLifecycle,
    observations: &[SourceObservationContext],
    evidence_ids: &mut HashSet<EvidenceId>,
) -> Result<(), ModelError> {
    let declaration_source = declaration.binding().source();
    let payload = declaration.payload();
    let mut previous_ordinal = None;
    for context in observations {
        if previous_ordinal.is_some_and(|previous| previous >= context.ordinal) {
            return Err(ModelError::MisorderedSourceRecordOrdinals);
        }
        previous_ordinal = Some(context.ordinal);
        if &context.interpretation.source != declaration_source
            || context.interpretation.application.as_ref() != payload.application()
            || &context.interpretation.quantity != payload.quantity()
            || &context.interpretation.unit != payload.unit()
        {
            return Err(ModelError::SourceInterpretationMismatch);
        }
        for evidence_id in [
            context.observation.evidence_id,
            context.raw.evidence_id,
            context.normalized.evidence_id,
        ] {
            if !evidence_ids.insert(evidence_id) {
                return Err(ModelError::DuplicateSourceEvidenceId);
            }
        }
        if context.raw.snapshot_id != lifecycle.snapshot.evidence_id {
            return Err(ModelError::SourceRawSnapshotMismatch);
        }
        if context.normalized.raw_record_id != context.raw.evidence_id {
            return Err(ModelError::SourceNormalizedRawMismatch);
        }
        if context.normalized.observation_evidence_id != context.observation.evidence_id {
            return Err(ModelError::SourceNormalizedObservationMismatch);
        }
        if context
            .raw
            .idempotency
            .as_ref()
            .is_some_and(|idempotency| &idempotency.content != context.raw.artifact.content())
        {
            return Err(ModelError::SourceRawIdempotencyMismatch);
        }
    }
    Ok(())
}

fn validate_gaps(
    envelope: &CollectionEnvelope,
    gaps: &[SourceGapEvidence],
) -> Result<(), ModelError> {
    if envelope.gaps().iter().zip(gaps).any(|(canonical, source)| {
        canonical.epoch() != source.epoch
            || canonical.start() != source.start
            || canonical.end() != source.end
    }) {
        return Err(ModelError::SourceGapMismatch);
    }
    Ok(())
}
