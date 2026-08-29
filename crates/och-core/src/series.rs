//! Bounded canonical series declaration, revision, retirement, and binding authority.
//!
//! The caller owns each registry instance. The registry retains finite immutable
//! declaration history and terminal tombstones, but performs no persistence,
//! admission, authorization, or runtime publication.

use crate::compact::{compact_string, compact_vec};
use crate::{
    ArtifactReference, CollectionEnvelope, CollectionMode, ExactValue, ModelError, ProducerId,
    SeriesId, SeriesMetadata, StoreId, Timestamp,
};
use core::fmt;
use std::collections::BTreeMap;

/// Maximum Unicode scalar values in a canonical declaration reference.
pub const MAX_DECLARATION_REFERENCE_SCALARS: usize = 1_024;

/// A compact non-empty external reference retained by a series declaration.
///
/// References contain at most 1,024 Unicode scalar values and no control
/// characters. They are preserved exactly without Unicode normalization or
/// interpretation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeclarationReference(String);

impl DeclarationReference {
    /// Validates and retains an exact declaration reference.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidDeclarationReference`] for an empty value,
    /// a control character, or more than 1,024 Unicode scalar values.
    pub fn new(value: String) -> Result<Self, ModelError> {
        let mut scalar_count = 0;
        let mut valid = !value.is_empty();
        for scalar in value.chars().take(MAX_DECLARATION_REFERENCE_SCALARS + 1) {
            scalar_count += 1;
            valid &= !scalar.is_control();
        }
        valid &= scalar_count <= MAX_DECLARATION_REFERENCE_SCALARS;
        if !valid {
            return Err(ModelError::InvalidDeclarationReference);
        }
        Ok(Self(compact_string(value)))
    }

    /// Borrows the exact retained reference.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the reference and returns its exact text.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl TryFrom<String> for DeclarationReference {
    type Error = ModelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for DeclarationReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// An opaque bounded source-projection reference.
///
/// Projection vocabularies belong to adapters. Core preserves the exact bounded
/// reference without freezing the currently known projection variants.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SourceProjection(DeclarationReference);

impl SourceProjection {
    /// Validates and retains an exact source-projection reference.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidDeclarationReference`] under the shared
    /// bounded-reference grammar.
    pub fn new(value: String) -> Result<Self, ModelError> {
        DeclarationReference::new(value).map(Self)
    }

    /// Borrows the exact projection reference.
    #[must_use]
    pub const fn as_reference(&self) -> &DeclarationReference {
        &self.0
    }

    /// Borrows the exact projection text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// The immutable provider, optional projection, and source locator of one logical point.
///
/// A correction that changes any component is a logical rebind. It requires
/// retirement of the old series and registration under a new [`SeriesId`].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceReference {
    provider: DeclarationReference,
    projection: Option<SourceProjection>,
    locator: DeclarationReference,
}

impl SourceReference {
    /// Constructs one exact provider/source-locator pair.
    ///
    /// This compatibility constructor omits projection. Projection-absent
    /// bindings remain valid declaration history but cannot authorize canonical
    /// source admission.
    #[must_use]
    pub const fn new(provider: DeclarationReference, locator: DeclarationReference) -> Self {
        Self {
            provider,
            projection: None,
            locator,
        }
    }

    /// Constructs one exact provider/projection/source-locator tuple.
    #[must_use]
    pub const fn with_projection(
        provider: DeclarationReference,
        projection: SourceProjection,
        locator: DeclarationReference,
    ) -> Self {
        Self {
            provider,
            projection: Some(projection),
            locator,
        }
    }

    /// Returns the exact external provider reference.
    #[must_use]
    pub const fn provider(&self) -> &DeclarationReference {
        &self.provider
    }

    /// Returns the optional opaque source projection.
    #[must_use]
    pub const fn projection(&self) -> Option<&SourceProjection> {
        self.projection.as_ref()
    }

    /// Returns the exact provider-scoped source locator.
    #[must_use]
    pub const fn locator(&self) -> &DeclarationReference {
        &self.locator
    }
}

/// The immutable logical-point binding of one series identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SeriesBinding {
    source: SourceReference,
}

impl SeriesBinding {
    /// Constructs an immutable logical-point binding.
    #[must_use]
    pub const fn new(source: SourceReference) -> Self {
        Self { source }
    }

    /// Returns the immutable provider/projection/source-locator binding.
    #[must_use]
    pub const fn source(&self) -> &SourceReference {
        &self.source
    }
}

/// The usable underlying family of exact values for one declaration revision.
///
/// [`ExactValue::Unavailable`] is explicit absence and is admissible for every
/// family; it is deliberately not a separate underlying family.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ValueFamily {
    /// Exact IEEE 754 binary64 bits.
    Real,
    /// Signed 64-bit integers.
    Signed,
    /// Unsigned 64-bit integers.
    Unsigned,
    /// Boolean values.
    Boolean,
    /// Explicit state class/member values.
    State,
    /// Exact bounded text.
    Text,
    /// External artifact references.
    Artifact,
}

impl ValueFamily {
    /// Reports whether an exact value is admissible for this family.
    #[must_use]
    pub const fn admits(self, value: &ExactValue) -> bool {
        matches!(value, ExactValue::Unavailable(_))
            || matches!(
                (self, value),
                (Self::Real, ExactValue::Real(_))
                    | (Self::Signed, ExactValue::Signed(_))
                    | (Self::Unsigned, ExactValue::Unsigned(_))
                    | (Self::Boolean, ExactValue::Boolean(_))
                    | (Self::State, ExactValue::State(_))
                    | (Self::Text, ExactValue::Text(_))
                    | (Self::Artifact, ExactValue::Artifact(_))
            )
    }
}

/// Quantity semantics attached to one declaration revision.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum QuantityEvidence {
    /// No quantity evidence was supplied.
    Absent,
    /// The reference resolves in an external quantity vocabulary.
    Resolved(DeclarationReference),
    /// The exact native quantity reference could not be resolved.
    Unresolved(DeclarationReference),
}

/// Unit semantics attached to one declaration revision.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum UnitEvidence {
    /// No unit evidence was supplied.
    Absent,
    /// The reference resolves in an external unit vocabulary.
    Resolved(DeclarationReference),
    /// The exact native unit reference could not be resolved.
    Unresolved(DeclarationReference),
}

/// The revisionable interpretation payload of a series declaration.
///
/// Producer and collection mode are revisioned explicitly. A bare
/// [`SeriesMetadata`] remains only envelope-local metadata and gains no
/// lifecycle authority from this type.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SeriesDeclarationPayload {
    producer_id: ProducerId,
    collection_mode: CollectionMode,
    value_family: ValueFamily,
    quantity: QuantityEvidence,
    unit: UnitEvidence,
    application: Option<DeclarationReference>,
}

impl SeriesDeclarationPayload {
    /// Constructs one exact revisionable declaration payload.
    #[must_use]
    pub const fn new(
        producer_id: ProducerId,
        collection_mode: CollectionMode,
        value_family: ValueFamily,
        quantity: QuantityEvidence,
        unit: UnitEvidence,
        application: Option<DeclarationReference>,
    ) -> Self {
        Self {
            producer_id,
            collection_mode,
            value_family,
            quantity,
            unit,
            application,
        }
    }

    /// Returns the producer identity governing this revision.
    #[must_use]
    pub const fn producer_id(&self) -> ProducerId {
        self.producer_id
    }

    /// Returns the collection semantics governing this revision.
    #[must_use]
    pub const fn collection_mode(&self) -> CollectionMode {
        self.collection_mode
    }

    /// Returns the usable value family governing this revision.
    #[must_use]
    pub const fn value_family(&self) -> ValueFamily {
        self.value_family
    }

    /// Returns the exact quantity evidence.
    #[must_use]
    pub const fn quantity(&self) -> &QuantityEvidence {
        &self.quantity
    }

    /// Returns the exact unit evidence.
    #[must_use]
    pub const fn unit(&self) -> &UnitEvidence {
        &self.unit
    }

    /// Returns the optional application-level reference.
    #[must_use]
    pub const fn application(&self) -> Option<&DeclarationReference> {
        self.application.as_ref()
    }

    fn series_metadata(&self, series_id: SeriesId) -> SeriesMetadata {
        SeriesMetadata::new(series_id, self.producer_id, self.collection_mode)
    }
}

/// Immutable evidence for one accepted declaration or retirement transition.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DeclarationEvidence {
    effective_at: Timestamp,
    artifact: Option<ArtifactReference>,
}

impl DeclarationEvidence {
    /// Constructs exact transition evidence without fetching an artifact.
    #[must_use]
    pub const fn new(effective_at: Timestamp, artifact: Option<ArtifactReference>) -> Self {
        Self {
            effective_at,
            artifact,
        }
    }

    /// Returns the caller-supplied effective transition time.
    #[must_use]
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    /// Returns optional immutable external supporting content.
    #[must_use]
    pub const fn artifact(&self) -> Option<&ArtifactReference> {
        self.artifact.as_ref()
    }
}

/// A non-zero monotonically ordered per-series declaration revision.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeclarationRevision(u128);

impl DeclarationRevision {
    /// The first declaration revision issued for every series.
    pub const FIRST: Self = Self(1);

    /// Constructs a non-zero declaration revision for comparison or recovery.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidDeclarationRevision`] for zero.
    pub const fn new(value: u128) -> Result<Self, ModelError> {
        if value == 0 {
            return Err(ModelError::InvalidDeclarationRevision);
        }
        Ok(Self(value))
    }

    /// Returns the exact numeric revision.
    #[must_use]
    pub const fn get(self) -> u128 {
        self.0
    }

    fn next(self) -> Self {
        // An accepted next revision requires total_count < a usize bound first.
        // A per-series revision cannot therefore reach the larger u128 limit.
        Self(self.0 + 1)
    }
}

impl fmt::Display for DeclarationRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// One immutable accepted series declaration revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeriesDeclaration {
    store_id: StoreId,
    series_id: SeriesId,
    revision: DeclarationRevision,
    previous_revision: Option<DeclarationRevision>,
    binding: SeriesBinding,
    payload: SeriesDeclarationPayload,
    evidence: DeclarationEvidence,
}

impl SeriesDeclaration {
    /// Returns the store authority that issued this declaration.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns the stable series identity.
    #[must_use]
    pub const fn series_id(&self) -> SeriesId {
        self.series_id
    }

    /// Returns the issued per-series declaration revision.
    #[must_use]
    pub const fn revision(&self) -> DeclarationRevision {
        self.revision
    }

    /// Returns the predecessor revision, or `None` for initial revision one.
    #[must_use]
    pub const fn previous_revision(&self) -> Option<DeclarationRevision> {
        self.previous_revision
    }

    /// Returns the immutable logical-point binding.
    #[must_use]
    pub const fn binding(&self) -> &SeriesBinding {
        &self.binding
    }

    /// Returns the exact revisionable interpretation payload.
    #[must_use]
    pub const fn payload(&self) -> &SeriesDeclarationPayload {
        &self.payload
    }

    /// Returns the immutable transition evidence for this revision.
    #[must_use]
    pub const fn evidence(&self) -> &DeclarationEvidence {
        &self.evidence
    }
}

/// Terminal retirement evidence anchored to the last active declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeriesRetirement {
    declaration_revision: DeclarationRevision,
    evidence: DeclarationEvidence,
}

impl SeriesRetirement {
    /// Returns the final declaration revision retired by this transition.
    #[must_use]
    pub const fn declaration_revision(&self) -> DeclarationRevision {
        self.declaration_revision
    }

    /// Returns the immutable retirement evidence.
    #[must_use]
    pub const fn evidence(&self) -> &DeclarationEvidence {
        &self.evidence
    }
}

/// The closed lifecycle of a registered series.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SeriesLifecycle {
    /// The latest declaration may authorize new bounded evidence.
    Active,
    /// The tombstone is terminal and only historical resolution remains.
    Retired,
}

/// Immutable retained history for one registered series.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeriesHistory {
    series_id: SeriesId,
    binding: SeriesBinding,
    declarations: Vec<SeriesDeclaration>,
    retirement: Option<SeriesRetirement>,
}

impl SeriesHistory {
    /// Returns the stable series identity.
    #[must_use]
    pub const fn series_id(&self) -> SeriesId {
        self.series_id
    }

    /// Returns the immutable logical-point binding.
    #[must_use]
    pub const fn binding(&self) -> &SeriesBinding {
        &self.binding
    }

    /// Returns every accepted declaration in ascending revision order.
    #[must_use]
    pub fn declarations(&self) -> &[SeriesDeclaration] {
        &self.declarations
    }

    /// Returns the latest retained declaration, including after retirement.
    ///
    /// Registry-produced histories always return `Some`; the optional shape
    /// keeps this accessor total even if future recovery validates external state.
    #[must_use]
    pub fn latest_declaration(&self) -> Option<&SeriesDeclaration> {
        self.declarations.last()
    }

    /// Returns immutable creation evidence from declaration revision one.
    ///
    /// Registry-produced histories always return `Some`; the optional shape
    /// keeps this accessor total even if future recovery validates external state.
    #[must_use]
    pub fn creation_evidence(&self) -> Option<&DeclarationEvidence> {
        self.declarations.first().map(SeriesDeclaration::evidence)
    }

    /// Returns the current closed lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> SeriesLifecycle {
        if self.retirement.is_some() {
            SeriesLifecycle::Retired
        } else {
            SeriesLifecycle::Active
        }
    }

    /// Returns terminal retirement evidence when present.
    #[must_use]
    pub const fn retirement(&self) -> Option<&SeriesRetirement> {
        self.retirement.as_ref()
    }

    fn compact_clone(&self) -> Self {
        Self {
            series_id: self.series_id,
            binding: self.binding.clone(),
            declarations: compact_vec(self.declarations.clone()),
            retirement: self.retirement.clone(),
        }
    }
}

/// Explicit finite bounds for one caller-owned [`SeriesRegistry`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SeriesRegistryLimits {
    max_series: usize,
    max_declaration_revisions: usize,
}

impl SeriesRegistryLimits {
    /// Defines exact total series and retained declaration-revision bounds.
    ///
    /// Zero is valid and creates a registry that refuses the corresponding
    /// mutation without allocating. Retired tombstones count as series.
    #[must_use]
    pub const fn new(max_series: usize, max_declaration_revisions: usize) -> Self {
        Self {
            max_series,
            max_declaration_revisions,
        }
    }

    /// Returns the total live-plus-retired series bound.
    #[must_use]
    pub const fn max_series(self) -> usize {
        self.max_series
    }

    /// Returns the total retained declaration-revision bound.
    #[must_use]
    pub const fn max_declaration_revisions(self) -> usize {
        self.max_declaration_revisions
    }
}

/// One deterministic immutable registry snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeriesRegistrySnapshot {
    store_id: StoreId,
    limits: SeriesRegistryLimits,
    declaration_revision_count: usize,
    series: Vec<SeriesHistory>,
}

impl SeriesRegistrySnapshot {
    /// Returns the one store authority represented by this snapshot.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns the finite registry bounds.
    #[must_use]
    pub const fn limits(&self) -> SeriesRegistryLimits {
        self.limits
    }

    /// Returns the total retained declaration revisions.
    #[must_use]
    pub const fn declaration_revision_count(&self) -> usize {
        self.declaration_revision_count
    }

    /// Returns histories ordered by nominal series identity; each history is in
    /// ascending declaration-revision order.
    #[must_use]
    pub fn series(&self) -> &[SeriesHistory] {
        &self.series
    }
}

/// A bounded caller-owned canonical declaration and lifecycle authority.
///
/// This pure in-memory model is not a durable registry. It never evicts and
/// checks every finite bound before changing state. The authority cannot be
/// cloned or forked; callers use immutable [`SeriesRegistrySnapshot`] values for
/// comparisons and reads.
///
/// ```compile_fail
/// use och_core::{SeriesRegistry, SeriesRegistryLimits, StoreId};
///
/// let store: StoreId = "01941f29-7c00-7000-8000-000000000064".parse().unwrap();
/// let registry = SeriesRegistry::new(store, SeriesRegistryLimits::new(1, 1));
/// let _forked_authority = registry.clone();
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct SeriesRegistry {
    store_id: StoreId,
    limits: SeriesRegistryLimits,
    declaration_revision_count: usize,
    series: BTreeMap<SeriesId, SeriesHistory>,
}

impl SeriesRegistry {
    /// Constructs an empty registry scoped to one store authority.
    #[must_use]
    pub const fn new(store_id: StoreId, limits: SeriesRegistryLimits) -> Self {
        Self {
            store_id,
            limits,
            declaration_revision_count: 0,
            series: BTreeMap::new(),
        }
    }

    /// Returns the stable store identity.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns the configured finite bounds.
    #[must_use]
    pub const fn limits(&self) -> SeriesRegistryLimits {
        self.limits
    }

    /// Returns the live-plus-retired series count.
    #[must_use]
    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    /// Returns the total retained declaration-revision count.
    #[must_use]
    pub const fn declaration_revision_count(&self) -> usize {
        self.declaration_revision_count
    }

    /// Returns registered identities in deterministic nominal byte order.
    #[must_use]
    pub fn series_ids(&self) -> impl ExactSizeIterator<Item = SeriesId> + '_ {
        self.series.keys().copied()
    }

    /// Returns immutable retained history for one series.
    #[must_use]
    pub fn history(&self, series_id: SeriesId) -> Option<&SeriesHistory> {
        self.series.get(&series_id)
    }

    /// Resolves one accepted historical declaration, including after retirement.
    #[must_use]
    pub fn resolve(
        &self,
        series_id: SeriesId,
        revision: DeclarationRevision,
    ) -> Option<&SeriesDeclaration> {
        self.series.get(&series_id).and_then(|history| {
            history
                .declarations
                .iter()
                .find(|declaration| declaration.revision == revision)
        })
    }

    /// Captures a deterministic immutable snapshot with compact history vectors.
    #[must_use]
    pub fn snapshot(&self) -> SeriesRegistrySnapshot {
        SeriesRegistrySnapshot {
            store_id: self.store_id,
            limits: self.limits,
            declaration_revision_count: self.declaration_revision_count,
            series: compact_vec(
                self.series
                    .values()
                    .map(SeriesHistory::compact_clone)
                    .collect(),
            ),
        }
    }

    /// Registers initial declaration revision one or replays it idempotently.
    ///
    /// # Errors
    ///
    /// Refuses terminal retirement, different input for an existing identity,
    /// or either finite capacity bound. Every refusal leaves state unchanged.
    pub fn register(
        &mut self,
        series_id: SeriesId,
        binding: SeriesBinding,
        payload: SeriesDeclarationPayload,
        evidence: DeclarationEvidence,
    ) -> Result<SeriesDeclaration, ModelError> {
        if let Some(history) = self.series.get(&series_id) {
            if history.retirement.is_some() {
                return Err(ModelError::SeriesRetired);
            }
            let initial = history
                .declarations
                .first()
                .ok_or(ModelError::SeriesNotFound)?;
            if history.declarations.len() == 1
                && history.binding == binding
                && initial.payload == payload
                && initial.evidence == evidence
            {
                return Ok(initial.clone());
            }
            return Err(ModelError::SeriesAlreadyRegistered);
        }
        if self.series.len() >= self.limits.max_series {
            return Err(ModelError::RegistrySeriesCapacityExceeded);
        }
        if self.declaration_revision_count >= self.limits.max_declaration_revisions {
            return Err(ModelError::RegistryRevisionCapacityExceeded);
        }

        let declaration = SeriesDeclaration {
            store_id: self.store_id,
            series_id,
            revision: DeclarationRevision::FIRST,
            previous_revision: None,
            binding: binding.clone(),
            payload,
            evidence,
        };
        let history = SeriesHistory {
            series_id,
            binding,
            declarations: vec![declaration.clone()],
            retirement: None,
        };
        let replaced = self.series.insert(series_id, history);
        debug_assert!(replaced.is_none());
        self.declaration_revision_count += 1;
        Ok(declaration)
    }

    /// Accepts the next revision or replays the latest accepted revision exactly.
    ///
    /// Only the revisionable payload and its transition evidence are supplied;
    /// the logical source binding cannot change through this operation.
    ///
    /// # Errors
    ///
    /// Refuses missing or retired series, a stale expected revision, unchanged
    /// payload, exhausted revision numbering, or total revision capacity. Every
    /// refusal leaves state unchanged.
    pub fn revise(
        &mut self,
        series_id: SeriesId,
        expected_revision: DeclarationRevision,
        payload: SeriesDeclarationPayload,
        evidence: DeclarationEvidence,
    ) -> Result<SeriesDeclaration, ModelError> {
        let history = self
            .series
            .get(&series_id)
            .ok_or(ModelError::SeriesNotFound)?;
        if history.retirement.is_some() {
            return Err(ModelError::SeriesRetired);
        }
        let current = history
            .latest_declaration()
            .ok_or(ModelError::SeriesNotFound)?;
        if current.previous_revision == Some(expected_revision)
            && current.payload == payload
            && current.evidence == evidence
        {
            return Ok(current.clone());
        }
        if current.revision != expected_revision {
            return Err(ModelError::StaleDeclarationRevision);
        }
        if current.payload == payload {
            return Err(ModelError::DeclarationUnchanged);
        }
        if self.declaration_revision_count >= self.limits.max_declaration_revisions {
            return Err(ModelError::RegistryRevisionCapacityExceeded);
        }
        let next_revision = current.revision.next();
        let binding = history.binding.clone();
        let declaration = SeriesDeclaration {
            store_id: self.store_id,
            series_id,
            revision: next_revision,
            previous_revision: Some(expected_revision),
            binding,
            payload,
            evidence,
        };

        self.series
            .get_mut(&series_id)
            .ok_or(ModelError::SeriesNotFound)?
            .declarations
            .push(declaration.clone());
        self.declaration_revision_count += 1;
        Ok(declaration)
    }

    /// Terminally retires a series or replays the same retirement exactly.
    ///
    /// # Errors
    ///
    /// Refuses a missing series, stale expected revision, or any non-identical
    /// request after retirement. Retirement never removes retained declarations.
    pub fn retire(
        &mut self,
        series_id: SeriesId,
        expected_revision: DeclarationRevision,
        evidence: DeclarationEvidence,
    ) -> Result<SeriesRetirement, ModelError> {
        let history = self
            .series
            .get(&series_id)
            .ok_or(ModelError::SeriesNotFound)?;
        if let Some(retirement) = &history.retirement {
            if retirement.declaration_revision == expected_revision
                && retirement.evidence == evidence
            {
                return Ok(retirement.clone());
            }
            return Err(ModelError::SeriesRetired);
        }
        if history
            .latest_declaration()
            .ok_or(ModelError::SeriesNotFound)?
            .revision
            != expected_revision
        {
            return Err(ModelError::StaleDeclarationRevision);
        }
        let retirement = SeriesRetirement {
            declaration_revision: expected_revision,
            evidence,
        };
        self.series
            .get_mut(&series_id)
            .ok_or(ModelError::SeriesNotFound)?
            .retirement = Some(retirement.clone());
        Ok(retirement)
    }

    /// Binds one already-valid envelope to the exact current active declaration.
    ///
    /// The returned wrapper can only be created by this registry. Historic
    /// declarations remain resolvable but cannot authorize new evidence.
    ///
    /// # Errors
    ///
    /// Refuses missing or retired series, metadata unequal to the active
    /// revision, or any usable observation outside the declared value family.
    pub fn bind(
        &self,
        envelope: CollectionEnvelope,
    ) -> Result<DeclaredCollectionEnvelope, ModelError> {
        let series_id = envelope.series().series_id();
        let history = self
            .series
            .get(&series_id)
            .ok_or(ModelError::SeriesNotFound)?;
        if history.retirement.is_some() {
            return Err(ModelError::SeriesRetired);
        }
        let declaration = history
            .latest_declaration()
            .ok_or(ModelError::SeriesNotFound)?;
        let expected_metadata = declaration.payload.series_metadata(series_id);
        if envelope.series() != &expected_metadata {
            return Err(ModelError::SeriesMetadataMismatch);
        }
        if envelope
            .observations()
            .iter()
            .any(|observation| !declaration.payload.value_family.admits(observation.value()))
        {
            return Err(ModelError::ObservationValueFamilyMismatch);
        }
        Ok(DeclaredCollectionEnvelope {
            store_id: self.store_id,
            declaration: declaration.clone(),
            envelope,
        })
    }
}

/// An immutable registry-issued declaration binding around one original envelope.
///
/// There is intentionally no public constructor. Possessing a bare envelope or
/// historic declaration is insufficient to construct this active binding.
/// The capability is deliberately not cloneable, so consuming it cannot leave
/// a second authorization token for the same envelope.
///
/// ```compile_fail
/// use och_core::DeclaredCollectionEnvelope;
///
/// fn fork(binding: DeclaredCollectionEnvelope) {
///     let _second_authority = binding.clone();
/// }
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct DeclaredCollectionEnvelope {
    store_id: StoreId,
    declaration: SeriesDeclaration,
    envelope: CollectionEnvelope,
}

impl DeclaredCollectionEnvelope {
    /// Returns the store authority that performed the binding.
    #[must_use]
    pub const fn store_id(&self) -> StoreId {
        self.store_id
    }

    /// Returns the exact active declaration snapshot governing the envelope.
    #[must_use]
    pub const fn declaration(&self) -> &SeriesDeclaration {
        &self.declaration
    }

    /// Returns the original atomically validated envelope.
    #[must_use]
    pub const fn envelope(&self) -> &CollectionEnvelope {
        &self.envelope
    }

    /// Consumes the binding and returns the original envelope.
    #[must_use]
    pub fn into_envelope(self) -> CollectionEnvelope {
        self.envelope
    }

    pub(crate) fn into_parts(self) -> (StoreId, SeriesDeclaration, CollectionEnvelope) {
        (self.store_id, self.declaration, self.envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ATTACKER_SPARE_CAPACITY: usize = 4 * 1_024 * 1_024;

    #[test]
    fn declaration_reference_discards_caller_spare_capacity() {
        let mut input = String::with_capacity(ATTACKER_SPARE_CAPACITY);
        input.push_str("provider/reference");
        assert!(input.capacity() > input.len());
        let reference = DeclarationReference::new(input).expect("valid reference");
        assert_eq!(reference.0.capacity(), reference.0.len());
    }
}
