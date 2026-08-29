#![forbid(unsafe_code)]
//! Focused deterministic evidence for declaration-authorized source admission.

use och_core::{
    ArtifactId, ArtifactReference, CanonicalAdmission, CaptureLifecycle, CaptureRunEvidence,
    CollectionEnvelope, CollectionMode, ContentFormat, ContentIdentity, ContentVersion,
    DeclarationEvidence, DeclarationReference, EvidenceId, ExactValue, Gap, GapReason,
    MAX_SOURCE_GAP_CONTEXTS, MAX_SOURCE_OBSERVATION_CONTEXTS, ModelError, NativeStatus, NoChange,
    NormalizedRecordEvidence, Observation, ObservationId, ObservationTimes, ProducerEpoch,
    ProducerId, ProducerSequence, Quality, QualityFlags, QualityLevel, QuantityEvidence,
    RawRecordEvidence, RetryKey, RetryQualification, SeriesBinding, SeriesDeclarationPayload,
    SeriesId, SeriesMetadata, SeriesRegistry, SeriesRegistryLimits, SourceBatchMetadata,
    SourceEndpointEvidence, SourceGapEvidence, SourceGapReason, SourceIdempotency,
    SourceInterpretation, SourceIntervalKind, SourceObservationContext, SourceObservationEvidence,
    SourceProjection, SourceReference, SourceSchemaIdentity, SourceSchemaVersion,
    SourceSnapshotEvidence, SourceSystemEvidence, SourceTransport, StoreId, TimeInterval,
    Timestamp, UnitEvidence, ValueFamily,
};

fn uuid_bytes(number: u64) -> [u8; 16] {
    let suffix = number.to_be_bytes();
    [
        0x01, 0x94, 0x1f, 0x29, 0x7c, 0x00, 0x70, 0x00, 0x80, 0x00, suffix[2], suffix[3],
        suffix[4], suffix[5], suffix[6], suffix[7],
    ]
}

fn reference(value: &str) -> DeclarationReference {
    DeclarationReference::new(value.to_owned()).expect("bounded reference")
}

fn content(seed: u8) -> ContentIdentity {
    ContentIdentity::new(
        ContentFormat::new("application/octet-stream".to_owned()).expect("bounded format"),
        ContentVersion::new(1),
        [seed; 32],
    )
}

fn artifact(number: u64, seed: u8) -> ArtifactReference {
    ArtifactReference::new(
        ArtifactId::from_bytes(uuid_bytes(number)).expect("UUIDv7 artifact"),
        content(seed),
    )
}

fn evidence_id(number: u64) -> EvidenceId {
    EvidenceId::from_bytes(uuid_bytes(number)).expect("UUIDv7 source evidence")
}

fn series_id(number: u64) -> SeriesId {
    SeriesId::from_bytes(uuid_bytes(number)).expect("UUIDv7 series")
}

fn producer_id(number: u64) -> ProducerId {
    ProducerId::from_bytes(uuid_bytes(number)).expect("UUIDv7 producer")
}

fn source(projection: Option<&str>, locator: &str) -> SourceReference {
    match projection {
        Some(projection) => SourceReference::with_projection(
            reference("provider:acme"),
            SourceProjection::new(projection.to_owned()).expect("bounded projection"),
            reference(locator),
        ),
        None => SourceReference::new(reference("provider:acme"), reference(locator)),
    }
}

fn payload(producer: ProducerId, mode: CollectionMode) -> SeriesDeclarationPayload {
    SeriesDeclarationPayload::new(
        producer,
        mode,
        ValueFamily::Boolean,
        QuantityEvidence::Resolved(reference("quantity:temperature")),
        UnitEvidence::Resolved(reference("unit:deg-c")),
        Some(reference("application:ahu-1")),
    )
}

fn observation(number: u64) -> Observation {
    let time = Timestamp::new(10, 0).expect("valid time");
    Observation::new(
        ObservationId::from_bytes(uuid_bytes(number)).expect("UUIDv7 observation"),
        ExactValue::Boolean(true),
        ObservationTimes::new(None, time, time),
        Quality::new(QualityLevel::Good, QualityFlags::none()),
        NativeStatus::absent(),
        None,
        None,
    )
}

fn observed_envelope(series: SeriesId, producer: ProducerId, count: usize) -> CollectionEnvelope {
    CollectionEnvelope::observed(
        SeriesMetadata::new(series, producer, CollectionMode::Sampled),
        (0..count)
            .map(|index| observation(10_000 + u64::try_from(index).expect("bounded index")))
            .collect(),
        Vec::new(),
    )
    .expect("valid observed envelope")
}

fn gap_envelope(series: SeriesId, producer: ProducerId, count: usize) -> CollectionEnvelope {
    CollectionEnvelope::observed(
        SeriesMetadata::new(series, producer, CollectionMode::Sampled),
        Vec::new(),
        (0..count)
            .map(|index| {
                let start = u128::try_from(index).expect("bounded index") * 2;
                Gap::new(
                    ProducerEpoch::new(1),
                    ProducerSequence::new(start),
                    ProducerSequence::new(start + 1),
                    GapReason::Unknown,
                )
                .expect("valid ordered gap")
            })
            .collect(),
    )
    .expect("valid gap envelope")
}

fn registry_bound(
    series: SeriesId,
    producer: ProducerId,
    projection: Option<&str>,
    envelope: CollectionEnvelope,
) -> (SeriesRegistry, och_core::DeclaredCollectionEnvelope) {
    let store = StoreId::from_bytes(uuid_bytes(1)).expect("UUIDv7 store");
    let mut registry = SeriesRegistry::new(store, SeriesRegistryLimits::new(4, 8));
    registry
        .register(
            series,
            SeriesBinding::new(source(projection, "locator:device-1")),
            payload(producer, envelope.series().collection_mode()),
            DeclarationEvidence::new(Timestamp::new(1, 0).expect("valid time"), None),
        )
        .expect("registered declaration");
    let bound = registry.bind(envelope).expect("active declaration binding");
    (registry, bound)
}

fn retry(series: SeriesId, producer: ProducerId, seed: u8) -> RetryQualification {
    RetryQualification::new(
        series,
        producer,
        RetryKey::new(format!("historian-request-{seed}")).expect("bounded retry key"),
        content(seed),
    )
}

fn batch(kind: SourceIntervalKind) -> SourceBatchMetadata {
    SourceBatchMetadata::new(
        SourceSchemaIdentity::new("studio.source-batch".to_owned()).expect("bounded schema"),
        SourceSchemaVersion::new(1).expect("non-zero version"),
        kind,
    )
}

fn lifecycle_with(
    provider: &str,
    projection: &str,
    locator: &str,
    ids: [u64; 4],
) -> CaptureLifecycle {
    let system = SourceSystemEvidence::new(
        evidence_id(ids[0]),
        reference(provider),
        SourceProjection::new(projection.to_owned()).expect("bounded projection"),
    );
    let endpoint =
        SourceEndpointEvidence::new(evidence_id(ids[1]), evidence_id(ids[0]), reference(locator));
    let run = CaptureRunEvidence::new(
        evidence_id(ids[2]),
        evidence_id(ids[1]),
        Timestamp::from_unix_milliseconds(1_000),
        Some(Timestamp::from_unix_milliseconds(2_000)),
    )
    .expect("ordered capture run");
    let snapshot =
        SourceSnapshotEvidence::new(evidence_id(ids[3]), evidence_id(ids[2]), artifact(2, 2));
    CaptureLifecycle::new(system, endpoint, run, snapshot).expect("linked lifecycle")
}

fn lifecycle() -> CaptureLifecycle {
    lifecycle_with(
        "provider:acme",
        "projection:mqtt",
        "locator:device-1",
        [100, 101, 102, 103],
    )
}

fn context(ordinal: u8, number: u64, lifecycle: &CaptureLifecycle) -> SourceObservationContext {
    let source_observation = SourceObservationEvidence::new(
        evidence_id(1_000 + number * 3),
        Some(artifact(3_000 + number, 3)),
        SourceTransport::Redelivered,
        Some(SourceIdempotency::new(
            RetryKey::new(format!("source-observation-{number}")).expect("bounded source key"),
            content(4),
        )),
    );
    let raw_artifact = artifact(4_000 + number, 5);
    let raw = RawRecordEvidence::new(
        evidence_id(1_001 + number * 3),
        lifecycle.snapshot().evidence_id(),
        raw_artifact.clone(),
        Some(SourceIdempotency::new(
            RetryKey::new(format!("source-raw-{number}")).expect("bounded raw key"),
            raw_artifact.content().clone(),
        )),
    );
    let normalized = NormalizedRecordEvidence::new(
        evidence_id(1_002 + number * 3),
        raw.evidence_id(),
        content(6),
        source_observation.evidence_id(),
    );
    SourceObservationContext::new(
        ordinal,
        SourceInterpretation::new(
            source(Some("projection:mqtt"), "locator:device-1"),
            Some(reference("application:ahu-1")),
            QuantityEvidence::Resolved(reference("quantity:temperature")),
            UnitEvidence::Resolved(reference("unit:deg-c")),
        ),
        source_observation,
        raw,
        normalized,
    )
}

#[test]
fn projection_schema_and_shared_evidence_identity_are_lossless_and_bounded() {
    for projection in ["File", "Bacnet", "Modbus", "Haystack", "Mqtt", "future:v2"] {
        let source = source(Some(projection), "locator");
        assert_eq!(source.projection().expect("present").as_str(), projection);
    }
    assert!(source(None, "locator").projection().is_none());
    assert_eq!(
        SourceSchemaVersion::new(0),
        Err(ModelError::InvalidSourceSchemaVersion)
    );
    assert_eq!(SourceSchemaVersion::new(1).expect("V1").get(), 1);
    assert_eq!(
        evidence_id(9).to_string(),
        "01941f29-7c00-7000-8000-000000000009"
    );
}

#[test]
fn capture_lifecycle_validates_time_and_exact_links() {
    assert_eq!(
        CaptureRunEvidence::new(
            evidence_id(1),
            evidence_id(2),
            Timestamp::new(2, 0).expect("time"),
            Some(Timestamp::new(1, 0).expect("time")),
        ),
        Err(ModelError::CaptureRunTimeOrder)
    );
    let system = SourceSystemEvidence::new(
        evidence_id(1),
        reference("provider"),
        SourceProjection::new("projection".to_owned()).expect("projection"),
    );
    let endpoint = SourceEndpointEvidence::new(evidence_id(2), evidence_id(9), reference("loc"));
    let run = CaptureRunEvidence::new(
        evidence_id(3),
        evidence_id(2),
        Timestamp::new(1, 0).expect("time"),
        None,
    )
    .expect("open run");
    let snapshot = SourceSnapshotEvidence::new(evidence_id(4), evidence_id(3), artifact(5, 1));
    assert_eq!(
        CaptureLifecycle::new(system, endpoint, run, snapshot),
        Err(ModelError::SourceEndpointSystemMismatch)
    );

    let system = SourceSystemEvidence::new(
        evidence_id(1),
        reference("provider"),
        SourceProjection::new("projection".to_owned()).expect("projection"),
    );
    let endpoint = SourceEndpointEvidence::new(evidence_id(2), evidence_id(1), reference("loc"));
    let run = CaptureRunEvidence::new(
        evidence_id(3),
        evidence_id(9),
        Timestamp::new(1, 0).expect("time"),
        None,
    )
    .expect("open run");
    let snapshot = SourceSnapshotEvidence::new(evidence_id(4), evidence_id(3), artifact(5, 1));
    assert_eq!(
        CaptureLifecycle::new(system, endpoint, run, snapshot),
        Err(ModelError::CaptureRunEndpointMismatch)
    );

    let system = SourceSystemEvidence::new(
        evidence_id(1),
        reference("provider"),
        SourceProjection::new("projection".to_owned()).expect("projection"),
    );
    let endpoint = SourceEndpointEvidence::new(evidence_id(2), evidence_id(1), reference("loc"));
    let run = CaptureRunEvidence::new(
        evidence_id(3),
        evidence_id(2),
        Timestamp::new(1, 0).expect("time"),
        None,
    )
    .expect("open run");
    let snapshot = SourceSnapshotEvidence::new(evidence_id(4), evidence_id(9), artifact(5, 1));
    assert_eq!(
        CaptureLifecycle::new(system, endpoint, run, snapshot),
        Err(ModelError::SourceSnapshotRunMismatch)
    );
}

#[test]
fn observed_admission_retains_exact_lineage_and_keeps_retry_independent() {
    let series = series_id(10);
    let producer = producer_id(11);
    let envelope = observed_envelope(series, producer, 1);
    let (registry, declared) = registry_bound(series, producer, Some("projection:mqtt"), envelope);
    let before = registry.snapshot();
    let capture = lifecycle();
    let request_retry = retry(series, producer, 9);
    let admission = CanonicalAdmission::observed(
        declared,
        request_retry.clone(),
        batch(SourceIntervalKind::Observed),
        capture.clone(),
        vec![context(7, 0, &capture)],
        Vec::new(),
    )
    .expect("valid admission");
    assert_eq!(registry.snapshot(), before);
    assert_eq!(admission.evidence_kind(), SourceIntervalKind::Observed);
    assert_eq!(admission.observations()[0].ordinal(), 7);
    assert_eq!(
        admission.observations()[0].observation().transport(),
        SourceTransport::Redelivered
    );
    assert_ne!(
        admission.observations()[0]
            .observation()
            .idempotency()
            .expect("source idempotency")
            .key(),
        admission.retry().key()
    );
    assert_eq!(admission.lifecycle(), &capture);
    assert_eq!(admission.retry(), &request_retry);
    assert_eq!(
        admission.observations().len(),
        admission.envelope().observations().len()
    );

    let (_, declared) = registry_bound(
        series,
        producer,
        Some("projection:mqtt"),
        observed_envelope(series, producer, 1),
    );
    let source_observation =
        SourceObservationEvidence::new(evidence_id(800), None, SourceTransport::New, None);
    let raw = RawRecordEvidence::new(
        evidence_id(801),
        capture.snapshot().evidence_id(),
        artifact(801, 8),
        None,
    );
    let normalized = NormalizedRecordEvidence::new(
        evidence_id(802),
        raw.evidence_id(),
        content(9),
        source_observation.evidence_id(),
    );
    let optional_absent = SourceObservationContext::new(
        0,
        SourceInterpretation::new(
            source(Some("projection:mqtt"), "locator:device-1"),
            Some(reference("application:ahu-1")),
            QuantityEvidence::Resolved(reference("quantity:temperature")),
            UnitEvidence::Resolved(reference("unit:deg-c")),
        ),
        source_observation,
        raw,
        normalized,
    );
    let admission = CanonicalAdmission::observed(
        declared,
        retry(series, producer, 10),
        batch(SourceIntervalKind::Observed),
        capture,
        vec![optional_absent],
        Vec::new(),
    )
    .expect("optional source artifacts and idempotencies may be absent");
    assert_eq!(
        admission.observations()[0].observation().transport(),
        SourceTransport::New
    );
    assert!(
        admission.observations()[0]
            .observation()
            .provenance_artifact()
            .is_none()
    );
    assert!(
        admission.observations()[0]
            .observation()
            .idempotency()
            .is_none()
    );
    assert!(admission.observations()[0].raw().idempotency().is_none());
}

#[test]
fn admission_refuses_projection_interval_scope_and_binding_without_registry_mutation() {
    let series = series_id(20);
    let producer = producer_id(21);
    let envelope = observed_envelope(series, producer, 1);
    let (registry, declared) = registry_bound(series, producer, None, envelope.clone());
    let before = registry.snapshot();
    let capture = lifecycle();
    assert_eq!(
        CanonicalAdmission::observed(
            declared,
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            vec![context(0, 0, &capture)],
            Vec::new(),
        ),
        Err(ModelError::SourceProjectionRequired)
    );
    assert_eq!(registry.snapshot(), before);

    let (_, declared) = registry_bound(series, producer, Some("projection:mqtt"), envelope.clone());
    assert_eq!(
        CanonicalAdmission::observed(
            declared,
            retry(series_id(99), producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            vec![context(0, 0, &capture)],
            Vec::new(),
        ),
        Err(ModelError::AdmissionRetryScopeMismatch)
    );
    let (_, declared) = registry_bound(series, producer, Some("projection:mqtt"), envelope.clone());
    assert_eq!(
        CanonicalAdmission::observed(
            declared,
            retry(series, producer, 1),
            batch(SourceIntervalKind::NoChange),
            capture.clone(),
            vec![context(0, 0, &capture)],
            Vec::new(),
        ),
        Err(ModelError::SourceIntervalMismatch)
    );
    let (_, declared) = registry_bound(series, producer, Some("projection:mqtt"), envelope);
    let wrong = lifecycle_with(
        "provider:other",
        "projection:mqtt",
        "locator:device-1",
        [200, 201, 202, 203],
    );
    assert_eq!(
        CanonicalAdmission::observed(
            declared,
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            wrong.clone(),
            vec![context(0, 0, &wrong)],
            Vec::new(),
        ),
        Err(ModelError::SourceLifecycleBindingMismatch)
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn counts_ordinals_ids_snapshot_and_interpretation_fail_closed() {
    let series = series_id(30);
    let producer = producer_id(31);
    let capture = lifecycle();
    let make_bound = || {
        registry_bound(
            series,
            producer,
            Some("projection:mqtt"),
            observed_envelope(series, producer, 2),
        )
        .1
    };
    assert_eq!(
        CanonicalAdmission::observed(
            make_bound(),
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            vec![context(0, 0, &capture)],
            Vec::new(),
        ),
        Err(ModelError::SourceObservationCountMismatch)
    );
    assert_eq!(
        CanonicalAdmission::observed(
            make_bound(),
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            vec![context(1, 0, &capture), context(1, 1, &capture)],
            Vec::new(),
        ),
        Err(ModelError::MisorderedSourceRecordOrdinals)
    );
    assert_eq!(
        CanonicalAdmission::observed(
            make_bound(),
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            vec![context(0, 0, &capture), context(1, 0, &capture)],
            Vec::new(),
        ),
        Err(ModelError::DuplicateSourceEvidenceId)
    );

    let source_observation =
        SourceObservationEvidence::new(evidence_id(700), None, SourceTransport::New, None);
    let raw = RawRecordEvidence::new(evidence_id(701), evidence_id(999), artifact(700, 7), None);
    let normalized = NormalizedRecordEvidence::new(
        evidence_id(702),
        evidence_id(701),
        content(8),
        evidence_id(700),
    );
    let bad_snapshot = SourceObservationContext::new(
        0,
        SourceInterpretation::new(
            source(Some("projection:mqtt"), "locator:device-1"),
            Some(reference("application:ahu-1")),
            QuantityEvidence::Resolved(reference("quantity:temperature")),
            UnitEvidence::Resolved(reference("unit:deg-c")),
        ),
        source_observation,
        raw,
        normalized,
    );
    let one_bound = || {
        registry_bound(
            series,
            producer,
            Some("projection:mqtt"),
            observed_envelope(series, producer, 1),
        )
        .1
    };
    assert_eq!(
        CanonicalAdmission::observed(
            one_bound(),
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            vec![bad_snapshot],
            Vec::new(),
        ),
        Err(ModelError::SourceRawSnapshotMismatch)
    );

    let valid = context(0, 0, &capture);
    let bad_interpretation = SourceObservationContext::new(
        0,
        SourceInterpretation::new(
            source(Some("projection:mqtt"), "locator:other"),
            Some(reference("application:ahu-1")),
            QuantityEvidence::Resolved(reference("quantity:temperature")),
            UnitEvidence::Resolved(reference("unit:deg-c")),
        ),
        valid.observation().clone(),
        valid.raw().clone(),
        valid.normalized().clone(),
    );
    assert_eq!(
        CanonicalAdmission::observed(
            one_bound(),
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture,
            vec![bad_interpretation],
            Vec::new(),
        ),
        Err(ModelError::SourceInterpretationMismatch)
    );
}

#[test]
fn record_pair_link_and_raw_idempotency_mismatches_are_distinct() {
    let series = series_id(40);
    let producer = producer_id(41);
    let capture = lifecycle();
    let bound = || {
        registry_bound(
            series,
            producer,
            Some("projection:mqtt"),
            observed_envelope(series, producer, 1),
        )
        .1
    };
    let base = context(0, 0, &capture);
    let wrong_raw = NormalizedRecordEvidence::new(
        evidence_id(900),
        evidence_id(999),
        content(1),
        base.observation().evidence_id(),
    );
    let case = SourceObservationContext::new(
        0,
        base.interpretation().clone(),
        base.observation().clone(),
        base.raw().clone(),
        wrong_raw,
    );
    assert_eq!(
        CanonicalAdmission::observed(
            bound(),
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            vec![case],
            Vec::new(),
        ),
        Err(ModelError::SourceNormalizedRawMismatch)
    );
    let wrong_observation = NormalizedRecordEvidence::new(
        evidence_id(901),
        base.raw().evidence_id(),
        content(1),
        evidence_id(999),
    );
    let case = SourceObservationContext::new(
        0,
        base.interpretation().clone(),
        base.observation().clone(),
        base.raw().clone(),
        wrong_observation,
    );
    assert_eq!(
        CanonicalAdmission::observed(
            bound(),
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            vec![case],
            Vec::new(),
        ),
        Err(ModelError::SourceNormalizedObservationMismatch)
    );
    let raw = RawRecordEvidence::new(
        evidence_id(902),
        capture.snapshot().evidence_id(),
        artifact(902, 2),
        Some(SourceIdempotency::new(
            RetryKey::new("raw-key".to_owned()).expect("key"),
            content(3),
        )),
    );
    let normalized = NormalizedRecordEvidence::new(
        evidence_id(903),
        raw.evidence_id(),
        content(4),
        base.observation().evidence_id(),
    );
    let case = SourceObservationContext::new(
        0,
        base.interpretation().clone(),
        base.observation().clone(),
        raw,
        normalized,
    );
    assert_eq!(
        CanonicalAdmission::observed(
            bound(),
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture,
            vec![case],
            Vec::new(),
        ),
        Err(ModelError::SourceRawIdempotencyMismatch)
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn gaps_and_no_change_are_exact_distinct_and_retain_shared_lifecycle() {
    let series = series_id(50);
    let producer = producer_id(51);
    let envelope = gap_envelope(series, producer, 2);
    let (_, declared) = registry_bound(series, producer, Some("projection:mqtt"), envelope);
    let capture = lifecycle();
    let gaps = vec![
        SourceGapEvidence::new(
            ProducerEpoch::new(1),
            ProducerSequence::new(0),
            ProducerSequence::new(1),
            SourceGapReason::CommunicationFailure,
        )
        .expect("gap"),
        SourceGapEvidence::new(
            ProducerEpoch::new(1),
            ProducerSequence::new(2),
            ProducerSequence::new(3),
            SourceGapReason::Filtered,
        )
        .expect("gap"),
    ];
    let admission = CanonicalAdmission::observed(
        declared,
        retry(series, producer, 1),
        batch(SourceIntervalKind::Observed),
        capture.clone(),
        Vec::new(),
        gaps,
    )
    .expect("gap-only observed admission");
    assert_eq!(admission.gaps()[1].reason(), SourceGapReason::Filtered);
    for reason in [
        SourceGapReason::CommunicationFailure,
        SourceGapReason::SourceUnavailable,
        SourceGapReason::ProducerReset,
        SourceGapReason::Filtered,
        SourceGapReason::Unknown,
    ] {
        assert_eq!(
            SourceGapEvidence::new(
                ProducerEpoch::new(1),
                ProducerSequence::new(1),
                ProducerSequence::new(2),
                reason,
            )
            .expect("source gap")
            .reason(),
            reason
        );
    }

    let (_, declared) = registry_bound(
        series,
        producer,
        Some("projection:mqtt"),
        gap_envelope(series, producer, 1),
    );
    assert_eq!(
        CanonicalAdmission::observed(
            declared,
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            Vec::new(),
            Vec::new(),
        ),
        Err(ModelError::SourceGapCountMismatch)
    );
    let (_, declared) = registry_bound(
        series,
        producer,
        Some("projection:mqtt"),
        gap_envelope(series, producer, 1),
    );
    let wrong_gap = SourceGapEvidence::new(
        ProducerEpoch::new(1),
        ProducerSequence::new(0),
        ProducerSequence::new(2),
        SourceGapReason::Unknown,
    )
    .expect("non-empty wrong gap");
    assert_eq!(
        CanonicalAdmission::observed(
            declared,
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            Vec::new(),
            vec![wrong_gap],
        ),
        Err(ModelError::SourceGapMismatch)
    );

    let no_change = CollectionEnvelope::no_change(
        SeriesMetadata::new(series, producer, CollectionMode::ChangeOnly),
        NoChange::new(
            TimeInterval::new(
                Timestamp::new(1, 0).expect("time"),
                Timestamp::new(2, 0).expect("time"),
            )
            .expect("interval"),
        ),
    )
    .expect("no-change envelope");
    let (_, declared) = registry_bound(series, producer, Some("projection:mqtt"), no_change);
    let admission = CanonicalAdmission::no_change(
        declared,
        retry(series, producer, 2),
        batch(SourceIntervalKind::NoChange),
        capture.clone(),
    )
    .expect("no-change admission");
    assert!(admission.observations().is_empty());
    assert!(admission.gaps().is_empty());
    assert_eq!(admission.lifecycle(), &capture);
}

#[test]
fn duplicate_lifecycle_evidence_identity_is_refused() {
    let series = series_id(55);
    let producer = producer_id(56);
    let (_, declared) = registry_bound(
        series,
        producer,
        Some("projection:mqtt"),
        observed_envelope(series, producer, 1),
    );
    let duplicate = lifecycle_with(
        "provider:acme",
        "projection:mqtt",
        "locator:device-1",
        [500, 500, 500, 500],
    );
    assert_eq!(
        CanonicalAdmission::observed(
            declared,
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            duplicate.clone(),
            vec![context(0, 0, &duplicate)],
            Vec::new(),
        ),
        Err(ModelError::DuplicateSourceEvidenceId)
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn source_context_and_gap_capacity_boundaries_are_exact() {
    let series = series_id(60);
    let producer = producer_id(61);
    let capture = lifecycle();
    let envelope = observed_envelope(series, producer, MAX_SOURCE_OBSERVATION_CONTEXTS);
    let (_, declared) = registry_bound(series, producer, Some("projection:mqtt"), envelope);
    let contexts = (0..MAX_SOURCE_OBSERVATION_CONTEXTS)
        .map(|index| {
            context(
                u8::try_from(index).expect("0..=255 ordinal"),
                u64::try_from(index).expect("bounded"),
                &capture,
            )
        })
        .collect();
    assert_eq!(
        CanonicalAdmission::observed(
            declared,
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            contexts,
            Vec::new(),
        )
        .expect("exact observation maximum")
        .observations()
        .len(),
        MAX_SOURCE_OBSERVATION_CONTEXTS
    );
    let (_, declared) = registry_bound(
        series,
        producer,
        Some("projection:mqtt"),
        observed_envelope(series, producer, 1),
    );
    let too_many = (0..=MAX_SOURCE_OBSERVATION_CONTEXTS)
        .map(|index| {
            context(
                u8::try_from(index.min(255)).expect("clamped"),
                u64::try_from(index).expect("bounded"),
                &capture,
            )
        })
        .collect();
    assert_eq!(
        CanonicalAdmission::observed(
            declared,
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            too_many,
            Vec::new(),
        ),
        Err(ModelError::TooManySourceObservationContexts)
    );

    let envelope = gap_envelope(series, producer, MAX_SOURCE_GAP_CONTEXTS);
    let (_, declared) = registry_bound(series, producer, Some("projection:mqtt"), envelope);
    let gaps = (0..MAX_SOURCE_GAP_CONTEXTS)
        .map(|index| {
            let start = u128::try_from(index).expect("bounded") * 2;
            SourceGapEvidence::new(
                ProducerEpoch::new(1),
                ProducerSequence::new(start),
                ProducerSequence::new(start + 1),
                SourceGapReason::Unknown,
            )
            .expect("gap")
        })
        .collect();
    assert_eq!(
        CanonicalAdmission::observed(
            declared,
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture.clone(),
            Vec::new(),
            gaps,
        )
        .expect("exact gap maximum")
        .gaps()
        .len(),
        MAX_SOURCE_GAP_CONTEXTS
    );
    let (_, declared) = registry_bound(
        series,
        producer,
        Some("projection:mqtt"),
        gap_envelope(series, producer, 1),
    );
    let too_many = (0..=MAX_SOURCE_GAP_CONTEXTS)
        .map(|index| {
            let start = u128::try_from(index).expect("bounded") * 2;
            SourceGapEvidence::new(
                ProducerEpoch::new(1),
                ProducerSequence::new(start),
                ProducerSequence::new(start + 1),
                SourceGapReason::Unknown,
            )
            .expect("gap")
        })
        .collect();
    assert_eq!(
        CanonicalAdmission::observed(
            declared,
            retry(series, producer, 1),
            batch(SourceIntervalKind::Observed),
            capture,
            Vec::new(),
            too_many,
        ),
        Err(ModelError::TooManySourceGapContexts)
    );
}

#[test]
fn already_issued_binding_keeps_its_revision_but_rebind_requires_a_new_series() {
    let old_series = series_id(70);
    let producer = producer_id(71);
    let envelope = observed_envelope(old_series, producer, 1);
    let (mut registry, declared) =
        registry_bound(old_series, producer, Some("projection:mqtt"), envelope);
    let authorized_revision = declared.declaration().clone();
    let before_refusal = registry.snapshot();
    assert_eq!(
        registry.register(
            old_series,
            SeriesBinding::new(source(Some("projection:file"), "locator:other")),
            payload(producer, CollectionMode::Sampled),
            DeclarationEvidence::new(Timestamp::new(2, 0).expect("time"), None),
        ),
        Err(ModelError::SeriesAlreadyRegistered)
    );
    assert_eq!(registry.snapshot(), before_refusal);

    let revised = registry
        .revise(
            old_series,
            authorized_revision.revision(),
            payload(producer_id(72), CollectionMode::Event),
            DeclarationEvidence::new(Timestamp::new(3, 0).expect("time"), None),
        )
        .expect("revision");
    registry
        .retire(
            old_series,
            revised.revision(),
            DeclarationEvidence::new(Timestamp::new(4, 0).expect("time"), None),
        )
        .expect("retirement");
    assert_eq!(
        registry.bind(observed_envelope(old_series, producer_id(72), 1)),
        Err(ModelError::SeriesRetired)
    );

    let capture = lifecycle();
    let admission = CanonicalAdmission::observed(
        declared,
        retry(old_series, producer, 1),
        batch(SourceIntervalKind::Observed),
        capture.clone(),
        vec![context(0, 0, &capture)],
        Vec::new(),
    )
    .expect("binding was issued while revision one was active");
    assert_eq!(admission.declaration(), &authorized_revision);

    let replacement = series_id(73);
    registry
        .register(
            replacement,
            SeriesBinding::new(source(Some("projection:file"), "locator:other")),
            payload(producer, CollectionMode::Sampled),
            DeclarationEvidence::new(Timestamp::new(5, 0).expect("time"), None),
        )
        .expect("new logical point has a new SeriesId");
}
