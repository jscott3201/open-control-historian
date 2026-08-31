#![allow(dead_code)]

pub mod segment_oracle;

use och_core::{
    ArtifactId, ArtifactReference, CanonicalAdmission, CaptureLifecycle, CaptureRunEvidence,
    CollectionEnvelope, CollectionMode, ContentFormat, ContentIdentity, ContentVersion,
    DeclarationEvidence, DeclarationReference, DeclarationRevision, EvidenceId, ExactValue, Gap,
    GapReason, NativeStatus, NativeStatusToken, NoChange, NormalizedRecordEvidence, Observation,
    ObservationId, ObservationTimes, ProducerEpoch, ProducerId, ProducerPosition, ProducerSequence,
    Quality, QualityFlags, QualityLevel, QuantityEvidence, RawRecordEvidence, RetryKey,
    RetryQualification, SeriesBinding, SeriesDeclarationPayload, SeriesId, SeriesMetadata,
    SeriesRegistry, SeriesRegistryLimits, SourceBatchMetadata, SourceEndpointEvidence,
    SourceGapEvidence, SourceGapReason, SourceIdempotency, SourceInterpretation,
    SourceIntervalKind, SourceObservationContext, SourceObservationEvidence, SourceProjection,
    SourceReference, SourceSchemaIdentity, SourceSchemaVersion, SourceSnapshotEvidence,
    SourceSystemEvidence, SourceTransport, StoreId, TimeInterval, Timestamp, UnitEvidence,
    ValueFamily,
};

pub fn uuid_bytes(number: u64) -> [u8; 16] {
    let suffix = number.to_be_bytes();
    [
        0x01, 0x94, 0x1f, 0x29, 0x7c, 0x00, 0x70, 0x00, 0x80, 0x00, suffix[2], suffix[3],
        suffix[4], suffix[5], suffix[6], suffix[7],
    ]
}

pub fn store_id(number: u64) -> StoreId {
    StoreId::from_bytes(uuid_bytes(number)).expect("UUIDv7 store")
}

pub fn series_id(number: u64) -> SeriesId {
    SeriesId::from_bytes(uuid_bytes(number)).expect("UUIDv7 series")
}

pub fn producer_id(number: u64) -> ProducerId {
    ProducerId::from_bytes(uuid_bytes(number)).expect("UUIDv7 producer")
}

pub fn observation_id(number: u64) -> ObservationId {
    ObservationId::from_bytes(uuid_bytes(number)).expect("UUIDv7 observation")
}

pub fn evidence_id(number: u64) -> EvidenceId {
    EvidenceId::from_bytes(uuid_bytes(number)).expect("UUIDv7 evidence")
}

pub fn reference(value: &str) -> DeclarationReference {
    DeclarationReference::new(value.to_owned()).expect("bounded declaration reference")
}

pub fn content(seed: u8) -> ContentIdentity {
    ContentIdentity::new(
        ContentFormat::new("application/octet-stream".to_owned()).expect("bounded format"),
        ContentVersion::new(u128::from(seed)),
        [seed; 32],
    )
}

pub fn artifact(number: u64, seed: u8) -> ArtifactReference {
    ArtifactReference::new(
        ArtifactId::from_bytes(uuid_bytes(number)).expect("UUIDv7 artifact"),
        content(seed),
    )
}

fn source() -> SourceReference {
    SourceReference::with_projection(
        reference("provider:acme"),
        SourceProjection::new("Mqtt".to_owned()).expect("bounded projection"),
        reference("locator:device-1"),
    )
}

fn payload(
    producer: ProducerId,
    mode: CollectionMode,
    family: ValueFamily,
    revised: bool,
) -> SeriesDeclarationPayload {
    SeriesDeclarationPayload::new(
        producer,
        mode,
        family,
        QuantityEvidence::Resolved(reference("quantity:temperature")),
        UnitEvidence::Unresolved(reference("native-unit:degC")),
        Some(reference(if revised {
            "application:ahu-1:revised"
        } else {
            "application:ahu-1"
        })),
    )
}

fn lifecycle() -> CaptureLifecycle {
    CaptureLifecycle::new(
        SourceSystemEvidence::new(
            evidence_id(100),
            reference("provider:acme"),
            SourceProjection::new("Mqtt".to_owned()).expect("bounded projection"),
        ),
        SourceEndpointEvidence::new(
            evidence_id(101),
            evidence_id(100),
            reference("locator:device-1"),
        ),
        CaptureRunEvidence::new(
            evidence_id(102),
            evidence_id(101),
            Timestamp::new(-2, 999_000_000).expect("normalized start"),
            Some(Timestamp::new(3, 4).expect("normalized completion")),
        )
        .expect("ordered capture run"),
        SourceSnapshotEvidence::new(evidence_id(103), evidence_id(102), artifact(200, 20)),
    )
    .expect("linked lifecycle")
}

fn retry_with_key(series: SeriesId, producer: ProducerId, key: &str) -> RetryQualification {
    RetryQualification::new(
        series,
        producer,
        RetryKey::new(key.to_owned()).expect("bounded retry key"),
        content(21),
    )
}

fn retry(series: SeriesId, producer: ProducerId) -> RetryQualification {
    retry_with_key(series, producer, "historian-request")
}

fn batch(interval: SourceIntervalKind) -> SourceBatchMetadata {
    SourceBatchMetadata::new(
        SourceSchemaIdentity::new("studio.source-batch".to_owned()).expect("bounded schema"),
        SourceSchemaVersion::new(7).expect("non-zero schema version"),
        interval,
    )
}

fn registry_bound(
    envelope: CollectionEnvelope,
    family: ValueFamily,
    revised: bool,
) -> och_core::DeclaredCollectionEnvelope {
    let store = store_id(1);
    let series = envelope.series().series_id();
    let producer = envelope.series().producer_id();
    let mode = envelope.series().collection_mode();
    let mut registry = SeriesRegistry::new(store, SeriesRegistryLimits::new(1, 2));
    registry
        .register(
            series,
            SeriesBinding::new(source()),
            payload(producer, mode, family, false),
            DeclarationEvidence::new(
                Timestamp::new(-3, 7).expect("normalized declaration time"),
                Some(artifact(201, 22)),
            ),
        )
        .expect("initial declaration");
    if revised {
        registry
            .revise(
                series,
                DeclarationRevision::FIRST,
                payload(producer, mode, family, true),
                DeclarationEvidence::new(
                    Timestamp::new(-1, 8).expect("normalized revision time"),
                    Some(artifact(202, 23)),
                ),
            )
            .expect("second declaration revision");
    }
    registry.bind(envelope).expect("active declaration binding")
}

fn observation(number: u64, value: ExactValue, positioned: bool) -> Observation {
    observation_with_times(
        number,
        value,
        positioned,
        Timestamp::new(10, 11).expect("receive timestamp"),
        Timestamp::new(9, 12).expect("effective timestamp"),
    )
}

fn observation_with_times(
    number: u64,
    value: ExactValue,
    positioned: bool,
    receive: Timestamp,
    effective: Timestamp,
) -> Observation {
    Observation::new(
        observation_id(number),
        value,
        ObservationTimes::new(
            Some(Timestamp::new(-1, 999_999_999).expect("source timestamp")),
            receive,
            effective,
        ),
        Quality::new(
            QualityLevel::Uncertain,
            QualityFlags::none()
                .with_stale(true)
                .with_substituted(true)
                .with_communication_failure(true),
        ),
        NativeStatus::new(vec![
            NativeStatusToken::new("source-ok".to_owned()).expect("portable token"),
            NativeStatusToken::new("vendor:42".to_owned()).expect("portable token"),
        ])
        .expect("bounded native status"),
        positioned.then(|| {
            ProducerPosition::new(
                ProducerEpoch::new(9),
                ProducerSequence::new(u128::from(number)),
            )
        }),
        None,
    )
}

fn context(
    index: usize,
    canonical_id: ObservationId,
    revised: bool,
    ordinal: u8,
) -> SourceObservationContext {
    let index_u64 = u64::try_from(index).expect("bounded index");
    let index_u8 = u8::try_from(index).expect("source ordinal");
    let observation_evidence = SourceObservationEvidence::new(
        evidence_id(1_000 + index_u64 * 3),
        (index == 0).then(|| artifact(300, 30)),
        if index.is_multiple_of(2) {
            SourceTransport::New
        } else {
            SourceTransport::Redelivered
        },
        (index == 0).then(|| {
            SourceIdempotency::new(
                RetryKey::new("source-observation-key".to_owned()).expect("bounded retry key"),
                content(31),
            )
        }),
    );
    let raw_content = content(40_u8.wrapping_add(index_u8));
    let raw = RawRecordEvidence::new(
        evidence_id(1_001 + index_u64 * 3),
        evidence_id(103),
        ArtifactReference::new(
            ArtifactId::from_bytes(uuid_bytes(400 + index_u64)).expect("UUIDv7 artifact"),
            raw_content.clone(),
        ),
        (index == 0).then(|| {
            SourceIdempotency::new(
                RetryKey::new(format!("raw-record-{index}")).expect("bounded retry key"),
                raw_content,
            )
        }),
    );
    let normalized = NormalizedRecordEvidence::new(
        evidence_id(1_002 + index_u64 * 3),
        raw.evidence_id(),
        content(80_u8.wrapping_add(index_u8)),
        observation_evidence.evidence_id(),
    );
    SourceObservationContext::new(
        ordinal,
        canonical_id,
        SourceInterpretation::new(
            source(),
            Some(reference(if revised {
                "application:ahu-1:revised"
            } else {
                "application:ahu-1"
            })),
            QuantityEvidence::Resolved(reference("quantity:temperature")),
            UnitEvidence::Unresolved(reference("native-unit:degC")),
        ),
        observation_evidence,
        raw,
        normalized,
    )
}

pub fn observed_admission(
    values: Vec<ExactValue>,
    family: ValueFamily,
    gap_count: usize,
    revised: bool,
) -> CanonicalAdmission {
    observed_admission_for_series(values, family, gap_count, revised, 2, 3, 10_000)
}

pub fn observed_admission_for_series(
    values: Vec<ExactValue>,
    family: ValueFamily,
    gap_count: usize,
    revised: bool,
    series_number: u64,
    producer_number: u64,
    first_observation_number: u64,
) -> CanonicalAdmission {
    let positioned = !values.is_empty();
    let observations: Vec<_> = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            observation(first_observation_number + index as u64, value, positioned)
        })
        .collect();
    observed_admission_from_observations(
        observations,
        family,
        gap_count,
        revised,
        series_id(series_number),
        producer_id(producer_number),
    )
}

pub fn observed_admission_with_raw_times(
    entries: &[(u64, i64, u32, i64, u32)],
) -> CanonicalAdmission {
    let observations = entries
        .iter()
        .map(
            |(number, receive_seconds, receive_nanos, effective_seconds, effective_nanos)| {
                observation_with_times(
                    *number,
                    ExactValue::Boolean(true),
                    true,
                    Timestamp::new(*receive_seconds, *receive_nanos)
                        .expect("normalized custom receive time"),
                    Timestamp::new(*effective_seconds, *effective_nanos)
                        .expect("normalized custom effective time"),
                )
            },
        )
        .collect();
    observed_admission_from_observations(
        observations,
        ValueFamily::Boolean,
        0,
        false,
        series_id(2),
        producer_id(3),
    )
}

pub fn observed_admission_with_lineage_ordinal(ordinal: u8) -> CanonicalAdmission {
    observed_admission_from_observations_with_ordinal(
        vec![observation(10_000, ExactValue::Boolean(true), true)],
        ValueFamily::Boolean,
        0,
        false,
        series_id(2),
        producer_id(3),
        ordinal,
    )
}

pub fn observed_admission_with_query_evidence(
    values: Vec<ExactValue>,
    family: ValueFamily,
    first_observation_number: u64,
    first_lineage_ordinal: u8,
    retry_key: &str,
) -> CanonicalAdmission {
    let observations = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| observation(first_observation_number + index as u64, value, true))
        .collect();
    observed_admission_from_observations_with_ordinal_and_retry_key(
        observations,
        family,
        0,
        false,
        series_id(2),
        producer_id(3),
        first_lineage_ordinal,
        retry_key,
    )
}

fn observed_admission_from_observations(
    observations: Vec<Observation>,
    family: ValueFamily,
    gap_count: usize,
    revised: bool,
    series: SeriesId,
    producer: ProducerId,
) -> CanonicalAdmission {
    observed_admission_from_observations_with_ordinal(
        observations,
        family,
        gap_count,
        revised,
        series,
        producer,
        0,
    )
}

fn observed_admission_from_observations_with_ordinal(
    observations: Vec<Observation>,
    family: ValueFamily,
    gap_count: usize,
    revised: bool,
    series: SeriesId,
    producer: ProducerId,
    first_ordinal: u8,
) -> CanonicalAdmission {
    observed_admission_from_observations_with_ordinal_and_retry_key(
        observations,
        family,
        gap_count,
        revised,
        series,
        producer,
        first_ordinal,
        "historian-request",
    )
}

#[allow(clippy::too_many_arguments)]
fn observed_admission_from_observations_with_ordinal_and_retry_key(
    observations: Vec<Observation>,
    family: ValueFamily,
    gap_count: usize,
    revised: bool,
    series: SeriesId,
    producer: ProducerId,
    first_ordinal: u8,
    retry_key: &str,
) -> CanonicalAdmission {
    let gaps: Vec<_> = (0..gap_count)
        .map(|index| {
            let start = 20_000 + index as u128 * 2;
            Gap::new(
                ProducerEpoch::new(10),
                ProducerSequence::new(start),
                ProducerSequence::new(start + 1),
                GapReason::SourceDataLoss,
            )
            .expect("ordered canonical gap")
        })
        .collect();
    let envelope = CollectionEnvelope::observed(
        SeriesMetadata::new(series, producer, CollectionMode::Sampled),
        observations,
        gaps,
    )
    .expect("valid observed envelope");
    let contexts = envelope
        .observations()
        .iter()
        .enumerate()
        .map(|(index, observation)| {
            let index = u8::try_from(index).expect("bounded source context index");
            context(
                usize::from(index),
                observation.observation_id(),
                revised,
                first_ordinal
                    .checked_add(index)
                    .expect("bounded source ordinal fixture"),
            )
        })
        .collect();
    let source_gaps = envelope
        .gaps()
        .iter()
        .map(|gap| {
            SourceGapEvidence::new(
                gap.epoch(),
                gap.start(),
                gap.end(),
                SourceGapReason::SourceUnavailable,
            )
            .expect("valid source gap")
        })
        .collect();
    CanonicalAdmission::observed(
        registry_bound(envelope, family, revised),
        retry_with_key(series, producer, retry_key),
        batch(SourceIntervalKind::Observed),
        lifecycle(),
        contexts,
        source_gaps,
    )
    .expect("valid canonical observed admission")
}

pub fn no_change_admission() -> CanonicalAdmission {
    no_change_admission_with_retry_key("historian-request")
}

pub fn no_change_admission_with_retry_key(key: &str) -> CanonicalAdmission {
    let series = series_id(4);
    let producer = producer_id(5);
    let envelope = CollectionEnvelope::no_change(
        SeriesMetadata::new(series, producer, CollectionMode::ChangeOnly),
        NoChange::new(
            TimeInterval::new(
                Timestamp::new(-10, 0).expect("interval start"),
                Timestamp::new(10, 0).expect("interval end"),
            )
            .expect("non-empty interval"),
        ),
    )
    .expect("valid no-change envelope");
    CanonicalAdmission::no_change(
        registry_bound(envelope, ValueFamily::Boolean, false),
        retry_with_key(series, producer, key),
        batch(SourceIntervalKind::NoChange),
        lifecycle(),
    )
    .expect("valid canonical no-change admission")
}
