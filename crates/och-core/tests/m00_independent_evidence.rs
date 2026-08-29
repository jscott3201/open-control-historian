#![forbid(unsafe_code)]
//! Independent fixture, oracle, golden, and public-adapter evidence for M00.

#[path = "m00_independent_evidence/fixtures.rs"]
mod fixtures;
#[path = "m00_independent_evidence/oracle.rs"]
mod oracle;
#[path = "m00_independent_evidence/series_oracle.rs"]
mod series_oracle;

use fixtures::{
    ConstructorCase, RawEnvelope, RawError, RawEvidence, RawGap, RawMode, RawObservation, RawRetry,
    RawTimestamp,
};
use och_core::{
    ArtifactId, ArtifactReference, CollectionEnvelope, CollectionMode, ContentFormat,
    ContentIdentity, ContentVersion, DeclarationEvidence, DeclarationReference,
    DeclarationRevision, EvidenceKind, ExactText, ExactValue, Gap, GapReason, ModelError,
    NativeStatus, NativeStatusToken, NoChange, Observation, ObservationId, ObservationTimes,
    ProducerEpoch, ProducerId, ProducerPosition, ProducerSequence, Quality, QualityFlags,
    QualityLevel, QuantityEvidence, RealBits, RetryClassification, RetryKey, RetryQualification,
    SeriesBinding, SeriesDeclarationPayload, SeriesId, SeriesMetadata, SeriesRegistry,
    SeriesRegistryLimits, SourceReference, StateClass, StateMember, StateValue, StoreId,
    TimeInterval, Timestamp, Unavailable, UnavailableReason, UnitEvidence, ValueFamily,
};
use std::collections::BTreeSet;

const FIXTURE_SOURCE: &str = include_str!("m00_independent_evidence/fixtures.rs");
const ORACLE_SOURCE: &str = include_str!("m00_independent_evidence/oracle.rs");
const SERIES_ORACLE_SOURCE: &str = include_str!("m00_independent_evidence/series_oracle.rs");
const IDENTITY_SOURCE: &str = include_str!("../src/identity.rs");
const GOLDEN: &str = include_str!("fixtures/m00-pr03-evidence-v1.txt");

fn actual_mode(mode: RawMode) -> CollectionMode {
    match mode {
        RawMode::Sampled => CollectionMode::Sampled,
        RawMode::ChangeOnly => CollectionMode::ChangeOnly,
        RawMode::Cumulative => CollectionMode::Cumulative,
        RawMode::Interval => CollectionMode::Interval,
        RawMode::Event => CollectionMode::Event,
    }
}

fn actual_timestamp(raw: RawTimestamp) -> Result<Timestamp, ModelError> {
    Timestamp::new(raw.seconds, raw.nanoseconds)
}

fn actual_interval(raw: fixtures::RawInterval) -> Result<TimeInterval, ModelError> {
    TimeInterval::new(actual_timestamp(raw.start)?, actual_timestamp(raw.end)?)
}

fn actual_series(mode: RawMode) -> SeriesMetadata {
    SeriesMetadata::new(
        SeriesId::from_bytes(fixtures::uuid_bytes(1)).expect("valid raw series UUIDv7"),
        ProducerId::from_bytes(fixtures::uuid_bytes(16)).expect("valid raw producer UUIDv7"),
        actual_mode(mode),
    )
}

fn actual_observation(raw: &RawObservation) -> Observation {
    Observation::new(
        ObservationId::from_bytes(raw.id).expect("valid raw observation UUIDv7"),
        ExactValue::Boolean(true),
        ObservationTimes::new(
            raw.source
                .map(|source| actual_timestamp(source).expect("normalized raw source time")),
            actual_timestamp(raw.receive).expect("normalized raw receive time"),
            actual_timestamp(raw.effective).expect("normalized raw effective time"),
        ),
        Quality::new(QualityLevel::Unknown, QualityFlags::none()),
        NativeStatus::absent(),
        raw.position.map(|position| {
            ProducerPosition::new(
                ProducerEpoch::new(position.epoch),
                ProducerSequence::new(position.sequence),
            )
        }),
        raw.interval.map(|interval| {
            actual_interval(interval).expect("non-empty normalized raw observation interval")
        }),
    )
}

fn actual_gap(raw: RawGap, reason: GapReason) -> Result<Gap, ModelError> {
    Gap::new(
        ProducerEpoch::new(raw.epoch),
        ProducerSequence::new(raw.start),
        ProducerSequence::new(raw.end),
        reason,
    )
}

fn actual_envelope(raw: &RawEnvelope) -> Result<CollectionEnvelope, ModelError> {
    match &raw.evidence {
        RawEvidence::Observed { observations, gaps } => CollectionEnvelope::observed(
            actual_series(raw.mode),
            observations.iter().map(actual_observation).collect(),
            gaps.iter()
                .map(|gap| actual_gap(*gap, GapReason::Unknown).expect("non-empty raw gap"))
                .collect(),
        ),
        RawEvidence::NoChange { interval } => CollectionEnvelope::no_change(
            actual_series(raw.mode),
            NoChange::new(actual_interval(*interval).expect("non-empty raw no-change interval")),
        ),
    }
}

fn actual_constructor(case: &ConstructorCase) -> Result<(), ModelError> {
    match case {
        ConstructorCase::IdentityText(text) => SeriesId::parse(text).map(|_| ()),
        ConstructorCase::ExactText(text) => ExactText::new(text.clone()).map(|_| ()),
        ConstructorCase::PortableToken(text) => StateClass::new(text.clone()).map(|_| ()),
        ConstructorCase::ContentFormat(text) => ContentFormat::new(text.clone()).map(|_| ()),
        ConstructorCase::RetryKey(text) => RetryKey::new(text.clone()).map(|_| ()),
        ConstructorCase::CanonicalDecimal(text) => ProducerEpoch::parse(text).map(|_| ()),
        ConstructorCase::TimestampNew(timestamp) => actual_timestamp(*timestamp).map(|_| ()),
        ConstructorCase::TimestampToMilliseconds(timestamp) => actual_timestamp(*timestamp)?
            .to_unix_milliseconds()
            .map(|_| ()),
        ConstructorCase::TimeInterval(interval) => actual_interval(*interval).map(|_| ()),
        ConstructorCase::Gap(gap) => actual_gap(*gap, GapReason::Unknown).map(|_| ()),
        ConstructorCase::NativeStatusCount(count) => {
            let token = NativeStatusToken::new("status".to_owned()).expect("valid status token");
            NativeStatus::new(vec![token; *count]).map(|_| ())
        }
    }
}

const fn error_code(error: ModelError) -> RawError {
    match error {
        ModelError::InvalidIdentity => RawError::InvalidIdentity,
        ModelError::InvalidExactText => RawError::InvalidExactText,
        ModelError::InvalidPortableToken => RawError::InvalidPortableToken,
        ModelError::InvalidContentFormat => RawError::InvalidContentFormat,
        ModelError::InvalidRetryKey => RawError::InvalidRetryKey,
        ModelError::InvalidCanonicalDecimal => RawError::InvalidCanonicalDecimal,
        ModelError::InvalidNanosecond => RawError::InvalidNanosecond,
        ModelError::InexactUnixMilliseconds => RawError::InexactUnixMilliseconds,
        ModelError::UnixMillisecondsOverflow => RawError::UnixMillisecondsOverflow,
        ModelError::EmptyTimeInterval => RawError::EmptyTimeInterval,
        ModelError::EmptyGap => RawError::EmptyGap,
        ModelError::TooManyNativeStatusTokens => RawError::TooManyNativeStatusTokens,
        ModelError::TooManyObservations => RawError::TooManyObservations,
        ModelError::TooManyGaps => RawError::TooManyGaps,
        ModelError::EmptyObservedEvidence => RawError::EmptyObservedEvidence,
        ModelError::DuplicateObservationId => RawError::DuplicateObservationId,
        ModelError::MixedProducerPositions => RawError::MixedProducerPositions,
        ModelError::MisorderedProducerPositions => RawError::MisorderedProducerPositions,
        ModelError::MisorderedGaps => RawError::MisorderedGaps,
        ModelError::OverlappingGaps => RawError::OverlappingGaps,
        ModelError::ObservationInsideGap => RawError::ObservationInsideGap,
        ModelError::MissingObservationInterval => RawError::MissingObservationInterval,
        ModelError::UnexpectedObservationInterval => RawError::UnexpectedObservationInterval,
        ModelError::InvalidNoChangeMode => RawError::InvalidNoChangeMode,
        ModelError::InvalidDeclarationReference => RawError::InvalidDeclarationReference,
        ModelError::InvalidDeclarationRevision => RawError::InvalidDeclarationRevision,
        ModelError::RegistrySeriesCapacityExceeded => RawError::RegistrySeriesCapacityExceeded,
        ModelError::RegistryRevisionCapacityExceeded => RawError::RegistryRevisionCapacityExceeded,
        ModelError::SeriesAlreadyRegistered => RawError::SeriesAlreadyRegistered,
        ModelError::SeriesNotFound => RawError::SeriesNotFound,
        ModelError::SeriesRetired => RawError::SeriesRetired,
        ModelError::StaleDeclarationRevision => RawError::StaleDeclarationRevision,
        ModelError::DeclarationUnchanged => RawError::DeclarationUnchanged,
        ModelError::SeriesMetadataMismatch => RawError::SeriesMetadataMismatch,
        ModelError::ObservationValueFamilyMismatch => RawError::ObservationValueFamilyMismatch,
    }
}

fn series_number(series_id: SeriesId) -> u8 {
    series_id.into_bytes()[15]
}

fn producer_number(producer_id: ProducerId) -> u8 {
    producer_id.into_bytes()[15]
}

fn actual_store() -> StoreId {
    StoreId::from_bytes(fixtures::uuid_bytes(100)).expect("valid raw store UUIDv7")
}

fn actual_series_id(number: u8) -> SeriesId {
    SeriesId::from_bytes(fixtures::uuid_bytes(u64::from(number))).expect("valid raw series UUIDv7")
}

fn actual_producer_id(number: u8) -> ProducerId {
    ProducerId::from_bytes(fixtures::uuid_bytes(u64::from(number)))
        .expect("valid raw producer UUIDv7")
}

fn actual_declaration_reference(value: String) -> DeclarationReference {
    DeclarationReference::new(value).expect("valid raw declaration reference")
}

fn actual_series_binding(code: u8) -> SeriesBinding {
    SeriesBinding::new(SourceReference::new(
        actual_declaration_reference("provider".to_owned()),
        actual_declaration_reference(format!("binding-{code}")),
    ))
}

const fn actual_series_mode(code: u8) -> CollectionMode {
    match code {
        1 => CollectionMode::Sampled,
        2 => CollectionMode::ChangeOnly,
        3 => CollectionMode::Event,
        _ => panic!("raw fixture uses a closed collection-mode code"),
    }
}

const fn actual_value_family(code: u8) -> ValueFamily {
    match code {
        1 => ValueFamily::Boolean,
        2 => ValueFamily::Signed,
        3 => ValueFamily::Text,
        _ => panic!("raw fixture uses a closed value-family code"),
    }
}

fn actual_series_payload(raw: series_oracle::RawPayload) -> SeriesDeclarationPayload {
    SeriesDeclarationPayload::new(
        actual_producer_id(raw.producer),
        actual_series_mode(raw.mode),
        actual_value_family(raw.family),
        QuantityEvidence::Absent,
        UnitEvidence::Absent,
        Some(actual_declaration_reference(format!(
            "metadata-{}",
            raw.metadata
        ))),
    )
}

fn actual_declaration_evidence(code: u8) -> DeclarationEvidence {
    DeclarationEvidence::new(
        Timestamp::new(i64::from(code), 0).expect("normalized raw evidence time"),
        None,
    )
}

fn actual_series_value(value: series_oracle::RawValue) -> ExactValue {
    match value {
        series_oracle::RawValue::Family(1) => ExactValue::Boolean(true),
        series_oracle::RawValue::Family(2) => ExactValue::Signed(2),
        series_oracle::RawValue::Family(3) => {
            ExactValue::Text(ExactText::new("value".to_owned()).expect("bounded raw exact text"))
        }
        series_oracle::RawValue::Family(_) => panic!("raw fixture uses a closed value code"),
        series_oracle::RawValue::Unavailable => {
            ExactValue::Unavailable(Unavailable::without_reason())
        }
    }
}

fn actual_series_envelope(
    series: u8,
    producer: u8,
    mode: u8,
    value: series_oracle::RawValue,
) -> CollectionEnvelope {
    let timestamp = Timestamp::new(50, 0).expect("normalized raw observation time");
    let observation = Observation::new(
        ObservationId::from_bytes(fixtures::uuid_bytes(500)).expect("valid raw observation UUIDv7"),
        actual_series_value(value),
        ObservationTimes::new(None, timestamp, timestamp),
        Quality::new(QualityLevel::Unknown, QualityFlags::none()),
        NativeStatus::absent(),
        None,
        None,
    );
    CollectionEnvelope::observed(
        SeriesMetadata::new(
            actual_series_id(series),
            actual_producer_id(producer),
            actual_series_mode(mode),
        ),
        vec![observation],
        Vec::new(),
    )
    .expect("raw series envelope is structurally valid")
}

fn actual_series_apply(
    registry: &mut SeriesRegistry,
    request: series_oracle::RawRequest,
) -> Result<series_oracle::RawOutcome, RawError> {
    use series_oracle::{RawOutcome, RawRequest};
    match request {
        RawRequest::Register {
            series,
            binding,
            payload,
            evidence,
        } => registry
            .register(
                actual_series_id(series),
                actual_series_binding(binding),
                actual_series_payload(payload),
                actual_declaration_evidence(evidence),
            )
            .map(|declaration| RawOutcome::Declaration(declaration.revision().get())),
        RawRequest::Revise {
            series,
            expected,
            payload,
            evidence,
        } => registry
            .revise(
                actual_series_id(series),
                DeclarationRevision::new(expected).expect("nonzero raw expected revision"),
                actual_series_payload(payload),
                actual_declaration_evidence(evidence),
            )
            .map(|declaration| RawOutcome::Declaration(declaration.revision().get())),
        RawRequest::Retire {
            series,
            expected,
            evidence,
        } => registry
            .retire(
                actual_series_id(series),
                DeclarationRevision::new(expected).expect("nonzero raw expected revision"),
                actual_declaration_evidence(evidence),
            )
            .map(|retirement| RawOutcome::Retirement(retirement.declaration_revision().get())),
        RawRequest::Bind {
            series,
            producer,
            mode,
            value,
        } => registry
            .bind(actual_series_envelope(series, producer, mode, value))
            .map(|bound| RawOutcome::Binding(bound.declaration().revision().get())),
    }
    .map_err(error_code)
}

fn raw_reference_code(reference: &DeclarationReference, prefix: &str) -> u8 {
    reference
        .as_str()
        .strip_prefix(prefix)
        .expect("actual adapter retains the raw prefix")
        .parse()
        .expect("actual adapter retains a raw numeric code")
}

const fn raw_mode_code(mode: CollectionMode) -> u8 {
    match mode {
        CollectionMode::Sampled => 1,
        CollectionMode::ChangeOnly => 2,
        CollectionMode::Event => 3,
        CollectionMode::Cumulative | CollectionMode::Interval => {
            panic!("actual adapter only emits scripted modes")
        }
    }
}

const fn raw_family_code(family: ValueFamily) -> u8 {
    match family {
        ValueFamily::Boolean => 1,
        ValueFamily::Signed => 2,
        ValueFamily::Text => 3,
        ValueFamily::Real | ValueFamily::Unsigned | ValueFamily::State | ValueFamily::Artifact => {
            panic!("actual adapter only emits scripted families")
        }
    }
}

fn actual_series_snapshot(registry: &SeriesRegistry) -> series_oracle::RawSnapshot {
    let snapshot = registry.snapshot();
    series_oracle::RawSnapshot {
        revision_count: snapshot.declaration_revision_count(),
        series: snapshot
            .series()
            .iter()
            .map(|history| series_oracle::RawHistorySnapshot {
                series: series_number(history.series_id()),
                binding: raw_reference_code(history.binding().source().locator(), "binding-"),
                revisions: history
                    .declarations()
                    .iter()
                    .map(|declaration| {
                        let payload = declaration.payload();
                        (
                            declaration.revision().get(),
                            declaration
                                .previous_revision()
                                .map(DeclarationRevision::get),
                            series_oracle::RawPayload::new(
                                producer_number(payload.producer_id()),
                                raw_mode_code(payload.collection_mode()),
                                raw_family_code(payload.value_family()),
                                raw_reference_code(
                                    payload
                                        .application()
                                        .expect("scripted application reference"),
                                    "metadata-",
                                ),
                            ),
                            u8::try_from(declaration.evidence().effective_at().unix_seconds())
                                .expect("scripted evidence code"),
                        )
                    })
                    .collect(),
                retirement: history.retirement().map(|retirement| {
                    (
                        retirement.declaration_revision().get(),
                        u8::try_from(retirement.evidence().effective_at().unix_seconds())
                            .expect("scripted retirement code"),
                    )
                }),
            })
            .collect(),
    }
}

fn actual_series_parts() -> (
    SeriesId,
    SeriesBinding,
    SeriesDeclarationPayload,
    DeclarationEvidence,
) {
    let series_id = actual_series_id(1);
    let binding = actual_series_binding(1);
    let payload = actual_series_payload(series_oracle::RawPayload::new(10, 1, 1, 1));
    let evidence = actual_declaration_evidence(1);
    (series_id, binding, payload, evidence)
}

fn actual_series_constructor_capacity_errors() -> Vec<ModelError> {
    let (series_id, binding, payload, evidence) = actual_series_parts();
    let mut errors = vec![
        DeclarationReference::new(format!("{}\n", fixtures::SECRET_SENTINEL))
            .expect_err("control character is invalid"),
        DeclarationRevision::new(0).expect_err("revision zero is invalid"),
    ];

    let mut zero_series = SeriesRegistry::new(actual_store(), SeriesRegistryLimits::new(0, 1));
    errors.push(
        zero_series
            .register(
                series_id,
                binding.clone(),
                payload.clone(),
                evidence.clone(),
            )
            .expect_err("zero series capacity"),
    );
    let mut zero_revisions = SeriesRegistry::new(actual_store(), SeriesRegistryLimits::new(1, 0));
    errors.push(
        zero_revisions
            .register(
                series_id,
                binding.clone(),
                payload.clone(),
                evidence.clone(),
            )
            .expect_err("zero revision capacity"),
    );
    errors
}

fn actual_series_registration_errors() -> Vec<ModelError> {
    let (series_id, binding, payload, evidence) = actual_series_parts();
    let mut already = SeriesRegistry::new(actual_store(), SeriesRegistryLimits::new(1, 1));
    already
        .register(
            series_id,
            binding.clone(),
            payload.clone(),
            evidence.clone(),
        )
        .expect("initial declaration");
    let already_error = already
        .register(
            series_id,
            actual_series_binding(2),
            payload.clone(),
            evidence.clone(),
        )
        .expect_err("different repeated registration");

    let mut missing = SeriesRegistry::new(actual_store(), SeriesRegistryLimits::new(1, 2));
    let missing_error = missing
        .revise(
            series_id,
            DeclarationRevision::FIRST,
            payload.clone(),
            evidence.clone(),
        )
        .expect_err("missing series");

    let mut retired = SeriesRegistry::new(actual_store(), SeriesRegistryLimits::new(1, 1));
    retired
        .register(
            series_id,
            binding.clone(),
            payload.clone(),
            evidence.clone(),
        )
        .expect("initial declaration");
    retired
        .retire(
            series_id,
            DeclarationRevision::FIRST,
            actual_declaration_evidence(2),
        )
        .expect("retirement");
    let retired_error = retired
        .bind(actual_series_envelope(
            1,
            10,
            1,
            series_oracle::RawValue::Family(1),
        ))
        .expect_err("retired bind");
    vec![already_error, missing_error, retired_error]
}

fn actual_series_revision_errors() -> Vec<ModelError> {
    let (series_id, binding, payload, evidence) = actual_series_parts();
    let mut stale = SeriesRegistry::new(actual_store(), SeriesRegistryLimits::new(1, 2));
    stale
        .register(
            series_id,
            binding.clone(),
            payload.clone(),
            evidence.clone(),
        )
        .expect("initial declaration");
    let stale_error = stale
        .revise(
            series_id,
            DeclarationRevision::new(2).expect("valid comparison revision"),
            actual_series_payload(series_oracle::RawPayload::new(11, 2, 2, 2)),
            actual_declaration_evidence(2),
        )
        .expect_err("stale revision");

    let mut unchanged = SeriesRegistry::new(actual_store(), SeriesRegistryLimits::new(1, 2));
    unchanged
        .register(
            series_id,
            binding.clone(),
            payload.clone(),
            evidence.clone(),
        )
        .expect("initial declaration");
    let unchanged_error = unchanged
        .revise(
            series_id,
            DeclarationRevision::FIRST,
            payload.clone(),
            actual_declaration_evidence(2),
        )
        .expect_err("unchanged payload");
    vec![stale_error, unchanged_error]
}

fn actual_series_binding_errors() -> Vec<ModelError> {
    let (series_id, binding, payload, evidence) = actual_series_parts();
    let mut bound = SeriesRegistry::new(actual_store(), SeriesRegistryLimits::new(1, 1));
    bound
        .register(series_id, binding, payload, evidence)
        .expect("initial declaration");
    let metadata_error = bound
        .bind(actual_series_envelope(
            1,
            11,
            1,
            series_oracle::RawValue::Family(1),
        ))
        .expect_err("metadata mismatch");
    let family_error = bound
        .bind(actual_series_envelope(
            1,
            10,
            1,
            series_oracle::RawValue::Family(2),
        ))
        .expect_err("value-family mismatch");
    vec![metadata_error, family_error]
}

fn actual_series_model_errors() -> Vec<ModelError> {
    let mut errors = actual_series_constructor_capacity_errors();
    errors.extend(actual_series_registration_errors());
    errors.extend(actual_series_revision_errors());
    errors.extend(actual_series_binding_errors());
    errors
}

fn actual_content(raw: &fixtures::RawContent) -> ContentIdentity {
    ContentIdentity::new(
        ContentFormat::new(raw.format.clone()).expect("valid raw content format"),
        ContentVersion::new(raw.version),
        raw.digest,
    )
}

fn actual_retry(raw: &RawRetry) -> RetryQualification {
    RetryQualification::new(
        SeriesId::from_bytes(raw.series).expect("valid raw retry series UUIDv7"),
        ProducerId::from_bytes(raw.producer).expect("valid raw retry producer UUIDv7"),
        RetryKey::new(raw.key.clone()).expect("valid raw retry key"),
        actual_content(&raw.content),
    )
}

const fn actual_retry_class(classification: RetryClassification) -> oracle::RetryClass {
    match classification {
        RetryClassification::Equivalent => oracle::RetryClass::Equivalent,
        RetryClassification::Conflict => oracle::RetryClass::Conflict,
        RetryClassification::Distinct => oracle::RetryClass::Distinct,
    }
}

const fn quality_level_name(level: QualityLevel) -> &'static str {
    match level {
        QualityLevel::Unknown => "unknown",
        QualityLevel::Good => "good",
        QualityLevel::Uncertain => "uncertain",
        QualityLevel::Bad => "bad",
        QualityLevel::NotEvaluated => "not-evaluated",
    }
}

const fn flag_values(flags: QualityFlags) -> [bool; 6] {
    [
        flags.stale(),
        flags.invalid(),
        flags.substituted(),
        flags.overridden(),
        flags.out_of_service(),
        flags.communication_failure(),
    ]
}

#[test]
fn golden_ledger_is_fresh_ascii_and_independent() {
    assert!(!FIXTURE_SOURCE.contains("och_core"));
    assert!(!ORACLE_SOURCE.contains("och_core"));
    assert!(!SERIES_ORACLE_SOURCE.contains("och_core"));
    let checked_in = GOLDEN.replace("\r\n", "\n");
    assert!(checked_in.is_ascii());
    assert!(!checked_in.contains('\r'));
    assert!(!checked_in.contains(fixtures::SECRET_SENTINEL));
    assert_eq!(
        checked_in.lines().next(),
        Some("och-core-m00-pr03-evidence|schema=1|scope=test-only|encoding=ascii|newline=lf")
    );
    assert!(checked_in.lines().any(|line| {
        line == "authority|test-only=true|wire=false|persistence=false|api-compatibility=false"
    }));
    assert_eq!(
        checked_in
            .lines()
            .filter(|line| line.starts_with("case|"))
            .count(),
        22
    );
    assert_eq!(checked_in, oracle::render_ledger());
}

#[test]
fn series_lifecycle_matches_the_independent_bounded_primitive_oracle() {
    let mut expected = series_oracle::RawRegistry::new(2, 3);
    let mut actual = SeriesRegistry::new(actual_store(), SeriesRegistryLimits::new(2, 3));
    for request in series_oracle::lifecycle_script() {
        let before_expected = expected.clone();
        let before_actual = actual.snapshot();
        let expected_result = expected.apply(request);
        let actual_result = actual_series_apply(&mut actual, request);
        assert_eq!(actual_result, expected_result, "{request:?}");
        assert_eq!(actual_series_snapshot(&actual), expected.snapshot());
        if expected_result.is_err() {
            assert_eq!(expected, before_expected);
            assert_eq!(actual.snapshot(), before_actual);
        }
    }
}

#[test]
fn identities_match_independent_uuid_text_and_byte_facts() {
    let store_bytes = oracle::parse_uuid_v7(fixtures::STORE_TEXT).expect("oracle store UUIDv7");
    let series_bytes = oracle::parse_uuid_v7(fixtures::SERIES_TEXT).expect("oracle series UUIDv7");
    let producer_bytes =
        oracle::parse_uuid_v7(fixtures::PRODUCER_TEXT).expect("oracle producer UUIDv7");
    let observation_bytes =
        oracle::parse_uuid_v7(fixtures::OBSERVATION_TEXT).expect("oracle observation UUIDv7");
    let artifact_bytes =
        oracle::parse_uuid_v7(fixtures::ARTIFACT_TEXT).expect("oracle artifact UUIDv7");

    let store = StoreId::parse(fixtures::STORE_TEXT).expect("actual store UUIDv7");
    let series = SeriesId::parse(fixtures::SERIES_TEXT).expect("actual series UUIDv7");
    let producer = ProducerId::parse(fixtures::PRODUCER_TEXT).expect("actual producer UUIDv7");
    let observation =
        ObservationId::parse(fixtures::OBSERVATION_TEXT).expect("actual observation UUIDv7");
    let artifact = ArtifactId::parse(fixtures::ARTIFACT_TEXT).expect("actual artifact UUIDv7");
    assert_eq!(store.into_bytes(), store_bytes);
    assert_eq!(series.into_bytes(), series_bytes);
    assert_eq!(producer.into_bytes(), producer_bytes);
    assert_eq!(observation.into_bytes(), observation_bytes);
    assert_eq!(artifact.into_bytes(), artifact_bytes);
    assert_eq!(store.to_string(), oracle::render_uuid(store_bytes));
    assert_eq!(series.to_string(), oracle::render_uuid(series_bytes));
    assert_eq!(producer.to_string(), oracle::render_uuid(producer_bytes));
    assert_eq!(
        observation.to_string(),
        oracle::render_uuid(observation_bytes)
    );
    assert_eq!(artifact.to_string(), oracle::render_uuid(artifact_bytes));
    assert_eq!(
        SeriesId::from_bytes(series_bytes).expect("actual valid bytes"),
        series
    );

    for noncanonical in [
        "01941F29-7C00-7000-8000-000000000001",
        "01941f297c0070008000000000000001",
        "01941f29-7c00-4000-8000-000000000001",
        "01941f29-7c00-7000-4000-000000000001",
    ] {
        assert_eq!(oracle::parse_uuid_v7(noncanonical), None);
        assert_eq!(
            SeriesId::parse(noncanonical),
            Err(ModelError::InvalidIdentity)
        );
    }

    let mut wrong_version = series_bytes;
    wrong_version[6] = 0x60;
    assert!(!oracle::valid_uuid_v7_bytes(wrong_version));
    assert_eq!(
        SeriesId::from_bytes(wrong_version),
        Err(ModelError::InvalidIdentity)
    );
    let mut wrong_variant = series_bytes;
    wrong_variant[8] = 0x40;
    assert!(!oracle::valid_uuid_v7_bytes(wrong_variant));
    assert_eq!(
        SeriesId::from_bytes(wrong_variant),
        Err(ModelError::InvalidIdentity)
    );

    assert!(IDENTITY_SOURCE.contains("```compile_fail"));
    assert!(IDENTITY_SOURCE.contains("let _producer: ProducerId = series;"));
}

#[test]
fn exact_values_and_bounded_primitives_match_raw_facts() {
    assert_real_and_integer_facts();

    let maximum_text = "🦀".repeat(4_096);
    assert!(oracle::valid_exact_text(""));
    assert!(oracle::valid_exact_text(&maximum_text));
    assert!(!oracle::valid_exact_text(&"🦀".repeat(4_097)));
    let text = ExactText::new(maximum_text.clone()).expect("actual maximum exact text");
    assert_eq!(text.as_str(), maximum_text);
    assert_eq!(
        ExactText::new("🦀".repeat(4_097)),
        Err(ModelError::InvalidExactText)
    );
    assert_ne!(
        ExactText::new("e\u{301}".to_owned()).expect("decomposed exact text"),
        ExactText::new("é".to_owned()).expect("composed exact text")
    );

    let maximum_token = "x".repeat(256);
    assert!(oracle::valid_portable_token(&maximum_token));
    assert!(!oracle::valid_portable_token(""));
    assert!(!oracle::valid_portable_token(&"x".repeat(257)));
    let class = StateClass::new(maximum_token.clone()).expect("actual maximum state class");
    let member = StateMember::new("member".to_owned()).expect("actual state member");
    let state = StateValue::new(class, member);
    assert_eq!(state.class().as_str(), maximum_token);
    assert_eq!(state.member().as_str(), "member");
    assert_eq!(
        StateClass::new("x".repeat(257)),
        Err(ModelError::InvalidPortableToken)
    );

    assert_state_unavailable_and_content_facts(state, text);
}

fn assert_real_and_integer_facts() {
    let raw_bits = [
        0x8000_0000_0000_0000,
        0,
        0x7ff8_0000_0000_0002,
        0x7ff8_0000_0000_0001,
    ];
    let mut actual_bits: Vec<RealBits> = raw_bits.into_iter().map(RealBits::from_bits).collect();
    actual_bits.sort_unstable();
    assert_eq!(
        actual_bits
            .iter()
            .copied()
            .map(RealBits::to_bits)
            .collect::<Vec<_>>(),
        oracle::sorted_real_bits(&raw_bits)
    );
    assert_ne!(actual_bits[1], actual_bits[2]);
    assert!(actual_bits[1].to_f64().is_nan());
    assert!(actual_bits[2].to_f64().is_nan());
    assert_ne!(RealBits::from_f64(0.0), RealBits::from_f64(-0.0));

    for signed in [i64::MIN, i64::MAX] {
        let ExactValue::Signed(actual) = ExactValue::Signed(signed) else {
            unreachable!("constructed signed exact value")
        };
        assert_eq!(actual, signed);
    }
    for unsigned in [0, u64::MAX] {
        let ExactValue::Unsigned(actual) = ExactValue::Unsigned(unsigned) else {
            unreachable!("constructed unsigned exact value")
        };
        assert_eq!(actual, unsigned);
    }
}

fn assert_state_unavailable_and_content_facts(state: StateValue, text: ExactText) {
    let reason = UnavailableReason::new("external reason".to_owned()).expect("actual reason");
    let unavailable = Unavailable::new(Some(reason));
    assert_eq!(
        unavailable.reason().map(UnavailableReason::as_str),
        Some("external reason")
    );
    assert_eq!(Unavailable::without_reason().reason(), None);

    let maximum_format = "a".repeat(64);
    assert!(oracle::valid_content_format(&maximum_format));
    assert!(!oracle::valid_content_format("Text/plain"));
    assert_eq!(
        ContentFormat::new(maximum_format.clone())
            .expect("actual maximum content format")
            .as_str(),
        maximum_format
    );
    assert_eq!(
        ContentFormat::new("a".repeat(65)),
        Err(ModelError::InvalidContentFormat)
    );
    assert!(oracle::valid_retry_key(&"k".repeat(128)));
    assert!(!oracle::valid_retry_key(&"k".repeat(129)));
    assert_eq!(
        RetryKey::new("k".repeat(128))
            .expect("actual maximum retry key")
            .as_str()
            .len(),
        128
    );
    assert_eq!(
        RetryKey::new("k".repeat(129)),
        Err(ModelError::InvalidRetryKey)
    );

    let digest = core::array::from_fn(|index| u8::try_from(index).expect("digest index under 32"));
    let maximum_version_text = u128::MAX.to_string();
    assert_eq!(
        oracle::parse_canonical_decimal(&maximum_version_text),
        Some(u128::MAX)
    );
    let content = ContentIdentity::new(
        ContentFormat::new("application/octet-stream".to_owned()).expect("actual format"),
        ContentVersion::parse(&maximum_version_text).expect("actual maximum content version"),
        digest,
    );
    assert_eq!(content.format().as_str(), "application/octet-stream");
    assert_eq!(content.version().get(), u128::MAX);
    assert_eq!(content.sha256(), &digest);
    let artifact = ArtifactReference::new(
        ArtifactId::from_bytes(fixtures::uuid_bytes(48)).expect("actual artifact UUIDv7"),
        content,
    );
    assert_eq!(
        artifact.artifact_id().into_bytes(),
        fixtures::uuid_bytes(48)
    );
    assert_eq!(artifact.content().sha256(), &digest);

    let variants = [
        ExactValue::Real(RealBits::from_bits(1)),
        ExactValue::Signed(i64::MIN),
        ExactValue::Unsigned(u64::MAX),
        ExactValue::Boolean(false),
        ExactValue::State(state),
        ExactValue::Text(text),
        ExactValue::Artifact(artifact),
        ExactValue::Unavailable(unavailable),
    ];
    assert_eq!(variants.len(), 8);
}

#[test]
fn time_quality_status_and_producer_order_match_primitive_oracles() {
    for milliseconds in [
        i64::MIN,
        -1_001,
        -1_000,
        -999,
        -1,
        0,
        1,
        999,
        1_000,
        1_001,
        i64::MAX,
    ] {
        let expected = oracle::milliseconds_to_timestamp(milliseconds);
        let actual = Timestamp::from_unix_milliseconds(milliseconds);
        assert_eq!(actual.unix_seconds(), expected.seconds);
        assert_eq!(actual.nanosecond(), expected.nanoseconds);
        assert_eq!(
            actual
                .to_unix_milliseconds()
                .expect("millisecond fixture remains exact"),
            oracle::timestamp_to_milliseconds(expected)
                .expect("oracle millisecond fixture remains exact")
        );
    }

    let source = Timestamp::new(30, 0).expect("source");
    let receive = Timestamp::new(10, 0).expect("receive");
    let effective = Timestamp::new(-10, 0).expect("effective");
    let times = ObservationTimes::new(Some(source), receive, effective);
    assert_eq!(times.source(), Some(source));
    assert_eq!(times.receive(), receive);
    assert_eq!(times.effective(), effective);

    let levels = [
        (QualityLevel::Unknown, "unknown"),
        (QualityLevel::Good, "good"),
        (QualityLevel::Uncertain, "uncertain"),
        (QualityLevel::Bad, "bad"),
        (QualityLevel::NotEvaluated, "not-evaluated"),
    ];
    for (level, expected_name) in levels {
        let quality = Quality::new(level, QualityFlags::none());
        assert_eq!(quality_level_name(quality.level()), expected_name);
    }

    let independent_flags = [
        QualityFlags::none().with_stale(true),
        QualityFlags::none().with_invalid(true),
        QualityFlags::none().with_substituted(true),
        QualityFlags::none().with_overridden(true),
        QualityFlags::none().with_out_of_service(true),
        QualityFlags::none().with_communication_failure(true),
    ];
    for (index, flags) in independent_flags.into_iter().enumerate() {
        let mut expected = [false; 6];
        expected[index] = true;
        assert_eq!(flag_values(flags), expected);
        assert_eq!(
            Quality::new(QualityLevel::NotEvaluated, flags).flags(),
            flags
        );
    }

    assert!(NativeStatus::absent().is_absent());
    let repeated = NativeStatusToken::new("vendor-opaque".to_owned()).expect("actual token");
    let status = NativeStatus::new(vec![repeated.clone(), repeated.clone()])
        .expect("actual repeated status");
    assert_eq!(status.tokens(), &[repeated.clone(), repeated.clone()]);
    let maximum_status = NativeStatus::new(vec![repeated; 16]).expect("actual maximum status");
    assert_eq!(maximum_status.tokens().len(), 16);
    let maximum_token = NativeStatusToken::new("s".repeat(256)).expect("maximum status token");
    assert_eq!(maximum_token.as_str().len(), 256);
    assert_eq!(
        NativeStatusToken::new("s".repeat(257)),
        Err(ModelError::InvalidPortableToken)
    );

    assert_producer_number_and_position_facts();
}

fn assert_producer_number_and_position_facts() {
    let maximum = u128::MAX.to_string();
    assert_eq!(oracle::parse_canonical_decimal(&maximum), Some(u128::MAX));
    assert_eq!(
        ProducerEpoch::parse(&maximum).expect("epoch").get(),
        u128::MAX
    );
    assert_eq!(
        ProducerSequence::parse(&maximum).expect("sequence").get(),
        u128::MAX
    );
    assert_eq!(
        ContentVersion::parse(&maximum).expect("version").get(),
        u128::MAX
    );
    for invalid in ["", "+1", "-1", " 1", "1 ", "01", "1_000"] {
        assert_eq!(oracle::parse_canonical_decimal(invalid), None);
        assert_eq!(
            ProducerEpoch::parse(invalid),
            Err(ModelError::InvalidCanonicalDecimal)
        );
    }

    let first_raw = fixtures::RawPosition::new(1, u128::MAX);
    let second_raw = fixtures::RawPosition::new(2, 0);
    assert_eq!(
        oracle::position_cmp(first_raw, second_raw),
        core::cmp::Ordering::Less
    );
    let first = ProducerPosition::new(
        ProducerEpoch::new(first_raw.epoch),
        ProducerSequence::new(first_raw.sequence),
    );
    let second = ProducerPosition::new(
        ProducerEpoch::new(second_raw.epoch),
        ProducerSequence::new(second_raw.sequence),
    );
    assert!(first < second);
    assert_eq!(first.epoch().get(), 1);
    assert_eq!(first.sequence().get(), u128::MAX);
}

#[test]
fn raw_order_uses_only_effective_receive_and_identity() {
    let raw = fixtures::raw_order_observations();
    let expected_ids = oracle::raw_order_ids(&raw);
    let mut actual_keys = raw
        .iter()
        .map(|spec| {
            let key = actual_observation(spec).raw_order_key();
            assert_eq!(key.effective().unix_seconds(), spec.effective.seconds);
            assert_eq!(key.receive().unix_seconds(), spec.receive.seconds);
            assert_eq!(key.observation_id().into_bytes(), spec.id);
            (key, spec.id)
        })
        .collect::<Vec<_>>();
    actual_keys.sort_by_key(|(key, _)| *key);
    let actual_ids = actual_keys
        .into_iter()
        .map(|(_, identity)| identity)
        .collect::<Vec<_>>();
    assert_eq!(actual_ids, expected_ids);

    let excluded = fixtures::raw_order_exclusion_pair();
    assert_ne!(excluded[0].source, excluded[1].source);
    assert_ne!(excluded[0].position, excluded[1].position);
    assert_eq!(
        oracle::raw_order_key(&excluded[0]),
        oracle::raw_order_key(&excluded[1])
    );
    assert_eq!(
        actual_observation(&excluded[0]).raw_order_key(),
        actual_observation(&excluded[1]).raw_order_key()
    );
}

#[test]
fn collection_modes_shapes_bounds_and_half_open_endpoints_match_oracle() {
    for mode in fixtures::ALL_MODES {
        let raw = fixtures::valid_observation_only(mode);
        assert!(oracle::envelope_violations(&raw).is_empty());
        let actual = actual_envelope(&raw).expect("actual valid observation-only evidence");
        assert_eq!(actual.evidence_kind(), EvidenceKind::Observed);
        assert_eq!(actual.observations().len(), 1);
        assert!(actual.gaps().is_empty());
        assert_eq!(actual.series().collection_mode(), actual_mode(mode));
        assert_eq!(
            actual.series().series_id().into_bytes(),
            fixtures::uuid_bytes(1)
        );
        assert_eq!(
            actual.series().producer_id().into_bytes(),
            fixtures::uuid_bytes(16)
        );
        assert_eq!(
            actual.observations()[0].interval().is_some(),
            mode == RawMode::Interval
        );
    }

    let gap_only = fixtures::valid_gap_only();
    assert!(oracle::envelope_violations(&gap_only).is_empty());
    let actual_gap_only = actual_envelope(&gap_only).expect("actual valid gap-only evidence");
    assert!(actual_gap_only.observations().is_empty());
    assert_eq!(actual_gap_only.gaps().len(), 1);

    let mixed = fixtures::valid_mixed();
    assert!(oracle::envelope_violations(&mixed).is_empty());
    let actual_mixed = actual_envelope(&mixed).expect("actual valid mixed evidence");
    assert_eq!(actual_mixed.observations().len(), 1);
    assert_eq!(actual_mixed.gaps().len(), 1);

    let no_change = fixtures::valid_no_change();
    assert!(oracle::envelope_violations(&no_change).is_empty());
    let actual_no_change = actual_envelope(&no_change).expect("actual valid no-change evidence");
    assert_eq!(actual_no_change.evidence_kind(), EvidenceKind::NoChange);
    assert!(actual_no_change.observations().is_empty());
    assert!(actual_no_change.gaps().is_empty());
    let no_change_interval = actual_no_change
        .no_change_evidence()
        .expect("actual no-change accessor")
        .interval();
    assert_eq!(no_change_interval.start().unix_seconds(), -2);
    assert_eq!(no_change_interval.end().unix_seconds(), -1);
    assert!(no_change_interval.contains(Timestamp::new(-2, 0).expect("interval start")));
    assert!(!no_change_interval.contains(Timestamp::new(-1, 0).expect("interval end")));

    let gap = actual_gap(RawGap::new(7, 4, 5), GapReason::Unknown).expect("actual half-open gap");
    assert_eq!(gap.epoch().get(), 7);
    assert_eq!(gap.start().get(), 4);
    assert_eq!(gap.end().get(), 5);
    assert!(gap.contains(ProducerEpoch::new(7), ProducerSequence::new(4)));
    assert!(!gap.contains(ProducerEpoch::new(7), ProducerSequence::new(5)));
    assert!(!gap.contains(ProducerEpoch::new(8), ProducerSequence::new(4)));
    for reason in [
        GapReason::Unknown,
        GapReason::ProducerRestart,
        GapReason::BufferOverflow,
        GapReason::CommunicationFailure,
        GapReason::SourceDataLoss,
        GapReason::AdministrativeExclusion,
    ] {
        assert_eq!(
            actual_gap(RawGap::new(1, 1, 2), reason)
                .expect("actual valid reason")
                .reason(),
            reason
        );
    }

    let maxima = fixtures::valid_maxima();
    assert!(oracle::envelope_violations(&maxima).is_empty());
    let actual_maxima = actual_envelope(&maxima).expect("actual exact collection maxima");
    assert_eq!(actual_maxima.observations().len(), 256);
    assert_eq!(actual_maxima.gaps().len(), 64);
    assert_eq!(
        actual_maxima.observations()[0]
            .observation_id()
            .into_bytes(),
        fixtures::uuid_bytes(1)
    );
    assert_eq!(
        actual_maxima.observations()[255]
            .observation_id()
            .into_bytes(),
        fixtures::uuid_bytes(256)
    );
}

#[test]
fn every_negative_fixture_has_one_oracle_violation_and_one_sanitized_model_error() {
    let mut covered = BTreeSet::new();
    for fixture in fixtures::constructor_failures() {
        let violations = oracle::constructor_violations(&fixture.case);
        assert_eq!(violations, vec![fixture.expected], "{}", fixture.stable_id);
        let actual = actual_constructor(&fixture.case).expect_err(fixture.stable_id);
        assert_eq!(
            error_code(actual),
            fixture.expected,
            "{}",
            fixture.stable_id
        );
        assert!(!actual.to_string().contains(fixtures::SECRET_SENTINEL));
        assert!(!format!("{actual:?}").contains(fixtures::SECRET_SENTINEL));
        assert!(covered.insert(fixture.expected));
    }

    for fixture in fixtures::negative_envelopes() {
        let violations = oracle::envelope_violations(&fixture.envelope);
        assert_eq!(violations, vec![fixture.expected], "{}", fixture.stable_id);
        let actual = actual_envelope(&fixture.envelope).expect_err(fixture.stable_id);
        assert_eq!(
            error_code(actual),
            fixture.expected,
            "{}",
            fixture.stable_id
        );
        assert!(covered.insert(fixture.expected));
    }

    let series_errors = actual_series_model_errors();
    assert_eq!(
        series_errors.len(),
        series_oracle::SERIES_ERROR_INVENTORY.len()
    );
    for (actual, expected) in series_errors
        .into_iter()
        .zip(series_oracle::SERIES_ERROR_INVENTORY)
    {
        assert_eq!(error_code(actual), expected);
        assert!(!actual.to_string().contains(fixtures::SECRET_SENTINEL));
        assert!(!format!("{actual:?}").contains(fixtures::SECRET_SENTINEL));
        assert!(covered.insert(expected));
    }

    assert_eq!(covered.len(), fixtures::ALL_ERROR_CODES.len());
    assert_eq!(
        covered,
        fixtures::ALL_ERROR_CODES
            .into_iter()
            .collect::<BTreeSet<_>>()
    );
}

#[test]
fn retry_matrix_and_debug_redaction_match_raw_scope_key_content_rules() {
    let base_raw = fixtures::retry_base();
    let base = actual_retry(&base_raw);
    let equivalent = actual_retry(&base_raw);
    assert_eq!(
        actual_retry_class(base.classify(&equivalent)),
        oracle::classify_retry(&base_raw, &base_raw)
    );
    assert_eq!(base.series_id().into_bytes(), base_raw.series);
    assert_eq!(base.producer_id().into_bytes(), base_raw.producer);
    assert_eq!(base.key().as_str(), base_raw.key);
    assert_eq!(base.content().format().as_str(), base_raw.content.format);
    assert_eq!(base.content().version().get(), base_raw.content.version);
    assert_eq!(base.content().sha256(), &base_raw.content.digest);

    for (stable_id, conflict_raw) in fixtures::retry_conflicts() {
        assert_eq!(
            oracle::classify_retry(&base_raw, &conflict_raw),
            oracle::RetryClass::Conflict,
            "{stable_id}"
        );
        assert_eq!(
            actual_retry_class(base.classify(&actual_retry(&conflict_raw))),
            oracle::RetryClass::Conflict,
            "{stable_id}"
        );
    }
    for (stable_id, distinct_raw) in fixtures::retry_distinct() {
        assert_eq!(
            oracle::classify_retry(&base_raw, &distinct_raw),
            oracle::RetryClass::Distinct,
            "{stable_id}"
        );
        assert_eq!(
            actual_retry_class(base.classify(&actual_retry(&distinct_raw))),
            oracle::RetryClass::Distinct,
            "{stable_id}"
        );
    }

    let key_debug = format!("{:?}", base.key());
    let qualification_debug = format!("{base:?}");
    assert!(!key_debug.contains(fixtures::SECRET_SENTINEL));
    assert!(!qualification_debug.contains(fixtures::SECRET_SENTINEL));
    assert!(key_debug.contains("[REDACTED]"));
    assert!(qualification_debug.contains("[REDACTED]"));
    assert!(!GOLDEN.contains(fixtures::SECRET_SENTINEL));
}
