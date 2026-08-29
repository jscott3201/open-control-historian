//! Public-contract tests for identity, bounded primitives, values, time, and quality.

use och_core::{
    ArtifactId, ArtifactReference, ContentFormat, ContentIdentity, ContentVersion, ExactText,
    ExactValue, ModelError, NativeStatus, NativeStatusToken, ObservationId, ObservationTimes,
    ProducerEpoch, ProducerId, ProducerPosition, ProducerSequence, Quality, QualityFlags,
    QualityLevel, RealBits, RetryKey, SeriesId, StateClass, StateMember, StateValue, StoreId,
    Timestamp, Unavailable, UnavailableReason,
};

const SERIES_TEXT: &str = "01941f29-7c00-7000-8000-000000000001";
const PRODUCER_TEXT: &str = "01941f29-7c00-7000-8000-000000000010";
const OBSERVATION_TEXT: &str = "01941f29-7c00-7000-8000-000000000020";
const ARTIFACT_TEXT: &str = "01941f29-7c00-7000-8000-000000000030";

#[test]
fn identity_requires_canonical_uuid_v7_text_and_round_trips_bytes() {
    let series = SeriesId::parse(SERIES_TEXT).expect("valid series identity");
    assert_eq!(series.to_string(), SERIES_TEXT);
    assert_eq!(
        SeriesId::from_bytes(*series.as_bytes()).expect("valid UUIDv7 bytes"),
        series
    );
    assert_eq!(series.into_bytes()[6] >> 4, 7);
    assert_eq!(series.into_bytes()[8] & 0b1100_0000, 0b1000_0000);

    let next =
        SeriesId::parse("01941f29-7c00-7000-8000-000000000002").expect("valid next identity");
    assert!(series < next);
}

#[test]
fn all_nominal_identity_families_parse_the_same_validated_shape() {
    let store = StoreId::parse(SERIES_TEXT).expect("store");
    let series = SeriesId::parse(SERIES_TEXT).expect("series");
    let producer = ProducerId::parse(SERIES_TEXT).expect("producer");
    let observation = ObservationId::parse(SERIES_TEXT).expect("observation");
    let artifact = ArtifactId::parse(SERIES_TEXT).expect("artifact");

    assert_eq!(store.to_string(), series.to_string());
    assert_eq!(series.to_string(), producer.to_string());
    assert_eq!(producer.to_string(), observation.to_string());
    assert_eq!(observation.to_string(), artifact.to_string());
}

#[test]
fn identity_rejects_noncanonical_text_version_and_variant() {
    for hostile in [
        "",
        "01941F29-7C00-7000-8000-000000000001",
        "01941f297c0070008000000000000001",
        "01941f29-7c00-4000-8000-000000000001",
        "01941f29-7c00-7000-0000-000000000001",
        "01941f29-7c00-7000-c000-000000000001",
        "01941f29-7c00-7000-8000-00000000000g",
        "01941f29-7c00-7000-8000-secret-secret",
    ] {
        let error = SeriesId::parse(hostile).expect_err("identity must be rejected");
        assert_eq!(error, ModelError::InvalidIdentity);
        if !hostile.is_empty() {
            assert!(!error.to_string().contains(hostile));
        }
    }

    let mut wrong_version = SeriesId::parse(SERIES_TEXT).expect("identity").into_bytes();
    wrong_version[6] = 0x60;
    assert_eq!(
        SeriesId::from_bytes(wrong_version),
        Err(ModelError::InvalidIdentity)
    );

    let mut wrong_variant = SeriesId::parse(SERIES_TEXT).expect("identity").into_bytes();
    wrong_variant[8] = 0x40;
    assert_eq!(
        SeriesId::from_bytes(wrong_variant),
        Err(ModelError::InvalidIdentity)
    );
}

#[test]
fn timestamp_normalizes_every_signed_millisecond_exactly() {
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
        let timestamp = Timestamp::from_unix_milliseconds(milliseconds);
        assert_eq!(
            timestamp
                .to_unix_milliseconds()
                .expect("millisecond input remains exact"),
            milliseconds
        );
    }

    let negative_millisecond = Timestamp::from_unix_milliseconds(-1);
    assert_eq!(negative_millisecond.unix_seconds(), -1);
    assert_eq!(negative_millisecond.nanosecond(), 999_000_000);
}

#[test]
fn timestamp_checks_normalization_precision_and_overflow() {
    assert_eq!(
        Timestamp::new(0, 1_000_000_000),
        Err(ModelError::InvalidNanosecond)
    );
    let maximum_fraction = Timestamp::new(0, 999_999_999).expect("normalized maximum");
    assert_eq!(maximum_fraction.nanosecond(), 999_999_999);
    assert_eq!(
        Timestamp::new(0, 1)
            .expect("nanosecond timestamp")
            .to_unix_milliseconds(),
        Err(ModelError::InexactUnixMilliseconds)
    );
    assert_eq!(
        Timestamp::new(i64::MAX, 0)
            .expect("normalized timestamp")
            .to_unix_milliseconds(),
        Err(ModelError::UnixMillisecondsOverflow)
    );
    assert_eq!(
        Timestamp::new(i64::MIN, 0)
            .expect("normalized timestamp")
            .to_unix_milliseconds(),
        Err(ModelError::UnixMillisecondsOverflow)
    );
    assert!(
        Timestamp::new(-1, 999_999_999).expect("earlier") < Timestamp::new(0, 0).expect("epoch")
    );
}

#[test]
fn observation_times_do_not_impose_chronology() {
    let source = Timestamp::new(30, 0).expect("source");
    let receive = Timestamp::new(10, 0).expect("receive");
    let effective = Timestamp::new(-10, 0).expect("effective");
    let times = ObservationTimes::new(Some(source), receive, effective);
    assert_eq!(times.source(), Some(source));
    assert_eq!(times.receive(), receive);
    assert_eq!(times.effective(), effective);
}

#[test]
fn real_bits_preserve_nan_payloads_and_signed_zero() {
    let first_nan = RealBits::from_bits(0x7ff8_0000_0000_0001);
    let second_nan = RealBits::from_bits(0x7ff8_0000_0000_0002);
    assert!(first_nan.to_f64().is_nan());
    assert!(second_nan.to_f64().is_nan());
    assert_ne!(first_nan, second_nan);
    assert!(first_nan < second_nan);
    assert_eq!(RealBits::from_f64(first_nan.to_f64()), first_nan);

    let positive_zero = RealBits::from_f64(0.0);
    let negative_zero = RealBits::from_f64(-0.0);
    assert_ne!(positive_zero, negative_zero);
    assert_eq!(positive_zero.to_bits(), 0);
    assert_eq!(negative_zero.to_bits(), 1_u64 << 63);
}

#[test]
fn exact_values_cover_full_integer_boolean_and_real_ranges() {
    assert_eq!(ExactValue::Signed(i64::MIN), ExactValue::Signed(i64::MIN));
    assert_eq!(ExactValue::Signed(i64::MAX), ExactValue::Signed(i64::MAX));
    assert_eq!(
        ExactValue::Unsigned(u64::MAX),
        ExactValue::Unsigned(u64::MAX)
    );
    assert_eq!(ExactValue::Boolean(false), ExactValue::Boolean(false));
    assert_ne!(ExactValue::Boolean(false), ExactValue::Boolean(true));
    assert_eq!(
        ExactValue::Real(RealBits::from_f64(-0.0)),
        ExactValue::Real(RealBits::from_bits(1_u64 << 63))
    );
}

#[test]
fn exact_text_uses_unicode_scalar_bounds_without_normalization() {
    let at_limit = "🦀".repeat(4_096);
    let text = ExactText::new(at_limit.clone()).expect("exact scalar limit");
    assert_eq!(text.as_str(), at_limit);
    assert_eq!(text.into_string().chars().count(), 4_096);
    assert_eq!(
        ExactText::new("🦀".repeat(4_097)),
        Err(ModelError::InvalidExactText)
    );
    assert_eq!(
        ExactText::new(String::new())
            .expect("empty exact text")
            .as_str(),
        ""
    );

    let decomposed = ExactText::new("e\u{301}".to_owned()).expect("decomposed");
    let composed = ExactText::new("é".to_owned()).expect("composed");
    assert_ne!(decomposed, composed);
}

#[test]
fn state_and_opaque_reason_tokens_enforce_printable_ascii_bounds() {
    let maximum = "x".repeat(256);
    let class = StateClass::new(maximum.clone()).expect("class at limit");
    let member = StateMember::new("member unknown to core".to_owned()).expect("member");
    let state = StateValue::new(class, member);
    assert_eq!(state.class().as_str(), maximum);
    assert_eq!(state.member().as_str(), "member unknown to core");

    let reason = UnavailableReason::new("opaque external reason".to_owned()).expect("reason");
    assert_eq!(reason.as_str(), "opaque external reason");

    for invalid in [
        String::new(),
        "x".repeat(257),
        "line\nbreak".to_owned(),
        "é".to_owned(),
    ] {
        assert_eq!(
            StateClass::new(invalid),
            Err(ModelError::InvalidPortableToken)
        );
    }
}

#[test]
fn content_identity_and_artifact_reference_preserve_external_identity() {
    let format = ContentFormat::new("application/octet-stream".to_owned()).expect("format");
    let version = ContentVersion::parse(&u128::MAX.to_string()).expect("maximum version");
    let digest = [0xa5; 32];
    let content = ContentIdentity::new(format, version, digest);
    assert_eq!(content.format().as_str(), "application/octet-stream");
    assert_eq!(content.version().get(), u128::MAX);
    assert_eq!(content.sha256(), &digest);

    let artifact_id = ArtifactId::parse(ARTIFACT_TEXT).expect("artifact identity");
    let artifact = ArtifactReference::new(artifact_id, content.clone());
    assert_eq!(artifact.artifact_id(), artifact_id);
    assert_eq!(artifact.content(), &content);
    assert_eq!(
        ExactValue::Artifact(artifact.clone()),
        ExactValue::Artifact(artifact)
    );
}

#[test]
fn content_format_has_a_bounded_lowercase_token_grammar() {
    let maximum = "a".repeat(64);
    assert_eq!(
        ContentFormat::new(maximum.clone())
            .expect("format at limit")
            .as_str(),
        maximum
    );
    for invalid in [
        String::new(),
        "a".repeat(65),
        "Text/plain".to_owned(),
        "text plain".to_owned(),
        "téxt".to_owned(),
        "text\nplain".to_owned(),
    ] {
        assert_eq!(
            ContentFormat::new(invalid),
            Err(ModelError::InvalidContentFormat)
        );
    }
}

#[test]
fn unavailable_is_explicit_and_retains_an_optional_reason() {
    let reason = UnavailableReason::new("sensor offline".to_owned()).expect("reason");
    let unavailable = Unavailable::new(Some(reason.clone()));
    assert_eq!(unavailable.reason(), Some(&reason));
    assert_eq!(Unavailable::without_reason().reason(), None);
    assert_ne!(
        ExactValue::Unavailable(unavailable),
        ExactValue::Boolean(false)
    );
}

#[test]
fn quality_level_flags_and_native_status_remain_independent() {
    let flags = QualityFlags::none()
        .with_stale(true)
        .with_invalid(true)
        .with_substituted(true)
        .with_overridden(true)
        .with_out_of_service(true)
        .with_communication_failure(true);
    assert!(flags.stale());
    assert!(flags.invalid());
    assert!(flags.substituted());
    assert!(flags.overridden());
    assert!(flags.out_of_service());
    assert!(flags.communication_failure());

    for level in [
        QualityLevel::Unknown,
        QualityLevel::Good,
        QualityLevel::Uncertain,
        QualityLevel::Bad,
        QualityLevel::NotEvaluated,
    ] {
        let quality = Quality::new(level, flags);
        assert_eq!(quality.level(), level);
        assert_eq!(quality.flags(), flags);
    }

    let first = NativeStatusToken::new("vendor-new".to_owned()).expect("unknown token");
    let second = NativeStatusToken::new("vendor-new".to_owned()).expect("duplicate token");
    let status = NativeStatus::new(vec![first.clone(), second]).expect("ordered status");
    assert_eq!(status.tokens(), &[first.clone(), first]);
    assert!(!status.is_absent());
    assert!(NativeStatus::absent().is_absent());
    assert_eq!(
        Quality::new(QualityLevel::Good, flags).level(),
        QualityLevel::Good
    );
}

#[test]
fn native_status_checks_token_and_collection_bounds() {
    let token = NativeStatusToken::new("x".repeat(256)).expect("token at limit");
    assert_eq!(token.as_str().len(), 256);
    assert_eq!(
        NativeStatusToken::new("x".repeat(257)),
        Err(ModelError::InvalidPortableToken)
    );
    assert!(NativeStatus::new(vec![token.clone(); 16]).is_ok());
    assert_eq!(
        NativeStatus::new(vec![token; 17]),
        Err(ModelError::TooManyNativeStatusTokens)
    );
}

#[test]
fn canonical_decimal_types_cover_u128_and_reject_noncanonical_text() {
    let maximum = u128::MAX.to_string();
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
    assert_eq!(ProducerEpoch::parse("0").expect("zero").to_string(), "0");
    assert!(ProducerEpoch::parse("9").expect("nine") < ProducerEpoch::parse("10").expect("ten"));

    for invalid in [
        "",
        "+1",
        "-1",
        " 1",
        "1 ",
        "01",
        "00",
        "1_000",
        "340282366920938463463374607431768211456",
        "0000000000000000000000000000000000000000",
    ] {
        let error = ProducerSequence::parse(invalid).expect_err("decimal must be rejected");
        assert_eq!(error, ModelError::InvalidCanonicalDecimal);
        if !invalid.is_empty() {
            assert!(!error.to_string().contains(invalid));
        }
        assert_eq!(
            ContentVersion::parse(invalid),
            Err(ModelError::InvalidCanonicalDecimal)
        );
    }
}

#[test]
fn producer_position_orders_numerically_by_epoch_then_sequence() {
    let early = ProducerPosition::new(ProducerEpoch::new(1), ProducerSequence::new(u128::MAX));
    let later = ProducerPosition::new(ProducerEpoch::new(2), ProducerSequence::new(0));
    assert!(early < later);
    assert_eq!(early.epoch().get(), 1);
    assert_eq!(early.sequence().get(), u128::MAX);
}

#[test]
fn retry_key_enforces_exact_printable_ascii_bounds_and_redacts_debug() {
    let one = RetryKey::new("x".to_owned()).expect("one-byte key");
    assert_eq!(one.as_str(), "x");
    let maximum = RetryKey::new("k".repeat(128)).expect("key at limit");
    assert_eq!(maximum.as_str().len(), 128);
    assert_eq!(format!("{maximum:?}"), "RetryKey([REDACTED])");

    for invalid in [
        String::new(),
        "k".repeat(129),
        "a\nb".to_owned(),
        "clé".to_owned(),
    ] {
        let error = RetryKey::new(invalid.clone()).expect_err("key must be rejected");
        assert_eq!(error, ModelError::InvalidRetryKey);
        if !invalid.is_empty() {
            assert!(!error.to_string().contains(&invalid));
        }
    }
}

#[test]
fn nominal_test_constants_are_all_valid() {
    assert!(SeriesId::parse(SERIES_TEXT).is_ok());
    assert!(ProducerId::parse(PRODUCER_TEXT).is_ok());
    assert!(ObservationId::parse(OBSERVATION_TEXT).is_ok());
    assert!(ArtifactId::parse(ARTIFACT_TEXT).is_ok());
}
