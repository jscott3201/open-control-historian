//! Public-contract tests for modes, ordering, atomic collection evidence, and retry.

use och_core::collection::{MAX_GAPS, MAX_OBSERVATIONS};
use och_core::{
    CollectionEnvelope, CollectionMode, ContentFormat, ContentIdentity, ContentVersion,
    EvidenceKind, ExactValue, Gap, GapReason, ModelError, NativeStatus, NoChange, Observation,
    ObservationId, ObservationTimes, ProducerEpoch, ProducerId, ProducerPosition, ProducerSequence,
    Quality, QualityFlags, QualityLevel, RetryClassification, RetryKey, RetryQualification,
    SeriesId, SeriesMetadata, TimeInterval, Timestamp,
};

fn series(mode: CollectionMode) -> SeriesMetadata {
    SeriesMetadata::new(
        SeriesId::parse("01941f29-7c00-7000-8000-000000000001").expect("series identity"),
        ProducerId::parse("01941f29-7c00-7000-8000-000000000010").expect("producer identity"),
        mode,
    )
}

fn alternate_series_id() -> SeriesId {
    SeriesId::parse("01941f29-7c00-7000-8000-000000000002").expect("alternate series")
}

fn alternate_producer_id() -> ProducerId {
    ProducerId::parse("01941f29-7c00-7000-8000-000000000011").expect("alternate producer")
}

fn observation_id(number: usize) -> ObservationId {
    ObservationId::parse(&format!("01941f29-7c00-7000-8000-{number:012x}"))
        .expect("generated test UUIDv7")
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("normalized test timestamp")
}

fn interval(start: i64, end: i64) -> TimeInterval {
    TimeInterval::new(timestamp(start), timestamp(end)).expect("non-empty test interval")
}

fn position(epoch: u128, sequence: u128) -> ProducerPosition {
    ProducerPosition::new(ProducerEpoch::new(epoch), ProducerSequence::new(sequence))
}

fn observation(
    number: usize,
    producer_position: Option<ProducerPosition>,
    observation_interval: Option<TimeInterval>,
) -> Observation {
    Observation::new(
        observation_id(number),
        ExactValue::Unsigned(u64::try_from(number).expect("small test number")),
        ObservationTimes::new(None, timestamp(20), timestamp(10)),
        Quality::new(QualityLevel::Unknown, QualityFlags::default()),
        NativeStatus::absent(),
        producer_position,
        observation_interval,
    )
}

fn gap(epoch: u128, start: u128, end: u128) -> Gap {
    Gap::new(
        ProducerEpoch::new(epoch),
        ProducerSequence::new(start),
        ProducerSequence::new(end),
        GapReason::Unknown,
    )
    .expect("non-empty test gap")
}

fn content(digest_byte: u8) -> ContentIdentity {
    ContentIdentity::new(
        ContentFormat::new("och-envelope".to_owned()).expect("content format"),
        ContentVersion::new(1),
        [digest_byte; 32],
    )
}

#[test]
fn series_metadata_owns_nominal_scope_and_all_five_closed_modes() {
    for mode in [
        CollectionMode::Sampled,
        CollectionMode::ChangeOnly,
        CollectionMode::Cumulative,
        CollectionMode::Interval,
        CollectionMode::Event,
    ] {
        let metadata = series(mode);
        assert_eq!(metadata.collection_mode(), mode);
        assert_eq!(
            metadata.series_id(),
            SeriesId::parse("01941f29-7c00-7000-8000-000000000001").expect("series")
        );
        assert_eq!(
            metadata.producer_id(),
            ProducerId::parse("01941f29-7c00-7000-8000-000000000010").expect("producer")
        );
    }
}

#[test]
fn interval_and_gap_ranges_are_non_empty_and_half_open() {
    assert_eq!(
        TimeInterval::new(timestamp(1), timestamp(1)),
        Err(ModelError::EmptyTimeInterval)
    );
    assert_eq!(
        TimeInterval::new(timestamp(2), timestamp(1)),
        Err(ModelError::EmptyTimeInterval)
    );
    let time_range = interval(1, 2);
    assert!(time_range.contains(timestamp(1)));
    assert!(!time_range.contains(timestamp(2)));

    assert_eq!(
        Gap::new(
            ProducerEpoch::new(1),
            ProducerSequence::new(4),
            ProducerSequence::new(4),
            GapReason::Unknown,
        ),
        Err(ModelError::EmptyGap)
    );
    let sequence_range = gap(1, 4, 5);
    assert!(sequence_range.contains(ProducerEpoch::new(1), ProducerSequence::new(4)));
    assert!(!sequence_range.contains(ProducerEpoch::new(1), ProducerSequence::new(5)));
    assert!(!sequence_range.contains(ProducerEpoch::new(2), ProducerSequence::new(4)));
}

#[test]
fn every_sanitized_gap_reason_is_retained_without_time_claims() {
    for reason in [
        GapReason::Unknown,
        GapReason::ProducerRestart,
        GapReason::BufferOverflow,
        GapReason::CommunicationFailure,
        GapReason::SourceDataLoss,
        GapReason::AdministrativeExclusion,
    ] {
        let value = Gap::new(
            ProducerEpoch::new(3),
            ProducerSequence::new(4),
            ProducerSequence::new(5),
            reason,
        )
        .expect("gap");
        assert_eq!(value.reason(), reason);
        assert_eq!(value.epoch(), ProducerEpoch::new(3));
        assert_eq!(value.start(), ProducerSequence::new(4));
        assert_eq!(value.end(), ProducerSequence::new(5));
    }
}

#[test]
fn raw_order_is_exact_effective_receive_identity_tuple() {
    let mut first = observation(1, Some(position(9, 99)), None);
    let second = observation(2, Some(position(0, 0)), None);
    assert!(first.raw_order_key() < second.raw_order_key());

    first = Observation::new(
        observation_id(9),
        ExactValue::Boolean(true),
        ObservationTimes::new(Some(timestamp(-999)), timestamp(50), timestamp(5)),
        Quality::new(QualityLevel::Bad, QualityFlags::default()),
        NativeStatus::absent(),
        Some(position(99, 99)),
        None,
    );
    let earlier_effective = Observation::new(
        observation_id(10),
        ExactValue::Boolean(false),
        ObservationTimes::new(Some(timestamp(999)), timestamp(100), timestamp(4)),
        Quality::new(QualityLevel::Good, QualityFlags::default()),
        NativeStatus::absent(),
        Some(position(0, 0)),
        None,
    );
    assert!(earlier_effective.raw_order_key() < first.raw_order_key());

    let earlier_receive = Observation::new(
        observation_id(11),
        ExactValue::Boolean(false),
        ObservationTimes::new(None, timestamp(49), timestamp(5)),
        Quality::new(QualityLevel::Unknown, QualityFlags::default()),
        NativeStatus::absent(),
        None,
        None,
    );
    assert!(earlier_receive.raw_order_key() < first.raw_order_key());

    let excluded_fields_a = Observation::new(
        observation_id(12),
        ExactValue::Boolean(false),
        ObservationTimes::new(Some(timestamp(-1)), timestamp(7), timestamp(6)),
        Quality::new(QualityLevel::Unknown, QualityFlags::default()),
        NativeStatus::absent(),
        Some(position(1, 1)),
        None,
    );
    let excluded_fields_b = Observation::new(
        observation_id(12),
        ExactValue::Boolean(true),
        ObservationTimes::new(Some(timestamp(100)), timestamp(7), timestamp(6)),
        Quality::new(QualityLevel::Bad, QualityFlags::default()),
        NativeStatus::absent(),
        Some(position(8, 8)),
        None,
    );
    assert_eq!(
        excluded_fields_a.raw_order_key(),
        excluded_fields_b.raw_order_key()
    );
    assert_eq!(excluded_fields_a.raw_order_key().effective(), timestamp(6));
    assert_eq!(excluded_fields_a.raw_order_key().receive(), timestamp(7));
    assert_eq!(
        excluded_fields_a.raw_order_key().observation_id(),
        observation_id(12)
    );
}

#[test]
fn observed_envelopes_accept_each_mode_with_exact_interval_rules() {
    for mode in [
        CollectionMode::Sampled,
        CollectionMode::ChangeOnly,
        CollectionMode::Cumulative,
        CollectionMode::Event,
    ] {
        let envelope = CollectionEnvelope::observed(
            series(mode),
            vec![observation(1, None, None)],
            Vec::new(),
        )
        .expect("non-interval observed evidence");
        assert_eq!(envelope.evidence_kind(), EvidenceKind::Observed);
        assert_eq!(envelope.observations().len(), 1);
        assert!(envelope.gaps().is_empty());
        assert_eq!(envelope.series().collection_mode(), mode);
    }

    let envelope = CollectionEnvelope::observed(
        series(CollectionMode::Interval),
        vec![observation(1, None, Some(interval(0, 1)))],
        Vec::new(),
    )
    .expect("interval evidence");
    assert_eq!(envelope.observations()[0].interval(), Some(interval(0, 1)));
}

#[test]
fn observed_envelopes_reject_wrong_interval_metadata_for_every_mode() {
    assert_eq!(
        CollectionEnvelope::observed(
            series(CollectionMode::Interval),
            vec![observation(1, None, None)],
            Vec::new(),
        ),
        Err(ModelError::MissingObservationInterval)
    );

    for mode in [
        CollectionMode::Sampled,
        CollectionMode::ChangeOnly,
        CollectionMode::Cumulative,
        CollectionMode::Event,
    ] {
        assert_eq!(
            CollectionEnvelope::observed(
                series(mode),
                vec![observation(1, None, Some(interval(0, 1)))],
                Vec::new(),
            ),
            Err(ModelError::UnexpectedObservationInterval)
        );
    }
}

#[test]
fn no_change_is_non_empty_change_only_evidence_with_no_items() {
    let no_change = NoChange::new(interval(-2, -1));
    let envelope = CollectionEnvelope::no_change(series(CollectionMode::ChangeOnly), no_change)
        .expect("change-only no-change evidence");
    assert_eq!(envelope.evidence_kind(), EvidenceKind::NoChange);
    assert_eq!(envelope.no_change_evidence(), Some(no_change));
    assert!(envelope.observations().is_empty());
    assert!(envelope.gaps().is_empty());

    for mode in [
        CollectionMode::Sampled,
        CollectionMode::Cumulative,
        CollectionMode::Interval,
        CollectionMode::Event,
    ] {
        assert_eq!(
            CollectionEnvelope::no_change(series(mode), no_change),
            Err(ModelError::InvalidNoChangeMode)
        );
    }
}

#[test]
fn observed_evidence_requires_an_observation_or_gap() {
    assert_eq!(
        CollectionEnvelope::observed(series(CollectionMode::Sampled), Vec::new(), Vec::new()),
        Err(ModelError::EmptyObservedEvidence)
    );
    assert!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            Vec::new(),
            vec![gap(1, 0, 1)]
        )
        .is_ok()
    );
}

#[test]
fn collection_accepts_exact_item_maxima() {
    let observations = (1..=MAX_OBSERVATIONS)
        .map(|number| observation(number, None, None))
        .collect();
    let gaps = (0..MAX_GAPS)
        .map(|number| {
            let start = u128::try_from(number * 2).expect("small gap start");
            gap(1, start, start + 1)
        })
        .collect();
    let envelope =
        CollectionEnvelope::observed(series(CollectionMode::Sampled), observations, gaps)
            .expect("exact maxima");
    assert_eq!(envelope.observations().len(), MAX_OBSERVATIONS);
    assert_eq!(envelope.gaps().len(), MAX_GAPS);
}

#[test]
fn collection_rejects_one_over_bounds_before_item_validation() {
    let observations = (0..=MAX_OBSERVATIONS)
        .map(|_| observation(1, None, Some(interval(0, 1))))
        .collect();
    assert_eq!(
        CollectionEnvelope::observed(series(CollectionMode::Sampled), observations, Vec::new(),),
        Err(ModelError::TooManyObservations)
    );

    let gaps = (0..=MAX_GAPS)
        .rev()
        .map(|number| {
            let start = u128::try_from(number * 2).expect("small gap start");
            gap(1, start, start + 1)
        })
        .collect();
    assert_eq!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            vec![observation(1, None, None)],
            gaps,
        ),
        Err(ModelError::TooManyGaps)
    );
}

#[test]
fn collection_rejects_duplicate_observation_ids() {
    assert_eq!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            vec![observation(1, None, None), observation(1, None, None)],
            Vec::new(),
        ),
        Err(ModelError::DuplicateObservationId)
    );
}

#[test]
fn collection_rejects_mixed_and_misordered_producer_positions() {
    assert_eq!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            vec![
                observation(1, Some(position(1, 1)), None),
                observation(2, None, None)
            ],
            Vec::new(),
        ),
        Err(ModelError::MixedProducerPositions)
    );
    assert_eq!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            vec![
                observation(1, Some(position(1, 2)), None),
                observation(2, Some(position(1, 1)), None),
            ],
            Vec::new(),
        ),
        Err(ModelError::MisorderedProducerPositions)
    );
    assert_eq!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            vec![
                observation(1, Some(position(1, 1)), None),
                observation(2, Some(position(1, 1)), None),
            ],
            Vec::new(),
        ),
        Err(ModelError::MisorderedProducerPositions)
    );
    assert!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            vec![
                observation(1, Some(position(1, u128::MAX)), None),
                observation(2, Some(position(2, 0)), None),
            ],
            Vec::new(),
        )
        .is_ok()
    );
}

#[test]
fn collection_rejects_misordered_and_overlapping_gaps_but_allows_adjacency() {
    assert_eq!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            Vec::new(),
            vec![gap(2, 0, 1), gap(1, 2, 3)],
        ),
        Err(ModelError::MisorderedGaps)
    );
    assert_eq!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            Vec::new(),
            vec![gap(1, 4, 5), gap(1, 3, 4)],
        ),
        Err(ModelError::MisorderedGaps)
    );
    assert_eq!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            Vec::new(),
            vec![gap(1, 1, 4), gap(1, 3, 5)],
        ),
        Err(ModelError::OverlappingGaps)
    );
    assert!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            Vec::new(),
            vec![gap(1, 1, 3), gap(1, 3, 5), gap(2, 0, 1)],
        )
        .is_ok()
    );
}

#[test]
fn positioned_observations_cannot_fall_inside_gaps() {
    assert_eq!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            vec![observation(1, Some(position(1, 3)), None)],
            vec![gap(1, 3, 5)],
        ),
        Err(ModelError::ObservationInsideGap)
    );
    assert!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            vec![
                observation(1, Some(position(1, 2)), None),
                observation(2, Some(position(1, 5)), None),
                observation(3, Some(position(2, 0)), None),
            ],
            vec![gap(1, 3, 5)],
        )
        .is_ok()
    );
    assert!(
        CollectionEnvelope::observed(
            series(CollectionMode::Sampled),
            vec![observation(1, None, None)],
            vec![gap(1, 0, 1)],
        )
        .is_ok()
    );
}

#[test]
fn observation_accessors_preserve_owned_evidence() {
    let producer_position = position(7, 8);
    let value = observation(1, Some(producer_position), None);
    assert_eq!(value.observation_id(), observation_id(1));
    assert_eq!(value.value(), &ExactValue::Unsigned(1));
    assert_eq!(value.times().effective(), timestamp(10));
    assert_eq!(value.quality().level(), QualityLevel::Unknown);
    assert!(value.native_status().is_absent());
    assert_eq!(value.producer_position(), Some(producer_position));
    assert_eq!(value.interval(), None);
}

#[test]
fn retry_comparison_uses_scope_key_and_external_content_matrix() {
    let metadata = series(CollectionMode::Sampled);
    let original = RetryQualification::new(
        metadata.series_id(),
        metadata.producer_id(),
        RetryKey::new("retry-secret".to_owned()).expect("retry key"),
        content(1),
    );
    let equivalent = original.clone();
    assert_eq!(
        original.classify(&equivalent),
        RetryClassification::Equivalent
    );

    let conflict = RetryQualification::new(
        original.series_id(),
        original.producer_id(),
        RetryKey::new("retry-secret".to_owned()).expect("same retry key"),
        content(2),
    );
    assert_eq!(original.classify(&conflict), RetryClassification::Conflict);

    let different_series = RetryQualification::new(
        alternate_series_id(),
        original.producer_id(),
        RetryKey::new("retry-secret".to_owned()).expect("same retry key"),
        content(1),
    );
    let different_producer = RetryQualification::new(
        original.series_id(),
        alternate_producer_id(),
        RetryKey::new("retry-secret".to_owned()).expect("same retry key"),
        content(1),
    );
    let different_key = RetryQualification::new(
        original.series_id(),
        original.producer_id(),
        RetryKey::new("other-key".to_owned()).expect("different retry key"),
        content(1),
    );
    for distinct in [&different_series, &different_producer, &different_key] {
        assert_eq!(original.classify(distinct), RetryClassification::Distinct);
    }
    assert_eq!(original.key().as_str(), "retry-secret");
    assert_eq!(original.content(), &content(1));
}

#[test]
fn retry_debug_redacts_key_at_both_key_and_qualification_levels() {
    let qualification = RetryQualification::new(
        series(CollectionMode::Sampled).series_id(),
        series(CollectionMode::Sampled).producer_id(),
        RetryKey::new("do-not-log-this".to_owned()).expect("retry key"),
        content(1),
    );
    let debug = format!("{qualification:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("do-not-log-this"));
}
