#![forbid(unsafe_code)]
//! Focused deterministic evidence for canonical series declaration authority.

use och_core::{
    ArtifactId, ArtifactReference, CollectionEnvelope, CollectionMode, ContentFormat,
    ContentIdentity, ContentVersion, DeclarationEvidence, DeclarationReference,
    DeclarationRevision, ExactText, ExactValue, ModelError, NativeStatus, NoChange, Observation,
    ObservationId, ObservationTimes, ProducerId, Quality, QualityFlags, QualityLevel,
    QuantityEvidence, RealBits, RetryClassification, RetryKey, RetryQualification, SeriesBinding,
    SeriesDeclarationPayload, SeriesId, SeriesLifecycle, SeriesMetadata, SeriesRegistry,
    SeriesRegistryLimits, SourceReference, StateClass, StateMember, StateValue, StoreId,
    TimeInterval, Timestamp, Unavailable, UnitEvidence, ValueFamily,
};

fn uuid_bytes(number: u64) -> [u8; 16] {
    let suffix = number.to_be_bytes();
    [
        0x01, 0x94, 0x1f, 0x29, 0x7c, 0x00, 0x70, 0x00, 0x80, 0x00, suffix[2], suffix[3],
        suffix[4], suffix[5], suffix[6], suffix[7],
    ]
}

fn store() -> StoreId {
    StoreId::from_bytes(uuid_bytes(100)).expect("valid store identity")
}

fn series(number: u64) -> SeriesId {
    SeriesId::from_bytes(uuid_bytes(number)).expect("valid series identity")
}

fn producer(number: u64) -> ProducerId {
    ProducerId::from_bytes(uuid_bytes(number)).expect("valid producer identity")
}

fn reference(value: &str) -> DeclarationReference {
    DeclarationReference::new(value.to_owned()).expect("valid declaration reference")
}

fn binding(provider: &str, locator: &str) -> SeriesBinding {
    SeriesBinding::new(SourceReference::new(
        reference(provider),
        reference(locator),
    ))
}

fn payload(
    producer_number: u64,
    mode: CollectionMode,
    family: ValueFamily,
    application: Option<&str>,
) -> SeriesDeclarationPayload {
    SeriesDeclarationPayload::new(
        producer(producer_number),
        mode,
        family,
        QuantityEvidence::Resolved(reference("quantity:temperature")),
        UnitEvidence::Resolved(reference("unit:deg-c")),
        application.map(reference),
    )
}

fn evidence(second: i64) -> DeclarationEvidence {
    DeclarationEvidence::new(
        Timestamp::new(second, 0).expect("valid evidence timestamp"),
        None,
    )
}

fn observation(value: ExactValue) -> Observation {
    let timestamp = Timestamp::new(10, 0).expect("valid timestamp");
    Observation::new(
        ObservationId::from_bytes(uuid_bytes(900)).expect("valid observation identity"),
        value,
        ObservationTimes::new(None, timestamp, timestamp),
        Quality::new(QualityLevel::Good, QualityFlags::none()),
        NativeStatus::absent(),
        None,
        None,
    )
}

fn envelope(
    series_id: SeriesId,
    declaration: &SeriesDeclarationPayload,
    value: ExactValue,
) -> CollectionEnvelope {
    CollectionEnvelope::observed(
        SeriesMetadata::new(
            series_id,
            declaration.producer_id(),
            declaration.collection_mode(),
        ),
        vec![observation(value)],
        Vec::new(),
    )
    .expect("valid collection envelope")
}

fn assert_refusal_unchanged<T>(
    registry: &mut SeriesRegistry,
    expected: ModelError,
    operation: impl FnOnce(&mut SeriesRegistry) -> Result<T, ModelError>,
) {
    let before = registry.snapshot();
    assert_eq!(operation(registry).err(), Some(expected));
    assert_eq!(registry.snapshot(), before);
}

fn assert_revision_refusals(
    registry: &mut SeriesRegistry,
    series_id: SeriesId,
    stale_revision: DeclarationRevision,
    active_revision: DeclarationRevision,
    active_payload: SeriesDeclarationPayload,
) {
    assert_refusal_unchanged(registry, ModelError::StaleDeclarationRevision, |registry| {
        registry.revise(
            series_id,
            stale_revision,
            payload(12, CollectionMode::Event, ValueFamily::Text, None),
            evidence(3),
        )
    });
    assert_refusal_unchanged(registry, ModelError::DeclarationUnchanged, |registry| {
        registry.revise(series_id, active_revision, active_payload, evidence(3))
    });
}

#[test]
fn store_identity_references_and_value_families_are_exact_and_bounded() {
    assert_eq!(store().to_string(), "01941f29-7c00-7000-8000-000000000064");
    assert_eq!(
        DeclarationReference::new(String::new()),
        Err(ModelError::InvalidDeclarationReference)
    );
    assert_eq!(
        DeclarationReference::new("control\ntext".to_owned()),
        Err(ModelError::InvalidDeclarationReference)
    );
    assert_eq!(
        DeclarationReference::new("x".repeat(1_025)),
        Err(ModelError::InvalidDeclarationReference)
    );
    assert_eq!(
        DeclarationRevision::new(0),
        Err(ModelError::InvalidDeclarationRevision)
    );
    assert_eq!(DeclarationRevision::new(1), Ok(DeclarationRevision::FIRST));

    let families_and_values = [
        (ValueFamily::Real, ExactValue::Real(RealBits::from_bits(1))),
        (ValueFamily::Signed, ExactValue::Signed(i64::MIN)),
        (ValueFamily::Unsigned, ExactValue::Unsigned(u64::MAX)),
        (ValueFamily::Boolean, ExactValue::Boolean(true)),
        (
            ValueFamily::State,
            ExactValue::State(StateValue::new(
                StateClass::new("class".to_owned()).expect("valid class"),
                StateMember::new("member".to_owned()).expect("valid member"),
            )),
        ),
        (
            ValueFamily::Text,
            ExactValue::Text(ExactText::new("text".to_owned()).expect("valid text")),
        ),
        (
            ValueFamily::Artifact,
            ExactValue::Artifact(ArtifactReference::new(
                ArtifactId::from_bytes(uuid_bytes(700)).expect("valid artifact identity"),
                ContentIdentity::new(
                    ContentFormat::new("application/octet-stream".to_owned())
                        .expect("valid content format"),
                    ContentVersion::new(1),
                    [0x11; 32],
                ),
            )),
        ),
    ];
    let unavailable = ExactValue::Unavailable(Unavailable::without_reason());
    for (index, (family, value)) in families_and_values.iter().enumerate() {
        assert!(family.admits(&unavailable));
        assert!(family.admits(value));
        for (other_index, (_, other_value)) in families_and_values.iter().enumerate() {
            assert_eq!(family.admits(other_value), index == other_index);
        }
    }

    let unresolved_payload = SeriesDeclarationPayload::new(
        producer(10),
        CollectionMode::Sampled,
        ValueFamily::Boolean,
        QuantityEvidence::Unresolved(reference("native-quantity")),
        UnitEvidence::Unresolved(reference("native-unit")),
        None,
    );
    assert!(matches!(
        unresolved_payload.quantity(),
        QuantityEvidence::Unresolved(reference) if reference.as_str() == "native-quantity"
    ));
    assert!(matches!(
        unresolved_payload.unit(),
        UnitEvidence::Unresolved(reference) if reference.as_str() == "native-unit"
    ));
}

#[test]
fn initial_revision_replay_and_correction_are_atomic_and_monotonic() {
    let series_id = series(1);
    let logical_binding = binding("provider:a", "locator:a");
    let first_payload = payload(
        10,
        CollectionMode::Sampled,
        ValueFamily::Boolean,
        Some("app:a"),
    );
    let first_evidence = evidence(1);
    let mut registry = SeriesRegistry::new(store(), SeriesRegistryLimits::new(4, 8));

    let initial = registry
        .register(
            series_id,
            logical_binding.clone(),
            first_payload.clone(),
            first_evidence.clone(),
        )
        .expect("initial declaration");
    assert_eq!(initial.revision(), DeclarationRevision::FIRST);
    assert_eq!(initial.previous_revision(), None);
    assert_eq!(initial.binding(), &logical_binding);
    assert_eq!(initial.evidence(), &first_evidence);
    let before_replay = registry.snapshot();
    assert_eq!(
        registry
            .register(
                series_id,
                logical_binding.clone(),
                first_payload.clone(),
                first_evidence.clone(),
            )
            .expect("exact registration replay"),
        initial
    );
    assert_eq!(registry.snapshot(), before_replay);

    assert_refusal_unchanged(
        &mut registry,
        ModelError::SeriesAlreadyRegistered,
        |registry| {
            registry.register(
                series_id,
                binding("provider:b", "locator:b"),
                first_payload.clone(),
                first_evidence.clone(),
            )
        },
    );

    let corrected = payload(
        11,
        CollectionMode::ChangeOnly,
        ValueFamily::Signed,
        Some("app:corrected"),
    );
    let correction_evidence = evidence(2);
    let second = registry
        .revise(
            series_id,
            initial.revision(),
            corrected.clone(),
            correction_evidence.clone(),
        )
        .expect("accepted correction");
    assert_eq!(second.revision().get(), 2);
    assert_eq!(second.previous_revision(), Some(initial.revision()));
    assert_eq!(second.binding(), initial.binding());
    assert_eq!(second.payload(), &corrected);

    let before_revision_replay = registry.snapshot();
    assert_eq!(
        registry
            .revise(
                series_id,
                initial.revision(),
                corrected.clone(),
                correction_evidence.clone(),
            )
            .expect("exact latest revision replay"),
        second
    );
    assert_eq!(registry.snapshot(), before_revision_replay);

    assert_revision_refusals(
        &mut registry,
        series_id,
        initial.revision(),
        second.revision(),
        corrected,
    );

    assert_eq!(registry.declaration_revision_count(), 2);
    assert_eq!(
        registry
            .resolve(series_id, DeclarationRevision::FIRST)
            .expect("historic revision")
            .payload(),
        &first_payload
    );
    assert_eq!(
        registry
            .resolve(series_id, second.revision())
            .expect("current revision"),
        &second
    );
}

#[test]
fn only_the_active_exact_declaration_can_bind_an_envelope() {
    let series_id = series(1);
    let first_payload = payload(10, CollectionMode::Sampled, ValueFamily::Boolean, None);
    let mut registry = SeriesRegistry::new(store(), SeriesRegistryLimits::new(2, 4));
    let first = registry
        .register(
            series_id,
            binding("provider:a", "locator:a"),
            first_payload.clone(),
            evidence(1),
        )
        .expect("initial declaration");

    let bound = registry
        .bind(envelope(
            series_id,
            &first_payload,
            ExactValue::Boolean(true),
        ))
        .expect("active declaration binding");
    assert_eq!(bound.store_id(), store());
    assert_eq!(bound.declaration(), &first);
    assert_eq!(bound.envelope().observations().len(), 1);

    let unavailable = registry
        .bind(envelope(
            series_id,
            &first_payload,
            ExactValue::Unavailable(Unavailable::without_reason()),
        ))
        .expect("unavailable is valid for every declared family");
    assert_eq!(unavailable.declaration().revision(), first.revision());

    assert_eq!(
        registry.bind(envelope(
            series(2),
            &first_payload,
            ExactValue::Boolean(true)
        )),
        Err(ModelError::SeriesNotFound)
    );
    let wrong_producer = payload(11, CollectionMode::Sampled, ValueFamily::Boolean, None);
    assert_eq!(
        registry.bind(envelope(
            series_id,
            &wrong_producer,
            ExactValue::Boolean(true)
        )),
        Err(ModelError::SeriesMetadataMismatch)
    );
    assert_eq!(
        registry.bind(envelope(series_id, &first_payload, ExactValue::Signed(1))),
        Err(ModelError::ObservationValueFamilyMismatch)
    );

    let revised_payload = payload(11, CollectionMode::ChangeOnly, ValueFamily::Signed, None);
    let revised = registry
        .revise(
            series_id,
            first.revision(),
            revised_payload.clone(),
            evidence(2),
        )
        .expect("metadata revision");
    assert_eq!(
        registry.bind(envelope(
            series_id,
            &first_payload,
            ExactValue::Boolean(true)
        )),
        Err(ModelError::SeriesMetadataMismatch)
    );
    let no_change = CollectionEnvelope::no_change(
        SeriesMetadata::new(
            series_id,
            revised_payload.producer_id(),
            revised_payload.collection_mode(),
        ),
        NoChange::new(
            TimeInterval::new(
                Timestamp::new(20, 0).expect("valid start"),
                Timestamp::new(21, 0).expect("valid end"),
            )
            .expect("valid interval"),
        ),
    )
    .expect("valid revised-mode no-change envelope");
    assert_eq!(
        registry
            .bind(no_change)
            .expect("current exact revision binding")
            .declaration(),
        &revised
    );
}

#[test]
fn retirement_is_terminal_historic_and_rebind_requires_a_new_series_identity() {
    let old_id = series(1);
    let old_binding = binding("provider:a", "locator:old");
    let old_payload = payload(10, CollectionMode::Sampled, ValueFamily::Boolean, None);
    let mut registry = SeriesRegistry::new(store(), SeriesRegistryLimits::new(3, 4));
    let initial = registry
        .register(
            old_id,
            old_binding.clone(),
            old_payload.clone(),
            evidence(1),
        )
        .expect("initial declaration");
    let retirement_evidence = evidence(2);

    assert_refusal_unchanged(
        &mut registry,
        ModelError::StaleDeclarationRevision,
        |registry| {
            registry.retire(
                old_id,
                DeclarationRevision::new(2).expect("valid comparison revision"),
                retirement_evidence.clone(),
            )
        },
    );
    let retirement = registry
        .retire(old_id, initial.revision(), retirement_evidence.clone())
        .expect("terminal retirement");
    let before_retry = registry.snapshot();
    assert_eq!(
        registry
            .retire(old_id, initial.revision(), retirement_evidence.clone())
            .expect("exact retirement replay"),
        retirement
    );
    assert_eq!(registry.snapshot(), before_retry);

    assert_refusal_unchanged(&mut registry, ModelError::SeriesRetired, |registry| {
        registry.revise(
            old_id,
            initial.revision(),
            payload(11, CollectionMode::Event, ValueFamily::Text, None),
            evidence(3),
        )
    });
    assert_refusal_unchanged(&mut registry, ModelError::SeriesRetired, |registry| {
        registry.register(
            old_id,
            old_binding.clone(),
            old_payload.clone(),
            evidence(1),
        )
    });
    assert_eq!(
        registry.bind(envelope(old_id, &old_payload, ExactValue::Boolean(true))),
        Err(ModelError::SeriesRetired)
    );
    assert_eq!(
        registry
            .history(old_id)
            .expect("retained tombstone")
            .lifecycle(),
        SeriesLifecycle::Retired
    );
    assert_eq!(
        registry
            .resolve(old_id, initial.revision())
            .expect("historic declaration after retirement"),
        &initial
    );

    let new_id = series(2);
    let new_binding = binding("provider:a", "locator:new");
    let replacement = registry
        .register(new_id, new_binding.clone(), old_payload, evidence(3))
        .expect("new logical point requires and accepts a new identity");
    assert_ne!(replacement.series_id(), initial.series_id());
    assert_ne!(replacement.binding(), initial.binding());
    assert_eq!(registry.series_count(), 2);
}

#[test]
fn list_snapshot_revision_and_tombstone_capacity_boundaries_are_exact() {
    let limits = SeriesRegistryLimits::new(2, 3);
    let mut registry = SeriesRegistry::new(store(), limits);
    let common = payload(10, CollectionMode::Sampled, ValueFamily::Boolean, None);
    let high = registry
        .register(
            series(2),
            binding("provider", "high"),
            common.clone(),
            evidence(1),
        )
        .expect("first series");
    let low = registry
        .register(
            series(1),
            binding("provider", "low"),
            common.clone(),
            evidence(1),
        )
        .expect("second series");
    assert_eq!(
        registry.series_ids().collect::<Vec<_>>(),
        vec![series(1), series(2)]
    );

    let revised = registry
        .revise(
            low.series_id(),
            low.revision(),
            payload(10, CollectionMode::Sampled, ValueFamily::Signed, None),
            evidence(2),
        )
        .expect("exact final revision slot");
    assert_eq!(revised.revision().get(), 2);
    assert_refusal_unchanged(
        &mut registry,
        ModelError::RegistryRevisionCapacityExceeded,
        |registry| {
            registry.revise(
                high.series_id(),
                high.revision(),
                payload(10, CollectionMode::Sampled, ValueFamily::Unsigned, None),
                evidence(2),
            )
        },
    );

    registry
        .retire(high.series_id(), high.revision(), evidence(3))
        .expect("retirement does not erase or consume a declaration slot");
    assert_refusal_unchanged(
        &mut registry,
        ModelError::RegistrySeriesCapacityExceeded,
        |registry| {
            registry.register(
                series(3),
                binding("provider", "third"),
                common.clone(),
                evidence(4),
            )
        },
    );

    let snapshot = registry.snapshot();
    assert_eq!(snapshot.store_id(), store());
    assert_eq!(snapshot.limits(), limits);
    assert_eq!(snapshot.declaration_revision_count(), 3);
    assert_eq!(
        snapshot
            .series()
            .iter()
            .map(och_core::SeriesHistory::series_id)
            .collect::<Vec<_>>(),
        vec![series(1), series(2)]
    );
    assert_eq!(
        snapshot.series()[0]
            .declarations()
            .iter()
            .map(|declaration| declaration.revision().get())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(snapshot.series()[1].lifecycle(), SeriesLifecycle::Retired);

    let mut zero_series = SeriesRegistry::new(store(), SeriesRegistryLimits::new(0, 1));
    assert_refusal_unchanged(
        &mut zero_series,
        ModelError::RegistrySeriesCapacityExceeded,
        |registry| {
            registry.register(
                series(1),
                binding("provider", "zero"),
                common.clone(),
                evidence(1),
            )
        },
    );
    let mut zero_revisions = SeriesRegistry::new(store(), SeriesRegistryLimits::new(1, 0));
    assert_refusal_unchanged(
        &mut zero_revisions,
        ModelError::RegistryRevisionCapacityExceeded,
        |registry| registry.register(series(1), binding("provider", "zero"), common, evidence(1)),
    );
}

#[test]
fn existing_envelope_and_retry_contracts_remain_independent_of_the_registry() {
    let series_id = series(1);
    let producer_id = producer(10);
    let metadata = SeriesMetadata::new(series_id, producer_id, CollectionMode::Sampled);
    let envelope = CollectionEnvelope::observed(
        metadata.clone(),
        vec![observation(ExactValue::Boolean(true))],
        Vec::new(),
    )
    .expect("existing envelope construction remains valid without a registry");
    assert_eq!(envelope.series(), &metadata);
    assert_eq!(
        envelope.observations()[0].value(),
        &ExactValue::Boolean(true)
    );

    let content = ContentIdentity::new(
        ContentFormat::new("och-envelope".to_owned()).expect("valid format"),
        ContentVersion::new(1),
        [0x11; 32],
    );
    let first = RetryQualification::new(
        series_id,
        producer_id,
        RetryKey::new("retry".to_owned()).expect("valid retry key"),
        content.clone(),
    );
    let second = RetryQualification::new(
        series_id,
        producer_id,
        RetryKey::new("retry".to_owned()).expect("valid retry key"),
        content,
    );
    assert_eq!(first.classify(&second), RetryClassification::Equivalent);
}
