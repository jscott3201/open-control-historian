use crate::crc32c::Crc32c;
use crate::error::{EvidenceError, Result};
use crate::ledger::{MAX_FRAMES, MAX_OBSERVATIONS};
use crate::model::FixtureMeta;
use crate::root::EvidenceRoot;
use och_core::{
    ArtifactId, ArtifactReference, CanonicalAdmission, CaptureLifecycle, CaptureRunEvidence,
    CollectionEnvelope, CollectionMode, ContentFormat, ContentIdentity, ContentVersion,
    DeclarationEvidence, DeclarationReference, EvidenceId, ExactText, ExactValue, NoChange,
    NormalizedRecordEvidence, Observation, ObservationId, ObservationTimes, ProducerId, Quality,
    QualityFlags, QualityLevel, QuantityEvidence, RawRecordEvidence, RetryKey, RetryQualification,
    SeriesBinding, SeriesDeclarationPayload, SeriesId, SeriesMetadata, SeriesRegistry,
    SeriesRegistryLimits, SourceBatchMetadata, SourceEndpointEvidence, SourceInterpretation,
    SourceIntervalKind, SourceObservationContext, SourceObservationEvidence, SourceProjection,
    SourceReference, SourceSchemaIdentity, SourceSchemaVersion, SourceSnapshotEvidence,
    SourceSystemEvidence, SourceTransport, StoreId, TimeInterval, Timestamp, UnitEvidence,
    ValueFamily,
};
use och_store::{AppendSequenceV1, JournalHeaderV1, encode_admission_frame_v1};
use std::fs::{self, OpenOptions};
use std::io::Write;

const MAX_TEXT_BYTES: usize = 16_384;
const MAX_BYTE_CASE_OBSERVATIONS_PER_FRAME: usize = 16;

#[derive(Clone, Copy)]
enum FixtureProfile {
    Min,
    MinObserved,
    Representative,
    MaxRecords,
    MaxSeries,
    MaxObservations,
    MaxBytes,
    #[cfg(test)]
    TestSmall,
    #[cfg(test)]
    ProductRepresentative,
}

impl FixtureProfile {
    fn named(case: &str) -> Result<Self> {
        match case {
            "min" => Ok(Self::Min),
            "min-observed" => Ok(Self::MinObserved),
            "representative" => Ok(Self::Representative),
            "max-records" => Ok(Self::MaxRecords),
            "max-series" => Ok(Self::MaxSeries),
            "max-observations" => Ok(Self::MaxObservations),
            "max-bytes" => Ok(Self::MaxBytes),
            _ => Err(EvidenceError::Usage),
        }
    }

    const fn dimensions(self) -> (usize, usize, usize) {
        match self {
            Self::Min => (1, 1, 0),
            Self::MinObserved => (1, 1, 1),
            Self::Representative => (256, 32, 64),
            Self::MaxRecords => (MAX_FRAMES, 1, 0),
            Self::MaxSeries => (MAX_FRAMES, MAX_FRAMES, 0),
            Self::MaxObservations => (MAX_FRAMES, 32, 256),
            Self::MaxBytes => (MAX_FRAMES, 32, MAX_BYTE_CASE_OBSERVATIONS_PER_FRAME),
            #[cfg(test)]
            Self::TestSmall => (2, 2, 1),
            #[cfg(test)]
            Self::ProductRepresentative => (4, 2, 2),
        }
    }
}

pub(crate) fn generate(root: &EvidenceRoot, case: &str, seed: u64) -> Result<FixtureMeta> {
    generate_profile(root, case, seed, FixtureProfile::named(case)?)
}

pub(crate) fn generate_representative_named(
    root: &EvidenceRoot,
    case: &str,
    seed: u64,
) -> Result<FixtureMeta> {
    generate_profile(root, case, seed, FixtureProfile::Representative)
}

fn generate_profile(
    root: &EvidenceRoot,
    case: &str,
    seed: u64,
    profile: FixtureProfile,
) -> Result<FixtureMeta> {
    root.ensure_layout()?;
    let raw_path = root.raw_path(case)?;
    let meta_path = root.fixture_meta_path(case)?;
    let temporary = raw_path.with_extension("raw-journal-v1-evidence.partial");
    remove_if_present(&temporary)?;
    remove_if_present(&raw_path)?;
    remove_if_present(&meta_path)?;

    let result = (|| {
        let store_id = store_id(seed)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|_| EvidenceError::Io)?;
        let header = JournalHeaderV1::new(store_id).encode();
        file.write_all(&header).map_err(|_| EvidenceError::Io)?;
        let mut source_crc = Crc32c::new();
        source_crc.update(&header);
        let (frame_count, series_count, observations_per_frame) = profile.dimensions();
        let max_byte_sizes = if matches!(profile, FixtureProfile::MaxBytes) {
            Some(max_byte_frame_text_sizes(store_id, seed, series_count)?)
        } else {
            None
        };
        let mut source_length = u64::try_from(header.len()).map_err(|_| EvidenceError::Bounds)?;
        for frame_index in 0..frame_count {
            let text_bytes = max_byte_sizes.map(|(ordinary, final_size)| {
                if frame_index + 1 == frame_count {
                    final_size
                } else {
                    ordinary
                }
            });
            let admission = admission(
                store_id,
                seed,
                frame_index,
                series_count,
                observations_per_frame,
                text_bytes,
            )?;
            let sequence = u64::try_from(frame_index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .and_then(|value| AppendSequenceV1::new(value).ok())
                .ok_or(EvidenceError::Bounds)?;
            let frame = encode_admission_frame_v1(sequence, &admission)
                .map_err(|_| EvidenceError::InvalidFixture)?;
            source_length = source_length
                .checked_add(u64::try_from(frame.len()).map_err(|_| EvidenceError::Bounds)?)
                .ok_or(EvidenceError::Bounds)?;
            if source_length > och_store::MAX_ACTIVE_JOURNAL_BYTES {
                return Err(EvidenceError::Bounds);
            }
            source_crc.update(&frame);
            file.write_all(&frame).map_err(|_| EvidenceError::Io)?;
        }
        if matches!(profile, FixtureProfile::MaxBytes)
            && source_length != och_store::MAX_ACTIVE_JOURNAL_BYTES
        {
            return Err(EvidenceError::InvalidFixture);
        }
        file.sync_all().map_err(|_| EvidenceError::Io)?;
        drop(file);
        fs::rename(&temporary, &raw_path).map_err(|_| EvidenceError::Io)?;
        let observation_count = frame_count
            .checked_mul(observations_per_frame)
            .ok_or(EvidenceError::Bounds)?;
        if observation_count > MAX_OBSERVATIONS {
            return Err(EvidenceError::Bounds);
        }
        let meta = FixtureMeta {
            case: case.to_owned(),
            seed,
            store_id,
            journal_generation: 1,
            sequence_floor: 0,
            sequence_cutoff: u64::try_from(frame_count).map_err(|_| EvidenceError::Bounds)?,
            registry_generation: 1,
            source_length,
            source_checksum: source_crc.finish(),
            frame_count,
            series_count,
            observation_count,
        };
        meta.validate()?;
        fs::write(&meta_path, meta.encode()).map_err(|_| EvidenceError::Io)?;
        Ok(meta)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn max_byte_frame_text_sizes(
    store_id: StoreId,
    seed: u64,
    series_count: usize,
) -> Result<(usize, usize)> {
    let frame_region = och_store::MAX_ACTIVE_JOURNAL_BYTES
        .checked_sub(och_store::JOURNAL_V1_HEADER_LEN as u64)
        .ok_or(EvidenceError::Bounds)?;
    let ordinary_length = frame_region / MAX_FRAMES as u64;
    let final_length = ordinary_length
        .checked_add(frame_region % MAX_FRAMES as u64)
        .ok_or(EvidenceError::Bounds)?;
    let ordinary_text = text_bytes_for_frame_length(
        store_id,
        seed,
        series_count,
        usize::try_from(ordinary_length).map_err(|_| EvidenceError::Bounds)?,
    )?;
    let final_text = text_bytes_for_frame_length(
        store_id,
        seed,
        series_count,
        usize::try_from(final_length).map_err(|_| EvidenceError::Bounds)?,
    )?;
    Ok((ordinary_text, final_text))
}

fn text_bytes_for_frame_length(
    store_id: StoreId,
    seed: u64,
    series_count: usize,
    target: usize,
) -> Result<usize> {
    let maximum = MAX_TEXT_BYTES
        .checked_mul(MAX_BYTE_CASE_OBSERVATIONS_PER_FRAME)
        .ok_or(EvidenceError::Bounds)?;
    let mut low = 0_usize;
    let mut high = maximum;
    while low <= high {
        let middle = low + (high - low) / 2;
        let candidate = admission(
            store_id,
            seed,
            0,
            series_count,
            MAX_BYTE_CASE_OBSERVATIONS_PER_FRAME,
            Some(middle),
        )?;
        let length = och_store::admission_frame_len_v1(&candidate)
            .map_err(|_| EvidenceError::InvalidFixture)?;
        match length.cmp(&target) {
            std::cmp::Ordering::Equal => return Ok(middle),
            std::cmp::Ordering::Less => low = middle.saturating_add(1),
            std::cmp::Ordering::Greater => {
                let Some(next) = middle.checked_sub(1) else {
                    break;
                };
                high = next;
            }
        }
    }
    Err(EvidenceError::InvalidFixture)
}

fn admission(
    store: StoreId,
    seed: u64,
    frame_index: usize,
    series_count: usize,
    observation_count: usize,
    total_text_bytes: Option<usize>,
) -> Result<CanonicalAdmission> {
    let series_index = frame_index % series_count;
    let series_number = identity_number(seed, 10_000, series_index)?;
    let producer_number = identity_number(seed, 20_000, series_index)?;
    let series = series_id(series_number)?;
    let producer = producer_id(producer_number)?;
    if observation_count == 0 {
        no_change_admission(store, seed, frame_index, series, producer)
    } else {
        observed_admission(
            store,
            seed,
            frame_index,
            series,
            producer,
            observation_count,
            total_text_bytes,
        )
    }
}

fn no_change_admission(
    store: StoreId,
    seed: u64,
    frame_index: usize,
    series: SeriesId,
    producer: ProducerId,
) -> Result<CanonicalAdmission> {
    let envelope = CollectionEnvelope::no_change(
        SeriesMetadata::new(series, producer, CollectionMode::ChangeOnly),
        NoChange::new(
            TimeInterval::new(timestamp(-10, 0)?, timestamp(10, 0)?)
                .map_err(|_| EvidenceError::InvalidFixture)?,
        ),
    )
    .map_err(|_| EvidenceError::InvalidFixture)?;
    let declared = registry_bound(store, seed, envelope, ValueFamily::Boolean)?;
    CanonicalAdmission::no_change(
        declared,
        retry(series, producer, frame_index)?,
        batch(SourceIntervalKind::NoChange)?,
        lifecycle(seed)?,
    )
    .map_err(|_| EvidenceError::InvalidFixture)
}

#[allow(clippy::too_many_arguments)]
fn observed_admission(
    store: StoreId,
    seed: u64,
    frame_index: usize,
    series: SeriesId,
    producer: ProducerId,
    observation_count: usize,
    total_text_bytes: Option<usize>,
) -> Result<CanonicalAdmission> {
    let mut remaining_text = total_text_bytes.unwrap_or(0);
    let mut observations = Vec::with_capacity(observation_count);
    for ordinal in 0..observation_count {
        let value = if total_text_bytes.is_some() {
            let bytes = remaining_text.min(MAX_TEXT_BYTES);
            remaining_text -= bytes;
            ExactValue::Text(
                ExactText::new(exact_text(bytes)).map_err(|_| EvidenceError::InvalidFixture)?,
            )
        } else {
            ExactValue::Boolean((frame_index + ordinal).is_multiple_of(2))
        };
        let observation_number = identity_number(
            seed,
            100_000,
            frame_index
                .checked_mul(256)
                .and_then(|value| value.checked_add(ordinal))
                .ok_or(EvidenceError::Bounds)?,
        )?;
        observations.push(Observation::new(
            observation_id(observation_number)?,
            value,
            ObservationTimes::new(
                Some(timestamp(-1, 999_999_999)?),
                timestamp(10, 11)?,
                timestamp(9, 12)?,
            ),
            Quality::new(QualityLevel::Good, QualityFlags::none()),
            och_core::NativeStatus::absent(),
            None,
            None,
        ));
    }
    if remaining_text != 0 {
        return Err(EvidenceError::Bounds);
    }
    let family = if total_text_bytes.is_some() {
        ValueFamily::Text
    } else {
        ValueFamily::Boolean
    };
    let envelope = CollectionEnvelope::observed(
        SeriesMetadata::new(series, producer, CollectionMode::Sampled),
        observations,
        Vec::new(),
    )
    .map_err(|_| EvidenceError::InvalidFixture)?;
    let contexts = envelope
        .observations()
        .iter()
        .enumerate()
        .map(|(ordinal, observation)| source_context(seed, ordinal, observation.observation_id()))
        .collect::<Result<Vec<_>>>()?;
    let declared = registry_bound(store, seed, envelope, family)?;
    CanonicalAdmission::observed(
        declared,
        retry(series, producer, frame_index)?,
        batch(SourceIntervalKind::Observed)?,
        lifecycle(seed)?,
        contexts,
        Vec::new(),
    )
    .map_err(|_| EvidenceError::InvalidFixture)
}

fn registry_bound(
    store: StoreId,
    seed: u64,
    envelope: CollectionEnvelope,
    family: ValueFamily,
) -> Result<och_core::DeclaredCollectionEnvelope> {
    let series = envelope.series().series_id();
    let producer = envelope.series().producer_id();
    let mode = envelope.series().collection_mode();
    let mut registry = SeriesRegistry::new(store, SeriesRegistryLimits::new(1, 1));
    registry
        .register(
            series,
            SeriesBinding::new(source()?),
            declaration_payload(producer, mode, family)?,
            DeclarationEvidence::new(timestamp(-3, 7)?, Some(artifact(seed, 201, 22)?)),
        )
        .map_err(|_| EvidenceError::InvalidFixture)?;
    registry
        .bind(envelope)
        .map_err(|_| EvidenceError::InvalidFixture)
}

fn declaration_payload(
    producer: ProducerId,
    mode: CollectionMode,
    family: ValueFamily,
) -> Result<SeriesDeclarationPayload> {
    Ok(SeriesDeclarationPayload::new(
        producer,
        mode,
        family,
        QuantityEvidence::Resolved(reference("quantity:evidence")?),
        UnitEvidence::Unresolved(reference("native-unit:evidence")?),
        Some(reference("application:evidence")?),
    ))
}

fn lifecycle(seed: u64) -> Result<CaptureLifecycle> {
    CaptureLifecycle::new(
        SourceSystemEvidence::new(
            evidence_id(seed, 100)?,
            reference("provider:evidence")?,
            projection()?,
        ),
        SourceEndpointEvidence::new(
            evidence_id(seed, 101)?,
            evidence_id(seed, 100)?,
            reference("locator:evidence")?,
        ),
        CaptureRunEvidence::new(
            evidence_id(seed, 102)?,
            evidence_id(seed, 101)?,
            timestamp(-2, 999_000_000)?,
            Some(timestamp(3, 4)?),
        )
        .map_err(|_| EvidenceError::InvalidFixture)?,
        SourceSnapshotEvidence::new(
            evidence_id(seed, 103)?,
            evidence_id(seed, 102)?,
            artifact(seed, 200, 20)?,
        ),
    )
    .map_err(|_| EvidenceError::InvalidFixture)
}

fn source_context(
    seed: u64,
    ordinal: usize,
    canonical_id: ObservationId,
) -> Result<SourceObservationContext> {
    let ordinal_u64 = u64::try_from(ordinal).map_err(|_| EvidenceError::Bounds)?;
    let ordinal_u8 = u8::try_from(ordinal).map_err(|_| EvidenceError::Bounds)?;
    let observation_evidence = SourceObservationEvidence::new(
        evidence_id(seed, 1_000 + ordinal_u64 * 3)?,
        None,
        SourceTransport::New,
        None,
    );
    let raw = RawRecordEvidence::new(
        evidence_id(seed, 1_001 + ordinal_u64 * 3)?,
        evidence_id(seed, 103)?,
        artifact(seed, 400 + ordinal_u64, ordinal_u8.wrapping_add(40))?,
        None,
    );
    let normalized = NormalizedRecordEvidence::new(
        evidence_id(seed, 1_002 + ordinal_u64 * 3)?,
        raw.evidence_id(),
        content(ordinal_u8.wrapping_add(80))?,
        observation_evidence.evidence_id(),
    );
    Ok(SourceObservationContext::new(
        ordinal_u8,
        canonical_id,
        SourceInterpretation::new(
            source()?,
            Some(reference("application:evidence")?),
            QuantityEvidence::Resolved(reference("quantity:evidence")?),
            UnitEvidence::Unresolved(reference("native-unit:evidence")?),
        ),
        observation_evidence,
        raw,
        normalized,
    ))
}

fn retry(series: SeriesId, producer: ProducerId, frame_index: usize) -> Result<RetryQualification> {
    Ok(RetryQualification::new(
        series,
        producer,
        RetryKey::new(format!("evidence-{frame_index:04}"))
            .map_err(|_| EvidenceError::InvalidFixture)?,
        content(21)?,
    ))
}

fn batch(interval: SourceIntervalKind) -> Result<SourceBatchMetadata> {
    Ok(SourceBatchMetadata::new(
        SourceSchemaIdentity::new("evidence.source-batch".to_owned())
            .map_err(|_| EvidenceError::InvalidFixture)?,
        SourceSchemaVersion::new(1).map_err(|_| EvidenceError::InvalidFixture)?,
        interval,
    ))
}

fn source() -> Result<SourceReference> {
    Ok(SourceReference::with_projection(
        reference("provider:evidence")?,
        projection()?,
        reference("locator:evidence")?,
    ))
}

fn projection() -> Result<SourceProjection> {
    SourceProjection::new("Evidence".to_owned()).map_err(|_| EvidenceError::InvalidFixture)
}

fn content(seed: u8) -> Result<ContentIdentity> {
    Ok(ContentIdentity::new(
        ContentFormat::new("application/octet-stream".to_owned())
            .map_err(|_| EvidenceError::InvalidFixture)?,
        ContentVersion::new(u128::from(seed)),
        [seed; 32],
    ))
}

fn artifact(seed: u64, number: u64, content_seed: u8) -> Result<ArtifactReference> {
    Ok(ArtifactReference::new(
        ArtifactId::from_bytes(uuid_bytes(identity_number_u64(seed, number)?))
            .map_err(|_| EvidenceError::InvalidFixture)?,
        content(content_seed)?,
    ))
}

fn reference(value: &str) -> Result<DeclarationReference> {
    DeclarationReference::new(value.to_owned()).map_err(|_| EvidenceError::InvalidFixture)
}

fn timestamp(seconds: i64, nanos: u32) -> Result<Timestamp> {
    Timestamp::new(seconds, nanos).map_err(|_| EvidenceError::InvalidFixture)
}

fn store_id(seed: u64) -> Result<StoreId> {
    StoreId::from_bytes(uuid_bytes(identity_number_u64(seed, 1)?))
        .map_err(|_| EvidenceError::InvalidFixture)
}

fn series_id(number: u64) -> Result<SeriesId> {
    SeriesId::from_bytes(uuid_bytes(number)).map_err(|_| EvidenceError::InvalidFixture)
}

fn producer_id(number: u64) -> Result<ProducerId> {
    ProducerId::from_bytes(uuid_bytes(number)).map_err(|_| EvidenceError::InvalidFixture)
}

fn observation_id(number: u64) -> Result<ObservationId> {
    ObservationId::from_bytes(uuid_bytes(number)).map_err(|_| EvidenceError::InvalidFixture)
}

fn evidence_id(seed: u64, number: u64) -> Result<EvidenceId> {
    EvidenceId::from_bytes(uuid_bytes(identity_number_u64(seed, number)?))
        .map_err(|_| EvidenceError::InvalidFixture)
}

fn identity_number(seed: u64, base: u64, index: usize) -> Result<u64> {
    identity_number_u64(
        seed,
        base.checked_add(u64::try_from(index).map_err(|_| EvidenceError::Bounds)?)
            .ok_or(EvidenceError::Bounds)?,
    )
}

fn identity_number_u64(seed: u64, number: u64) -> Result<u64> {
    seed.checked_mul(2_000_000)
        .and_then(|value| value.checked_add(number))
        .ok_or(EvidenceError::Bounds)
}

fn uuid_bytes(number: u64) -> [u8; 16] {
    let suffix = number.to_be_bytes();
    [
        0x01, 0x94, 0x1f, 0x29, 0x7c, 0x00, 0x70, 0x00, 0x80, 0x00, suffix[2], suffix[3],
        suffix[4], suffix[5], suffix[6], suffix[7],
    ]
}

fn exact_text(bytes: usize) -> String {
    let four_byte = bytes / 4;
    let remainder = bytes % 4;
    let mut output = "🦀".repeat(four_byte);
    output.push_str(match remainder {
        0 => "",
        1 => "a",
        2 => "¢",
        3 => "€",
        _ => unreachable!(),
    });
    output
}

fn remove_if_present(path: &std::path::Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(EvidenceError::Io),
    }
}

#[cfg(test)]
pub(crate) fn write_raw_fixture(
    root: &EvidenceRoot,
    case: &str,
    seed: u64,
    profile: &str,
) -> Result<FixtureMeta> {
    let profile = if profile == "test-small" {
        FixtureProfile::TestSmall
    } else if profile == "product-representative" {
        FixtureProfile::ProductRepresentative
    } else {
        FixtureProfile::named(profile)?
    };
    generate_profile(root, case, seed, profile)
}

#[cfg(test)]
pub(crate) fn admission_for_test(store: StoreId, seed: u64) -> Result<CanonicalAdmission> {
    admission(store, seed, 0, 1, 0, None)
}

#[cfg(test)]
pub(crate) fn product_representative_admission_for_test(
    store: StoreId,
    seed: u64,
    frame_index: usize,
) -> Result<CanonicalAdmission> {
    admission(store, seed, frame_index, 2, 2, None)
}

#[cfg(test)]
pub(crate) fn bound_probe() -> Result<(u64, u64)> {
    let seed = 19;
    let store = store_id(seed)?;
    let (ordinary_text, final_text) = max_byte_frame_text_sizes(store, seed, 32)?;
    let ordinary = admission(
        store,
        seed,
        0,
        32,
        MAX_BYTE_CASE_OBSERVATIONS_PER_FRAME,
        Some(ordinary_text),
    )?;
    let final_frame = admission(
        store,
        seed,
        0,
        32,
        MAX_BYTE_CASE_OBSERVATIONS_PER_FRAME,
        Some(final_text),
    )?;
    let max_byte_source = (och_store::JOURNAL_V1_HEADER_LEN as u64)
        .checked_add(
            u64::try_from(
                och_store::admission_frame_len_v1(&ordinary)
                    .map_err(|_| EvidenceError::InvalidFixture)?,
            )
            .map_err(|_| EvidenceError::Bounds)?
            .checked_mul((MAX_FRAMES - 1) as u64)
            .ok_or(EvidenceError::Bounds)?,
        )
        .and_then(|length| {
            length.checked_add(
                u64::try_from(och_store::admission_frame_len_v1(&final_frame).ok()?).ok()?,
            )
        })
        .ok_or(EvidenceError::Bounds)?;
    let max_observation_frame = admission(store, seed, 0, 32, 256, None)?;
    let max_observation_source = (och_store::JOURNAL_V1_HEADER_LEN as u64)
        .checked_add(
            u64::try_from(
                och_store::admission_frame_len_v1(&max_observation_frame)
                    .map_err(|_| EvidenceError::InvalidFixture)?,
            )
            .map_err(|_| EvidenceError::Bounds)?
            .checked_mul(MAX_FRAMES as u64)
            .ok_or(EvidenceError::Bounds)?,
        )
        .ok_or(EvidenceError::Bounds)?;
    Ok((max_byte_source, max_observation_source))
}
