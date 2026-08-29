//! Primitive-only oracle for the M00-PR05 Studio source/capture crosswalk.
//!
//! This module deliberately imports no `och_core` types or implementation.

use super::fixtures::RawError;
use std::collections::BTreeSet;

pub const SOURCE_ERROR_INVENTORY: [RawError; 21] = [
    RawError::InvalidSourceSchemaVersion,
    RawError::CaptureRunTimeOrder,
    RawError::SourceEndpointSystemMismatch,
    RawError::CaptureRunEndpointMismatch,
    RawError::SourceSnapshotRunMismatch,
    RawError::SourceProjectionRequired,
    RawError::SourceIntervalMismatch,
    RawError::SourceLifecycleBindingMismatch,
    RawError::AdmissionRetryScopeMismatch,
    RawError::TooManySourceObservationContexts,
    RawError::SourceObservationCountMismatch,
    RawError::TooManySourceGapContexts,
    RawError::SourceGapCountMismatch,
    RawError::MisorderedSourceRecordOrdinals,
    RawError::DuplicateSourceEvidenceId,
    RawError::SourceRawSnapshotMismatch,
    RawError::SourceNormalizedRawMismatch,
    RawError::SourceNormalizedObservationMismatch,
    RawError::SourceRawIdempotencyMismatch,
    RawError::SourceInterpretationMismatch,
    RawError::SourceGapMismatch,
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawLifecycle {
    pub ids: [u16; 4],
    pub endpoint_system: u16,
    pub run_endpoint: u16,
    pub snapshot_run: u16,
    pub started_ms: i64,
    pub completed_ms: Option<i64>,
    pub provider: &'static str,
    pub projection: Option<&'static str>,
    pub locator: &'static str,
    pub snapshot_artifact: (u16, &'static str, u128, [u8; 32]),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawLineage {
    pub ordinal: u16,
    pub source_observation: u16,
    pub provenance_artifact: Option<(u16, &'static str, u128, [u8; 32])>,
    pub redelivered: bool,
    pub observation_idempotency: Option<(&'static str, &'static str, u128, [u8; 32])>,
    pub raw: u16,
    pub raw_snapshot: u16,
    pub raw_artifact: (u16, &'static str, u128, [u8; 32]),
    pub raw_idempotency: Option<(&'static str, &'static str, u128, [u8; 32])>,
    pub normalized: u16,
    pub normalized_raw: u16,
    pub normalized_content: (&'static str, u128, [u8; 32]),
    pub normalized_observation: u16,
    pub provider: &'static str,
    pub projection: &'static str,
    pub locator: &'static str,
    pub application: Option<&'static str>,
    pub quantity: Option<Result<&'static str, &'static str>>,
    pub unit: Option<Result<&'static str, &'static str>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawGapContext {
    pub epoch: u128,
    pub start: u128,
    pub end: u128,
    pub reason: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawAdmission {
    pub store: u16,
    pub declaration_revision: u128,
    pub schema: &'static str,
    pub schema_version: u128,
    pub interval_observed: bool,
    pub envelope_observed: bool,
    pub series: u16,
    pub producer: u16,
    pub mode: &'static str,
    pub family: &'static str,
    pub retry_series: u16,
    pub retry_producer: u16,
    pub retry_key: &'static str,
    pub retry_content: (&'static str, u128, [u8; 32]),
    pub declaration_provider: &'static str,
    pub declaration_projection: Option<&'static str>,
    pub declaration_locator: &'static str,
    pub declaration_application: Option<&'static str>,
    pub declaration_quantity: Option<Result<&'static str, &'static str>>,
    pub declaration_unit: Option<Result<&'static str, &'static str>>,
    pub lifecycle: RawLifecycle,
    pub envelope_observation_count: usize,
    pub lineages: Vec<RawLineage>,
    pub canonical_gaps: Vec<(u128, u128, u128)>,
    pub gaps: Vec<RawGapContext>,
}

pub fn valid_observed() -> RawAdmission {
    let digest = [7; 32];
    RawAdmission {
        store: 100,
        declaration_revision: 1,
        schema: "studio.source-batch",
        schema_version: 1,
        interval_observed: true,
        envelope_observed: true,
        series: 1,
        producer: 2,
        mode: "sampled",
        family: "boolean",
        retry_series: 1,
        retry_producer: 2,
        retry_key: "historian-key",
        retry_content: ("application/octet-stream", 1, [9; 32]),
        declaration_provider: "provider",
        declaration_projection: Some("Mqtt"),
        declaration_locator: "locator",
        declaration_application: Some("application"),
        declaration_quantity: Some(Ok("temperature")),
        declaration_unit: Some(Ok("deg-c")),
        lifecycle: RawLifecycle {
            ids: [10, 11, 12, 13],
            endpoint_system: 10,
            run_endpoint: 11,
            snapshot_run: 12,
            started_ms: -1,
            completed_ms: Some(1),
            provider: "provider",
            projection: Some("Mqtt"),
            locator: "locator",
            snapshot_artifact: (20, "application/json", 1, [6; 32]),
        },
        envelope_observation_count: 1,
        lineages: vec![RawLineage {
            ordinal: 0,
            source_observation: 30,
            provenance_artifact: Some((21, "application/octet-stream", 1, [5; 32])),
            redelivered: true,
            observation_idempotency: Some((
                "observation-key",
                "application/octet-stream",
                1,
                [4; 32],
            )),
            raw: 31,
            raw_snapshot: 13,
            raw_artifact: (22, "application/octet-stream", 1, digest),
            raw_idempotency: Some(("raw-key", "application/octet-stream", 1, digest)),
            normalized: 32,
            normalized_raw: 31,
            normalized_content: ("application/octet-stream", 1, [8; 32]),
            normalized_observation: 30,
            provider: "provider",
            projection: "Mqtt",
            locator: "locator",
            application: Some("application"),
            quantity: Some(Ok("temperature")),
            unit: Some(Ok("deg-c")),
        }],
        canonical_gaps: vec![(1, 4, 5)],
        gaps: vec![RawGapContext {
            epoch: 1,
            start: 4,
            end: 5,
            reason: 0,
        }],
    }
}

#[allow(clippy::too_many_lines)]
pub fn violations(raw: &RawAdmission) -> Vec<RawError> {
    let mut errors = Vec::new();
    if raw.schema_version == 0 {
        errors.push(RawError::InvalidSourceSchemaVersion);
    }
    if raw
        .lifecycle
        .completed_ms
        .is_some_and(|end| end < raw.lifecycle.started_ms)
    {
        errors.push(RawError::CaptureRunTimeOrder);
    }
    if raw.lifecycle.endpoint_system != raw.lifecycle.ids[0] {
        errors.push(RawError::SourceEndpointSystemMismatch);
    }
    if raw.lifecycle.run_endpoint != raw.lifecycle.ids[1] {
        errors.push(RawError::CaptureRunEndpointMismatch);
    }
    if raw.lifecycle.snapshot_run != raw.lifecycle.ids[2] {
        errors.push(RawError::SourceSnapshotRunMismatch);
    }
    if raw.declaration_projection.is_none() {
        errors.push(RawError::SourceProjectionRequired);
    }
    if raw.interval_observed != raw.envelope_observed {
        errors.push(RawError::SourceIntervalMismatch);
    }
    if raw.lifecycle.provider != raw.declaration_provider
        || raw.lifecycle.projection != raw.declaration_projection
        || raw.lifecycle.locator != raw.declaration_locator
    {
        errors.push(RawError::SourceLifecycleBindingMismatch);
    }
    if raw.retry_series != raw.series || raw.retry_producer != raw.producer {
        errors.push(RawError::AdmissionRetryScopeMismatch);
    }
    if raw.lineages.len() > 256 {
        errors.push(RawError::TooManySourceObservationContexts);
    } else if raw.lineages.len() != raw.envelope_observation_count {
        errors.push(RawError::SourceObservationCountMismatch);
    }
    if raw.gaps.len() > 64 {
        errors.push(RawError::TooManySourceGapContexts);
    } else if raw.gaps.len() != raw.canonical_gaps.len() {
        errors.push(RawError::SourceGapCountMismatch);
    }
    let mut ids = raw.lifecycle.ids.into_iter().collect::<BTreeSet<_>>();
    if ids.len() != 4 {
        errors.push(RawError::DuplicateSourceEvidenceId);
    }
    let mut previous = None;
    for lineage in &raw.lineages {
        if lineage.ordinal > 255 || previous.is_some_and(|value| value >= lineage.ordinal) {
            errors.push(RawError::MisorderedSourceRecordOrdinals);
        }
        previous = Some(lineage.ordinal);
        for id in [lineage.source_observation, lineage.raw, lineage.normalized] {
            if !ids.insert(id) && !errors.contains(&RawError::DuplicateSourceEvidenceId) {
                errors.push(RawError::DuplicateSourceEvidenceId);
            }
        }
        if lineage.raw_snapshot != raw.lifecycle.ids[3] {
            errors.push(RawError::SourceRawSnapshotMismatch);
        }
        if lineage.normalized_raw != lineage.raw {
            errors.push(RawError::SourceNormalizedRawMismatch);
        }
        if lineage.normalized_observation != lineage.source_observation {
            errors.push(RawError::SourceNormalizedObservationMismatch);
        }
        if lineage
            .raw_idempotency
            .is_some_and(|(_, format, version, digest)| {
                (format, version, digest)
                    != (
                        lineage.raw_artifact.1,
                        lineage.raw_artifact.2,
                        lineage.raw_artifact.3,
                    )
            })
        {
            errors.push(RawError::SourceRawIdempotencyMismatch);
        }
        if lineage.provider != raw.declaration_provider
            || Some(lineage.projection) != raw.declaration_projection
            || lineage.locator != raw.declaration_locator
            || lineage.application != raw.declaration_application
            || lineage.quantity != raw.declaration_quantity
            || lineage.unit != raw.declaration_unit
        {
            errors.push(RawError::SourceInterpretationMismatch);
        }
    }
    if raw
        .canonical_gaps
        .iter()
        .zip(&raw.gaps)
        .any(|(canonical, source)| *canonical != (source.epoch, source.start, source.end))
    {
        errors.push(RawError::SourceGapMismatch);
    }
    errors
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedContent {
    pub format: String,
    pub version: u128,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedArtifact {
    pub id: u16,
    pub content: NormalizedContent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedIdempotency {
    pub key: String,
    pub content: NormalizedContent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedLifecycle {
    pub ids: [u16; 4],
    pub provider: String,
    pub projection: String,
    pub locator: String,
    pub started_ms: i64,
    pub completed_ms: Option<i64>,
    pub snapshot: NormalizedArtifact,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedLineage {
    pub ordinal: u8,
    pub source_observation: u16,
    pub provenance_artifact: Option<NormalizedArtifact>,
    pub redelivered: bool,
    pub observation_idempotency: Option<NormalizedIdempotency>,
    pub raw: u16,
    pub raw_snapshot: u16,
    pub raw_artifact: NormalizedArtifact,
    pub raw_idempotency: Option<NormalizedIdempotency>,
    pub normalized: u16,
    pub normalized_raw: u16,
    pub normalized_content: NormalizedContent,
    pub normalized_observation: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedGap {
    pub epoch: u128,
    pub start: u128,
    pub end: u128,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedAdmission {
    pub store: u16,
    pub declaration_revision: u128,
    pub series: u16,
    pub producer: u16,
    pub mode: String,
    pub family: String,
    pub provider: String,
    pub projection: String,
    pub locator: String,
    pub application: Option<String>,
    pub quantity: String,
    pub unit: String,
    pub schema: String,
    pub schema_version: u128,
    pub observed: bool,
    pub lifecycle: NormalizedLifecycle,
    pub retry_series: u16,
    pub retry_producer: u16,
    pub retry_key: String,
    pub retry_content: NormalizedContent,
    pub envelope_observation_count: usize,
    pub canonical_gaps: Vec<(u128, u128, u128)>,
    pub lineages: Vec<NormalizedLineage>,
    pub gaps: Vec<NormalizedGap>,
}

fn content(raw: (&str, u128, [u8; 32])) -> NormalizedContent {
    NormalizedContent {
        format: raw.0.to_owned(),
        version: raw.1,
        digest: raw.2,
    }
}

fn artifact(raw: (u16, &str, u128, [u8; 32])) -> NormalizedArtifact {
    NormalizedArtifact {
        id: raw.0,
        content: content((raw.1, raw.2, raw.3)),
    }
}

fn idempotency(raw: (&str, &str, u128, [u8; 32])) -> NormalizedIdempotency {
    NormalizedIdempotency {
        key: raw.0.to_owned(),
        content: content((raw.1, raw.2, raw.3)),
    }
}

fn tri_state(raw: Option<Result<&str, &str>>) -> String {
    match raw {
        None => "absent".to_owned(),
        Some(Ok(value)) => format!("resolved:{value}"),
        Some(Err(value)) => format!("unresolved:{value}"),
    }
}

pub fn expected_retained(raw: &RawAdmission) -> NormalizedAdmission {
    NormalizedAdmission {
        store: raw.store,
        declaration_revision: raw.declaration_revision,
        series: raw.series,
        producer: raw.producer,
        mode: raw.mode.to_owned(),
        family: raw.family.to_owned(),
        provider: raw.declaration_provider.to_owned(),
        projection: raw
            .declaration_projection
            .expect("valid fixture projection")
            .to_owned(),
        locator: raw.declaration_locator.to_owned(),
        application: raw.declaration_application.map(str::to_owned),
        quantity: tri_state(raw.declaration_quantity),
        unit: tri_state(raw.declaration_unit),
        schema: raw.schema.to_owned(),
        schema_version: raw.schema_version,
        observed: raw.interval_observed,
        lifecycle: NormalizedLifecycle {
            ids: raw.lifecycle.ids,
            provider: raw.lifecycle.provider.to_owned(),
            projection: raw
                .lifecycle
                .projection
                .expect("valid fixture projection")
                .to_owned(),
            locator: raw.lifecycle.locator.to_owned(),
            started_ms: raw.lifecycle.started_ms,
            completed_ms: raw.lifecycle.completed_ms,
            snapshot: artifact(raw.lifecycle.snapshot_artifact),
        },
        retry_series: raw.retry_series,
        retry_producer: raw.retry_producer,
        retry_key: raw.retry_key.to_owned(),
        retry_content: content(raw.retry_content),
        envelope_observation_count: raw.envelope_observation_count,
        canonical_gaps: raw.canonical_gaps.clone(),
        lineages: raw
            .lineages
            .iter()
            .map(|lineage| NormalizedLineage {
                ordinal: u8::try_from(lineage.ordinal).expect("valid fixture ordinal"),
                source_observation: lineage.source_observation,
                provenance_artifact: lineage.provenance_artifact.map(artifact),
                redelivered: lineage.redelivered,
                observation_idempotency: lineage.observation_idempotency.map(idempotency),
                raw: lineage.raw,
                raw_snapshot: lineage.raw_snapshot,
                raw_artifact: artifact(lineage.raw_artifact),
                raw_idempotency: lineage.raw_idempotency.map(idempotency),
                normalized: lineage.normalized,
                normalized_raw: lineage.normalized_raw,
                normalized_content: content(lineage.normalized_content),
                normalized_observation: lineage.normalized_observation,
            })
            .collect(),
        gaps: raw
            .gaps
            .iter()
            .map(|gap| NormalizedGap {
                epoch: gap.epoch,
                start: gap.start,
                end: gap.end,
                reason: match gap.reason {
                    0 => "communication-failure",
                    1 => "source-unavailable",
                    2 => "producer-reset",
                    3 => "filtered",
                    _ => "unknown",
                }
                .to_owned(),
            })
            .collect(),
    }
}
