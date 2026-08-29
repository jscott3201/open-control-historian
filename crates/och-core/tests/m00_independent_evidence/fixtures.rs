//! Primitive fixture specifications for the independent M00 evidence.
//!
//! These values deliberately contain only standard Rust primitives. They are
//! inputs to both the contract-literal oracle and the public-model adapter.

pub const SERIES_TEXT: &str = "01941f29-7c00-7000-8000-000000000001";
pub const STORE_TEXT: &str = "01941f29-7c00-7000-8000-000000000064";
pub const PRODUCER_TEXT: &str = "01941f29-7c00-7000-8000-000000000010";
pub const OBSERVATION_TEXT: &str = "01941f29-7c00-7000-8000-000000000020";
pub const ARTIFACT_TEXT: &str = "01941f29-7c00-7000-8000-000000000030";
pub const SECRET_SENTINEL: &str = "m00-pr03-secret-sentinel";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawMode {
    Sampled,
    ChangeOnly,
    Cumulative,
    Interval,
    Event,
}

pub const ALL_MODES: [RawMode; 5] = [
    RawMode::Sampled,
    RawMode::ChangeOnly,
    RawMode::Cumulative,
    RawMode::Interval,
    RawMode::Event,
];

impl RawMode {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sampled => "sampled",
            Self::ChangeOnly => "change-only",
            Self::Cumulative => "cumulative",
            Self::Interval => "interval",
            Self::Event => "event",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RawTimestamp {
    pub seconds: i64,
    pub nanoseconds: u32,
}

impl RawTimestamp {
    pub const fn new(seconds: i64, nanoseconds: u32) -> Self {
        Self {
            seconds,
            nanoseconds,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawInterval {
    pub start: RawTimestamp,
    pub end: RawTimestamp,
}

impl RawInterval {
    pub const fn seconds(start: i64, end: i64) -> Self {
        Self {
            start: RawTimestamp::new(start, 0),
            end: RawTimestamp::new(end, 0),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RawPosition {
    pub epoch: u128,
    pub sequence: u128,
}

impl RawPosition {
    pub const fn new(epoch: u128, sequence: u128) -> Self {
        Self { epoch, sequence }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawObservation {
    pub id: [u8; 16],
    pub source: Option<RawTimestamp>,
    pub receive: RawTimestamp,
    pub effective: RawTimestamp,
    pub position: Option<RawPosition>,
    pub interval: Option<RawInterval>,
}

impl RawObservation {
    pub fn valid(number: u64) -> Self {
        Self {
            id: uuid_bytes(number),
            source: None,
            receive: RawTimestamp::new(20, 0),
            effective: RawTimestamp::new(10, 0),
            position: None,
            interval: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawGap {
    pub epoch: u128,
    pub start: u128,
    pub end: u128,
}

impl RawGap {
    pub const fn new(epoch: u128, start: u128, end: u128) -> Self {
        Self { epoch, start, end }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RawEvidence {
    Observed {
        observations: Vec<RawObservation>,
        gaps: Vec<RawGap>,
    },
    NoChange {
        interval: RawInterval,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawEnvelope {
    pub mode: RawMode,
    pub evidence: RawEvidence,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RawError {
    InvalidIdentity,
    InvalidExactText,
    InvalidPortableToken,
    InvalidContentFormat,
    InvalidRetryKey,
    InvalidCanonicalDecimal,
    InvalidNanosecond,
    InexactUnixMilliseconds,
    UnixMillisecondsOverflow,
    EmptyTimeInterval,
    EmptyGap,
    TooManyNativeStatusTokens,
    TooManyObservations,
    TooManyGaps,
    EmptyObservedEvidence,
    DuplicateObservationId,
    MixedProducerPositions,
    MisorderedProducerPositions,
    MisorderedGaps,
    OverlappingGaps,
    ObservationInsideGap,
    MissingObservationInterval,
    UnexpectedObservationInterval,
    InvalidNoChangeMode,
    InvalidDeclarationReference,
    InvalidDeclarationRevision,
    RegistrySeriesCapacityExceeded,
    RegistryRevisionCapacityExceeded,
    SeriesAlreadyRegistered,
    SeriesNotFound,
    SeriesRetired,
    StaleDeclarationRevision,
    DeclarationUnchanged,
    SeriesMetadataMismatch,
    ObservationValueFamilyMismatch,
}

impl RawError {
    pub const fn name(self) -> &'static str {
        match self {
            Self::InvalidIdentity => "InvalidIdentity",
            Self::InvalidExactText => "InvalidExactText",
            Self::InvalidPortableToken => "InvalidPortableToken",
            Self::InvalidContentFormat => "InvalidContentFormat",
            Self::InvalidRetryKey => "InvalidRetryKey",
            Self::InvalidCanonicalDecimal => "InvalidCanonicalDecimal",
            Self::InvalidNanosecond => "InvalidNanosecond",
            Self::InexactUnixMilliseconds => "InexactUnixMilliseconds",
            Self::UnixMillisecondsOverflow => "UnixMillisecondsOverflow",
            Self::EmptyTimeInterval => "EmptyTimeInterval",
            Self::EmptyGap => "EmptyGap",
            Self::TooManyNativeStatusTokens => "TooManyNativeStatusTokens",
            Self::TooManyObservations => "TooManyObservations",
            Self::TooManyGaps => "TooManyGaps",
            Self::EmptyObservedEvidence => "EmptyObservedEvidence",
            Self::DuplicateObservationId => "DuplicateObservationId",
            Self::MixedProducerPositions => "MixedProducerPositions",
            Self::MisorderedProducerPositions => "MisorderedProducerPositions",
            Self::MisorderedGaps => "MisorderedGaps",
            Self::OverlappingGaps => "OverlappingGaps",
            Self::ObservationInsideGap => "ObservationInsideGap",
            Self::MissingObservationInterval => "MissingObservationInterval",
            Self::UnexpectedObservationInterval => "UnexpectedObservationInterval",
            Self::InvalidNoChangeMode => "InvalidNoChangeMode",
            Self::InvalidDeclarationReference => "InvalidDeclarationReference",
            Self::InvalidDeclarationRevision => "InvalidDeclarationRevision",
            Self::RegistrySeriesCapacityExceeded => "RegistrySeriesCapacityExceeded",
            Self::RegistryRevisionCapacityExceeded => "RegistryRevisionCapacityExceeded",
            Self::SeriesAlreadyRegistered => "SeriesAlreadyRegistered",
            Self::SeriesNotFound => "SeriesNotFound",
            Self::SeriesRetired => "SeriesRetired",
            Self::StaleDeclarationRevision => "StaleDeclarationRevision",
            Self::DeclarationUnchanged => "DeclarationUnchanged",
            Self::SeriesMetadataMismatch => "SeriesMetadataMismatch",
            Self::ObservationValueFamilyMismatch => "ObservationValueFamilyMismatch",
        }
    }
}

pub const ALL_ERROR_CODES: [RawError; 35] = [
    RawError::InvalidIdentity,
    RawError::InvalidExactText,
    RawError::InvalidPortableToken,
    RawError::InvalidContentFormat,
    RawError::InvalidRetryKey,
    RawError::InvalidCanonicalDecimal,
    RawError::InvalidNanosecond,
    RawError::InexactUnixMilliseconds,
    RawError::UnixMillisecondsOverflow,
    RawError::EmptyTimeInterval,
    RawError::EmptyGap,
    RawError::TooManyNativeStatusTokens,
    RawError::TooManyObservations,
    RawError::TooManyGaps,
    RawError::EmptyObservedEvidence,
    RawError::DuplicateObservationId,
    RawError::MixedProducerPositions,
    RawError::MisorderedProducerPositions,
    RawError::MisorderedGaps,
    RawError::OverlappingGaps,
    RawError::ObservationInsideGap,
    RawError::MissingObservationInterval,
    RawError::UnexpectedObservationInterval,
    RawError::InvalidNoChangeMode,
    RawError::InvalidDeclarationReference,
    RawError::InvalidDeclarationRevision,
    RawError::RegistrySeriesCapacityExceeded,
    RawError::RegistryRevisionCapacityExceeded,
    RawError::SeriesAlreadyRegistered,
    RawError::SeriesNotFound,
    RawError::SeriesRetired,
    RawError::StaleDeclarationRevision,
    RawError::DeclarationUnchanged,
    RawError::SeriesMetadataMismatch,
    RawError::ObservationValueFamilyMismatch,
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NegativeEnvelope {
    pub stable_id: &'static str,
    pub expected: RawError,
    pub envelope: RawEnvelope,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstructorCase {
    IdentityText(String),
    ExactText(String),
    PortableToken(String),
    ContentFormat(String),
    RetryKey(String),
    CanonicalDecimal(String),
    TimestampNew(RawTimestamp),
    TimestampToMilliseconds(RawTimestamp),
    TimeInterval(RawInterval),
    Gap(RawGap),
    NativeStatusCount(usize),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConstructorFailure {
    pub stable_id: &'static str,
    pub expected: RawError,
    pub case: ConstructorCase,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawContent {
    pub format: String,
    pub version: u128,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawRetry {
    pub series: [u8; 16],
    pub producer: [u8; 16],
    pub key: String,
    pub content: RawContent,
}

pub fn uuid_bytes(number: u64) -> [u8; 16] {
    let suffix = number.to_be_bytes();
    [
        0x01, 0x94, 0x1f, 0x29, 0x7c, 0x00, 0x70, 0x00, 0x80, 0x00, suffix[2], suffix[3],
        suffix[4], suffix[5], suffix[6], suffix[7],
    ]
}

pub fn valid_observation_only(mode: RawMode) -> RawEnvelope {
    let mut observation = RawObservation::valid(1);
    if mode == RawMode::Interval {
        observation.interval = Some(RawInterval::seconds(0, 1));
    }
    RawEnvelope {
        mode,
        evidence: RawEvidence::Observed {
            observations: vec![observation],
            gaps: Vec::new(),
        },
    }
}

pub fn valid_gap_only() -> RawEnvelope {
    RawEnvelope {
        mode: RawMode::Sampled,
        evidence: RawEvidence::Observed {
            observations: Vec::new(),
            gaps: vec![RawGap::new(1, 2, 3)],
        },
    }
}

pub fn valid_mixed() -> RawEnvelope {
    let mut observation = RawObservation::valid(1);
    observation.position = Some(RawPosition::new(1, 1));
    RawEnvelope {
        mode: RawMode::Sampled,
        evidence: RawEvidence::Observed {
            observations: vec![observation],
            gaps: vec![RawGap::new(1, 2, 3)],
        },
    }
}

pub fn valid_no_change() -> RawEnvelope {
    RawEnvelope {
        mode: RawMode::ChangeOnly,
        evidence: RawEvidence::NoChange {
            interval: RawInterval::seconds(-2, -1),
        },
    }
}

pub fn valid_maxima() -> RawEnvelope {
    let observations = (1_u64..=256).map(RawObservation::valid).collect();
    let gaps = (0_u128..64)
        .map(|number| RawGap::new(1, number * 2, number * 2 + 1))
        .collect();
    RawEnvelope {
        mode: RawMode::Sampled,
        evidence: RawEvidence::Observed { observations, gaps },
    }
}

pub fn negative_envelopes() -> Vec<NegativeEnvelope> {
    vec![
        too_many_observations(),
        too_many_gaps(),
        empty_observed(),
        duplicate_identity(),
        mixed_positions(),
        misordered_positions(),
        misordered_gaps(),
        overlapping_gaps(),
        observation_inside_gap(),
        missing_interval(),
        unexpected_interval(),
        invalid_no_change_mode(),
    ]
}

fn observed_parts(envelope: &mut RawEnvelope) -> (&mut Vec<RawObservation>, &mut Vec<RawGap>) {
    let RawEvidence::Observed { observations, gaps } = &mut envelope.evidence else {
        unreachable!("fixture builder requires observed evidence")
    };
    (observations, gaps)
}

fn too_many_observations() -> NegativeEnvelope {
    let mut envelope = valid_observation_only(RawMode::Sampled);
    let (observations, _) = observed_parts(&mut envelope);
    observations.extend((2_u64..=257).map(RawObservation::valid));
    NegativeEnvelope {
        stable_id: "atomic-too-many-observations",
        expected: RawError::TooManyObservations,
        envelope,
    }
}

fn too_many_gaps() -> NegativeEnvelope {
    let mut envelope = valid_observation_only(RawMode::Sampled);
    let (_, gaps) = observed_parts(&mut envelope);
    gaps.extend((0_u128..65).map(|number| RawGap::new(1, number * 2, number * 2 + 1)));
    NegativeEnvelope {
        stable_id: "atomic-too-many-gaps",
        expected: RawError::TooManyGaps,
        envelope,
    }
}

fn empty_observed() -> NegativeEnvelope {
    let mut envelope = valid_observation_only(RawMode::Sampled);
    let (observations, gaps) = observed_parts(&mut envelope);
    observations.clear();
    gaps.clear();
    NegativeEnvelope {
        stable_id: "atomic-empty-observed",
        expected: RawError::EmptyObservedEvidence,
        envelope,
    }
}

fn duplicate_identity() -> NegativeEnvelope {
    let mut envelope = valid_observation_only(RawMode::Sampled);
    let (observations, _) = observed_parts(&mut envelope);
    observations.push(observations[0].clone());
    NegativeEnvelope {
        stable_id: "atomic-duplicate-observation-id",
        expected: RawError::DuplicateObservationId,
        envelope,
    }
}

fn mixed_positions() -> NegativeEnvelope {
    let mut envelope = valid_observation_only(RawMode::Sampled);
    let (observations, _) = observed_parts(&mut envelope);
    observations[0].position = Some(RawPosition::new(1, 1));
    observations.push(RawObservation::valid(2));
    NegativeEnvelope {
        stable_id: "atomic-mixed-producer-positions",
        expected: RawError::MixedProducerPositions,
        envelope,
    }
}

fn misordered_positions() -> NegativeEnvelope {
    let mut envelope = valid_observation_only(RawMode::Sampled);
    let (observations, _) = observed_parts(&mut envelope);
    observations[0].position = Some(RawPosition::new(1, 2));
    let mut second = RawObservation::valid(2);
    second.position = Some(RawPosition::new(1, 1));
    observations.push(second);
    NegativeEnvelope {
        stable_id: "atomic-misordered-producer-positions",
        expected: RawError::MisorderedProducerPositions,
        envelope,
    }
}

fn misordered_gaps() -> NegativeEnvelope {
    let mut envelope = valid_gap_only();
    let (_, gaps) = observed_parts(&mut envelope);
    gaps[0] = RawGap::new(1, 10, 11);
    gaps.push(RawGap::new(1, 0, 1));
    NegativeEnvelope {
        stable_id: "atomic-misordered-gaps",
        expected: RawError::MisorderedGaps,
        envelope,
    }
}

fn overlapping_gaps() -> NegativeEnvelope {
    let mut envelope = valid_gap_only();
    let (_, gaps) = observed_parts(&mut envelope);
    gaps[0] = RawGap::new(1, 1, 4);
    gaps.push(RawGap::new(1, 3, 5));
    NegativeEnvelope {
        stable_id: "atomic-overlapping-gaps",
        expected: RawError::OverlappingGaps,
        envelope,
    }
}

fn observation_inside_gap() -> NegativeEnvelope {
    let mut envelope = valid_mixed();
    let (observations, gaps) = observed_parts(&mut envelope);
    observations[0].position = Some(RawPosition::new(1, 3));
    gaps[0] = RawGap::new(1, 3, 5);
    NegativeEnvelope {
        stable_id: "atomic-observation-inside-gap",
        expected: RawError::ObservationInsideGap,
        envelope,
    }
}

fn missing_interval() -> NegativeEnvelope {
    let mut envelope = valid_observation_only(RawMode::Interval);
    let (observations, _) = observed_parts(&mut envelope);
    observations[0].interval = None;
    NegativeEnvelope {
        stable_id: "atomic-missing-interval",
        expected: RawError::MissingObservationInterval,
        envelope,
    }
}

fn unexpected_interval() -> NegativeEnvelope {
    let mut envelope = valid_observation_only(RawMode::Sampled);
    let (observations, _) = observed_parts(&mut envelope);
    observations[0].interval = Some(RawInterval::seconds(0, 1));
    NegativeEnvelope {
        stable_id: "atomic-unexpected-interval",
        expected: RawError::UnexpectedObservationInterval,
        envelope,
    }
}

fn invalid_no_change_mode() -> NegativeEnvelope {
    let mut envelope = valid_no_change();
    envelope.mode = RawMode::Sampled;
    NegativeEnvelope {
        stable_id: "atomic-invalid-no-change-mode",
        expected: RawError::InvalidNoChangeMode,
        envelope,
    }
}

pub fn constructor_failures() -> Vec<ConstructorFailure> {
    vec![
        ConstructorFailure {
            stable_id: "constructor-invalid-identity",
            expected: RawError::InvalidIdentity,
            case: ConstructorCase::IdentityText("01941F29-7C00-7000-8000-000000000001".to_owned()),
        },
        ConstructorFailure {
            stable_id: "constructor-invalid-exact-text",
            expected: RawError::InvalidExactText,
            case: ConstructorCase::ExactText("x".repeat(4_097)),
        },
        ConstructorFailure {
            stable_id: "constructor-invalid-portable-token",
            expected: RawError::InvalidPortableToken,
            case: ConstructorCase::PortableToken(String::new()),
        },
        ConstructorFailure {
            stable_id: "constructor-invalid-content-format",
            expected: RawError::InvalidContentFormat,
            case: ConstructorCase::ContentFormat("Text/plain".to_owned()),
        },
        ConstructorFailure {
            stable_id: "constructor-invalid-retry-key",
            expected: RawError::InvalidRetryKey,
            case: ConstructorCase::RetryKey(format!("{SECRET_SENTINEL}\n")),
        },
        ConstructorFailure {
            stable_id: "constructor-invalid-canonical-decimal",
            expected: RawError::InvalidCanonicalDecimal,
            case: ConstructorCase::CanonicalDecimal("01".to_owned()),
        },
        ConstructorFailure {
            stable_id: "constructor-invalid-nanosecond",
            expected: RawError::InvalidNanosecond,
            case: ConstructorCase::TimestampNew(RawTimestamp::new(0, 1_000_000_000)),
        },
        ConstructorFailure {
            stable_id: "constructor-inexact-milliseconds",
            expected: RawError::InexactUnixMilliseconds,
            case: ConstructorCase::TimestampToMilliseconds(RawTimestamp::new(0, 1)),
        },
        ConstructorFailure {
            stable_id: "constructor-millisecond-overflow",
            expected: RawError::UnixMillisecondsOverflow,
            case: ConstructorCase::TimestampToMilliseconds(RawTimestamp::new(i64::MAX, 0)),
        },
        ConstructorFailure {
            stable_id: "constructor-empty-time-interval",
            expected: RawError::EmptyTimeInterval,
            case: ConstructorCase::TimeInterval(RawInterval::seconds(0, 0)),
        },
        ConstructorFailure {
            stable_id: "constructor-empty-gap",
            expected: RawError::EmptyGap,
            case: ConstructorCase::Gap(RawGap::new(1, 4, 4)),
        },
        ConstructorFailure {
            stable_id: "constructor-too-many-native-status-tokens",
            expected: RawError::TooManyNativeStatusTokens,
            case: ConstructorCase::NativeStatusCount(17),
        },
    ]
}

pub fn raw_order_observations() -> Vec<RawObservation> {
    let mut identity_last = RawObservation::valid(4);
    identity_last.source = Some(RawTimestamp::new(999, 0));
    identity_last.receive = RawTimestamp::new(50, 0);
    identity_last.effective = RawTimestamp::new(0, 0);
    identity_last.position = Some(RawPosition::new(99, 99));

    let mut effective_first = RawObservation::valid(3);
    effective_first.source = Some(RawTimestamp::new(-999, 0));
    effective_first.receive = RawTimestamp::new(100, 0);
    effective_first.effective = RawTimestamp::new(-1, 0);
    effective_first.position = Some(RawPosition::new(0, 0));

    let mut receive_first = RawObservation::valid(2);
    receive_first.receive = RawTimestamp::new(49, 0);
    receive_first.effective = RawTimestamp::new(0, 0);

    let mut identity_first = RawObservation::valid(1);
    identity_first.receive = RawTimestamp::new(50, 0);
    identity_first.effective = RawTimestamp::new(0, 0);

    vec![
        identity_last,
        effective_first,
        receive_first,
        identity_first,
    ]
}

pub fn raw_order_exclusion_pair() -> [RawObservation; 2] {
    let mut first = RawObservation::valid(12);
    first.source = Some(RawTimestamp::new(-1, 0));
    first.receive = RawTimestamp::new(7, 0);
    first.effective = RawTimestamp::new(6, 0);
    first.position = Some(RawPosition::new(1, 1));

    let mut second = first.clone();
    second.source = Some(RawTimestamp::new(100, 0));
    second.position = Some(RawPosition::new(8, 8));
    [first, second]
}

pub fn retry_base() -> RawRetry {
    RawRetry {
        series: uuid_bytes(1),
        producer: uuid_bytes(16),
        key: SECRET_SENTINEL.to_owned(),
        content: RawContent {
            format: "och-envelope".to_owned(),
            version: 1,
            digest: [0x11; 32],
        },
    }
}

pub fn retry_conflicts() -> Vec<(&'static str, RawRetry)> {
    let base = retry_base();
    let mut format = base.clone();
    "och-envelope-alt".clone_into(&mut format.content.format);
    let mut version = base.clone();
    version.content.version = 2;
    let mut digest = base;
    digest.content.digest = [0x22; 32];
    vec![
        ("retry-conflict-format", format),
        ("retry-conflict-version", version),
        ("retry-conflict-digest", digest),
    ]
}

pub fn retry_distinct() -> Vec<(&'static str, RawRetry)> {
    let base = retry_base();
    let mut series = base.clone();
    series.series = uuid_bytes(2);
    let mut producer = base.clone();
    producer.producer = uuid_bytes(17);
    let mut key = base;
    "different-key".clone_into(&mut key.key);
    vec![
        ("retry-distinct-series", series),
        ("retry-distinct-producer", producer),
        ("retry-distinct-key", key),
    ]
}
