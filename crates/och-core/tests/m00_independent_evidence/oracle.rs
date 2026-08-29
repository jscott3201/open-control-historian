//! Contract-literal calculations over the primitive fixtures.
//!
//! Expected results here are computed from written bounds and tuple rules. This
//! module intentionally has no dependency on the public model adapter.

use super::fixtures::{
    self, ConstructorCase, RawEnvelope, RawError, RawEvidence, RawGap, RawMode, RawObservation,
    RawPosition, RawRetry, RawTimestamp,
};
use core::cmp::Ordering;
use core::fmt::Write as _;

const NANOS_PER_SECOND: u32 = 1_000_000_000;
const MILLIS_PER_SECOND: i64 = 1_000;
const NANOS_PER_MILLI: u32 = 1_000_000;
const MAX_TEXT_SCALARS: usize = 4_096;
const MAX_PORTABLE_TOKEN_BYTES: usize = 256;
const MAX_CONTENT_FORMAT_BYTES: usize = 64;
const MAX_RETRY_KEY_BYTES: usize = 128;
const MAX_NATIVE_STATUS_TOKENS: usize = 16;
const MAX_OBSERVATIONS: usize = 256;
const MAX_GAPS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryClass {
    Equivalent,
    Conflict,
    Distinct,
}

impl RetryClass {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Equivalent => "Equivalent",
            Self::Conflict => "Conflict",
            Self::Distinct => "Distinct",
        }
    }
}

pub fn parse_uuid_v7(text: &str) -> Option<[u8; 16]> {
    let groups: Vec<&str> = text.split('-').collect();
    if groups.len() != 5
        || groups[0].len() != 8
        || groups[1].len() != 4
        || groups[2].len() != 4
        || groups[3].len() != 4
        || groups[4].len() != 12
    {
        return None;
    }

    let mut compact = [0_u8; 32];
    let mut compact_index = 0;
    for group in groups {
        for byte in group.bytes() {
            if !(byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) {
                return None;
            }
            compact[compact_index] = byte;
            compact_index += 1;
        }
    }

    let mut bytes = [0_u8; 16];
    let (pairs, remainder) = compact.as_chunks::<2>();
    debug_assert!(remainder.is_empty());
    for (destination, pair) in bytes.iter_mut().zip(pairs) {
        *destination = lower_hex_value(pair[0])? << 4 | lower_hex_value(pair[1])?;
    }
    valid_uuid_v7_bytes(bytes).then_some(bytes)
}

const fn lower_hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

pub const fn valid_uuid_v7_bytes(bytes: [u8; 16]) -> bool {
    bytes[6] >> 4 == 7 && bytes[8] >> 6 == 2
}

pub fn render_uuid(bytes: [u8; 16]) -> String {
    let mut text = String::with_capacity(36);
    for (index, byte) in bytes.into_iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            text.push('-');
        }
        write!(text, "{byte:02x}").expect("writing to String cannot fail");
    }
    text
}

pub fn hex_bytes(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(text, "{byte:02x}").expect("writing to String cannot fail");
    }
    text
}

pub fn valid_exact_text(text: &str) -> bool {
    text.chars().count() <= MAX_TEXT_SCALARS
}

pub fn valid_portable_token(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= MAX_PORTABLE_TOKEN_BYTES
        && text.bytes().all(|byte| (b' '..=b'~').contains(&byte))
}

pub fn valid_content_format(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= MAX_CONTENT_FORMAT_BYTES
        && text
            .bytes()
            .all(|byte| (b'!'..=b'~').contains(&byte) && !byte.is_ascii_uppercase())
}

pub fn valid_retry_key(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= MAX_RETRY_KEY_BYTES
        && text.bytes().all(|byte| (b' '..=b'~').contains(&byte))
}

pub fn parse_canonical_decimal(text: &str) -> Option<u128> {
    if text.is_empty()
        || (text.len() > 1 && text.starts_with('0'))
        || !text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    text.parse::<u128>().ok()
}

pub fn milliseconds_to_timestamp(milliseconds: i64) -> RawTimestamp {
    let mut seconds = milliseconds / MILLIS_PER_SECOND;
    let mut remainder = milliseconds % MILLIS_PER_SECOND;
    if remainder < 0 {
        seconds -= 1;
        remainder += MILLIS_PER_SECOND;
    }
    RawTimestamp::new(
        seconds,
        u32::try_from(remainder).expect("adjusted remainder is nonnegative") * NANOS_PER_MILLI,
    )
}

pub fn timestamp_to_milliseconds(timestamp: RawTimestamp) -> Result<i64, RawError> {
    if timestamp.nanoseconds >= NANOS_PER_SECOND {
        return Err(RawError::InvalidNanosecond);
    }
    if !timestamp.nanoseconds.is_multiple_of(NANOS_PER_MILLI) {
        return Err(RawError::InexactUnixMilliseconds);
    }
    let value = i128::from(timestamp.seconds) * i128::from(MILLIS_PER_SECOND)
        + i128::from(timestamp.nanoseconds / NANOS_PER_MILLI);
    i64::try_from(value).map_err(|_| RawError::UnixMillisecondsOverflow)
}

pub const fn raw_order_key(observation: &RawObservation) -> (RawTimestamp, RawTimestamp, [u8; 16]) {
    (observation.effective, observation.receive, observation.id)
}

pub fn raw_order_ids(observations: &[RawObservation]) -> Vec<[u8; 16]> {
    let mut ordered = observations.to_vec();
    ordered.sort_by_key(raw_order_key);
    ordered
        .into_iter()
        .map(|observation| observation.id)
        .collect()
}

pub const fn position_cmp(first: RawPosition, second: RawPosition) -> Ordering {
    if first.epoch < second.epoch {
        Ordering::Less
    } else if first.epoch > second.epoch {
        Ordering::Greater
    } else if first.sequence < second.sequence {
        Ordering::Less
    } else if first.sequence > second.sequence {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

pub fn envelope_violations(envelope: &RawEnvelope) -> Vec<RawError> {
    let mut violations = Vec::new();
    match &envelope.evidence {
        RawEvidence::NoChange { interval } => {
            if interval.start >= interval.end {
                push_unique(&mut violations, RawError::EmptyTimeInterval);
            }
            if envelope.mode != RawMode::ChangeOnly {
                push_unique(&mut violations, RawError::InvalidNoChangeMode);
            }
        }
        RawEvidence::Observed { observations, gaps } => {
            if observations.len() > MAX_OBSERVATIONS {
                push_unique(&mut violations, RawError::TooManyObservations);
            }
            if gaps.len() > MAX_GAPS {
                push_unique(&mut violations, RawError::TooManyGaps);
            }
            if observations.is_empty() && gaps.is_empty() {
                push_unique(&mut violations, RawError::EmptyObservedEvidence);
            }
            validate_interval_metadata(envelope.mode, observations, &mut violations);
            validate_unique_identities(observations, &mut violations);
            validate_positions(observations, &mut violations);
            validate_gaps(gaps, &mut violations);
            validate_observations_outside_gaps(observations, gaps, &mut violations);
        }
    }
    violations
}

fn validate_interval_metadata(
    mode: RawMode,
    observations: &[RawObservation],
    violations: &mut Vec<RawError>,
) {
    for observation in observations {
        if mode == RawMode::Interval && observation.interval.is_none() {
            push_unique(violations, RawError::MissingObservationInterval);
        }
        if mode != RawMode::Interval && observation.interval.is_some() {
            push_unique(violations, RawError::UnexpectedObservationInterval);
        }
    }
}

fn validate_unique_identities(observations: &[RawObservation], violations: &mut Vec<RawError>) {
    for (index, observation) in observations.iter().enumerate() {
        if observations[index + 1..]
            .iter()
            .any(|candidate| candidate.id == observation.id)
        {
            push_unique(violations, RawError::DuplicateObservationId);
        }
    }
}

fn validate_positions(observations: &[RawObservation], violations: &mut Vec<RawError>) {
    let positioned = observations
        .iter()
        .filter(|observation| observation.position.is_some())
        .count();
    if positioned != 0 && positioned != observations.len() {
        push_unique(violations, RawError::MixedProducerPositions);
        return;
    }
    if positioned == observations.len() {
        for pair in observations.windows(2) {
            let previous = pair[0].position.expect("all positions are present");
            let current = pair[1].position.expect("all positions are present");
            if position_cmp(previous, current) != Ordering::Less {
                push_unique(violations, RawError::MisorderedProducerPositions);
            }
        }
    }
}

fn validate_gaps(gaps: &[RawGap], violations: &mut Vec<RawError>) {
    for pair in gaps.windows(2) {
        let previous = pair[0];
        let current = pair[1];
        if (current.epoch, current.start) < (previous.epoch, previous.start) {
            push_unique(violations, RawError::MisorderedGaps);
        }
    }
    for (index, first) in gaps.iter().enumerate() {
        for second in &gaps[index + 1..] {
            if first.epoch == second.epoch && first.start < second.end && second.start < first.end {
                push_unique(violations, RawError::OverlappingGaps);
            }
        }
    }
}

fn validate_observations_outside_gaps(
    observations: &[RawObservation],
    gaps: &[RawGap],
    violations: &mut Vec<RawError>,
) {
    for observation in observations {
        let Some(position) = observation.position else {
            continue;
        };
        if gaps.iter().any(|gap| {
            gap.epoch == position.epoch
                && gap.start <= position.sequence
                && position.sequence < gap.end
        }) {
            push_unique(violations, RawError::ObservationInsideGap);
        }
    }
}

fn push_unique(violations: &mut Vec<RawError>, error: RawError) {
    if !violations.contains(&error) {
        violations.push(error);
    }
}

pub fn constructor_violations(case: &ConstructorCase) -> Vec<RawError> {
    let mut violations = Vec::new();
    match case {
        ConstructorCase::IdentityText(text) => {
            if parse_uuid_v7(text).is_none() {
                violations.push(RawError::InvalidIdentity);
            }
        }
        ConstructorCase::ExactText(text) => {
            if !valid_exact_text(text) {
                violations.push(RawError::InvalidExactText);
            }
        }
        ConstructorCase::PortableToken(text) => {
            if !valid_portable_token(text) {
                violations.push(RawError::InvalidPortableToken);
            }
        }
        ConstructorCase::ContentFormat(text) => {
            if !valid_content_format(text) {
                violations.push(RawError::InvalidContentFormat);
            }
        }
        ConstructorCase::RetryKey(text) => {
            if !valid_retry_key(text) {
                violations.push(RawError::InvalidRetryKey);
            }
        }
        ConstructorCase::CanonicalDecimal(text) => {
            if parse_canonical_decimal(text).is_none() {
                violations.push(RawError::InvalidCanonicalDecimal);
            }
        }
        ConstructorCase::TimestampNew(timestamp) => {
            if timestamp.nanoseconds >= NANOS_PER_SECOND {
                violations.push(RawError::InvalidNanosecond);
            }
        }
        ConstructorCase::TimestampToMilliseconds(timestamp) => {
            if let Err(error) = timestamp_to_milliseconds(*timestamp) {
                violations.push(error);
            }
        }
        ConstructorCase::TimeInterval(interval) => {
            if interval.start >= interval.end {
                violations.push(RawError::EmptyTimeInterval);
            }
        }
        ConstructorCase::Gap(gap) => {
            if gap.start >= gap.end {
                violations.push(RawError::EmptyGap);
            }
        }
        ConstructorCase::NativeStatusCount(count) => {
            if *count > MAX_NATIVE_STATUS_TOKENS {
                violations.push(RawError::TooManyNativeStatusTokens);
            }
        }
    }
    violations
}

pub fn classify_retry(first: &RawRetry, second: &RawRetry) -> RetryClass {
    if first.series != second.series || first.producer != second.producer || first.key != second.key
    {
        RetryClass::Distinct
    } else if first.content == second.content {
        RetryClass::Equivalent
    } else {
        RetryClass::Conflict
    }
}

pub fn sorted_real_bits(bits: &[u64]) -> Vec<u64> {
    let mut ordered = bits.to_vec();
    ordered.sort_unstable();
    ordered
}

pub fn render_ledger() -> String {
    let mut ledger = String::new();
    ledger.push_str(
        "och-core-m00-pr03-evidence|schema=1|scope=test-only|encoding=ascii|newline=lf\n",
    );
    ledger.push_str(
        "authority|test-only=true|wire=false|persistence=false|api-compatibility=false\n",
    );

    render_identity_and_value_rows(&mut ledger);
    render_time_order_and_collection_rows(&mut ledger);
    render_retry_and_inventory_rows(&mut ledger);
    ledger
}

fn render_identity_and_value_rows(ledger: &mut String) {
    let identity = parse_uuid_v7(fixtures::SERIES_TEXT).expect("fixture UUID is valid");
    writeln!(
        ledger,
        "case|001-identity-canonical|identity|accept|text={};bytes={};version=7;variant=rfc9562",
        render_uuid(identity),
        hex_bytes(&identity)
    )
    .expect("writing to String cannot fail");
    ledger.push_str(
        "case|002-identity-rejections|identity|reject|noncanonical=uppercase,shape;version=non-7;variant=non-rfc\n",
    );
    ledger.push_str(
        "case|003-nominal-families|identity|inventory|families=4;names=series,producer,observation,artifact;separation=compile-fail-doctest\n",
    );

    let bits = [
        0x8000_0000_0000_0000,
        0,
        0x7ff8_0000_0000_0002,
        0x7ff8_0000_0000_0001,
    ];
    let ordered_bits = sorted_real_bits(&bits);
    writeln!(
        ledger,
        "case|010-real-bits|value|exact-u64-order|ordered={};nan-payloads=distinct;signed-zero=distinct",
        ordered_bits
            .iter()
            .map(|value| format!("{value:016x}"))
            .collect::<Vec<_>>()
            .join(",")
    )
    .expect("writing to String cannot fail");
    ledger.push_str(
        "case|011-scalars|value|exact|i64-min=-9223372036854775808;i64-max=9223372036854775807;u64-max=18446744073709551615\n",
    );
    ledger.push_str(
        "case|012-text-and-tokens|value|bounded|text-scalars=0..4096;portable-bytes=1..256;format-bytes=1..64;retry-key-bytes=1..128\n",
    );
    ledger.push_str(
        "case|013-state-unavailable|value|exact|state=class+member;unavailable=reason-optional\n",
    );
    let digest: Vec<u8> = (0_u8..32).collect();
    writeln!(
        ledger,
        "case|014-external-content|value|exact|format=application/octet-stream;version={};sha256={};artifact=nominal",
        u128::MAX,
        hex_bytes(&digest)
    )
    .expect("writing to String cannot fail");
}

fn render_time_order_and_collection_rows(ledger: &mut String) {
    let minimum = milliseconds_to_timestamp(i64::MIN);
    let negative_one = milliseconds_to_timestamp(-1);
    let maximum = milliseconds_to_timestamp(i64::MAX);
    writeln!(
        ledger,
        "case|020-timestamps|time|euclidean-exact|i64-min={}:{};negative-one={}:{};zero=0:0;i64-max={}:{}",
        minimum.seconds,
        minimum.nanoseconds,
        negative_one.seconds,
        negative_one.nanoseconds,
        maximum.seconds,
        maximum.nanoseconds
    )
    .expect("writing to String cannot fail");
    ledger.push_str(
        "case|021-time-rejections|time|reject|sub-millisecond=InexactUnixMilliseconds;range=UnixMillisecondsOverflow;chronology=not-imposed\n",
    );

    ledger.push_str(
        "case|030-quality-status|quality|independent|levels=5;flags=6;status=absent,repeated,max-16,reject-17\n",
    );
    writeln!(
        ledger,
        "case|031-producer-order|position|epoch-then-sequence|u128-max={};canonical-decimal=true",
        u128::MAX
    )
    .expect("writing to String cannot fail");

    let raw_order = raw_order_ids(&fixtures::raw_order_observations());
    writeln!(
        ledger,
        "case|040-raw-order|ordering|effective-receive-id|ids={};source=excluded;producer-position=excluded",
        raw_order
            .iter()
            .map(|bytes| render_uuid(*bytes))
            .collect::<Vec<_>>()
            .join(",")
    )
    .expect("writing to String cannot fail");

    writeln!(
        ledger,
        "case|050-collection-modes|collection|closed|count={};names={}",
        fixtures::ALL_MODES.len(),
        fixtures::ALL_MODES
            .iter()
            .map(|mode| mode.name())
            .collect::<Vec<_>>()
            .join(",")
    )
    .expect("writing to String cannot fail");
    ledger.push_str(
        "case|051-evidence-shapes|collection|accept|observed=observation-only,gap-only,mixed;no-change=change-only;interval-metadata=interval-only\n",
    );
    ledger.push_str(
        "case|052-half-open|collection|end-exclusive|time=[start,end);gap=[start,end);non-empty=true\n",
    );
    let maxima = fixtures::valid_maxima();
    let RawEvidence::Observed { observations, gaps } = maxima.evidence else {
        unreachable!("maxima fixture is observed")
    };
    writeln!(
        ledger,
        "case|053-atomic-maxima|collection|accept|observations={};observation-first={};observation-last={};gaps={};gap-first=0:1;gap-last=126:127",
        observations.len(),
        render_uuid(observations.first().expect("nonempty maxima").id),
        render_uuid(observations.last().expect("nonempty maxima").id),
        gaps.len()
    )
    .expect("writing to String cannot fail");
    let negatives = fixtures::negative_envelopes();
    writeln!(
        ledger,
        "case|054-atomic-rejections|collection|one-violation-each|count={};errors={}",
        negatives.len(),
        negatives
            .iter()
            .map(|fixture| fixture.expected.name())
            .collect::<Vec<_>>()
            .join(",")
    )
    .expect("writing to String cannot fail");
    writeln!(
        ledger,
        "case|055-model-errors|constructors|sanitized|count={};variants={}",
        fixtures::ALL_ERROR_CODES.len(),
        fixtures::ALL_ERROR_CODES
            .iter()
            .map(|error| error.name())
            .collect::<Vec<_>>()
            .join(",")
    )
    .expect("writing to String cannot fail");
}

fn render_retry_and_inventory_rows(ledger: &mut String) {
    let retry = fixtures::retry_base();
    writeln!(
        ledger,
        "case|060-retry-matrix|retry|exact|same={};content-format={};content-version={};content-digest={};series={};producer={};key={}",
        classify_retry(&retry, &retry).name(),
        classify_retry(&retry, &fixtures::retry_conflicts()[0].1).name(),
        classify_retry(&retry, &fixtures::retry_conflicts()[1].1).name(),
        classify_retry(&retry, &fixtures::retry_conflicts()[2].1).name(),
        classify_retry(&retry, &fixtures::retry_distinct()[0].1).name(),
        classify_retry(&retry, &fixtures::retry_distinct()[1].1).name(),
        classify_retry(&retry, &fixtures::retry_distinct()[2].1).name()
    )
    .expect("writing to String cannot fail");
    ledger.push_str(
        "case|061-retry-redaction|retry|boolean-assertion|retry-key-debug-secret=false;qualification-debug-secret=false;golden-secret=false\n",
    );
    ledger.push_str(
        "case|070-retained-capacity|inventory|implementation-owned|oracle=false;wire=false;covered-by=existing-unit-tests\n",
    );
}
