# Canonical native model contract

## Authority and boundary

M00-PR02 defines the dependency-free public model in `och-core`. It is native
Historian authority rather than a serialization of a Studio, Engine, transport,
or persistence schema. Future adapters must preserve supported values exactly and
reject or report unsupported unsigned, unavailable, or collection-mode extensions
instead of truncating them.

The model does not generate identities, normalize content, hash bytes, define a
wire format, schedule work, retain retry state, persist data, answer queries, or
integrate a platform. MIT OR Apache-2.0 remains the repository license.

## Bounds and exact primitives

| Contract | Native bound |
| --- | ---: |
| Exact text | 0–4,096 Unicode scalar values |
| State class/member, native status token, unavailable reason | 1–256 printable ASCII bytes |
| Content format | 1–64 printable non-space ASCII bytes, no uppercase letters |
| Native status | absent or 1–16 ordered opaque tokens |
| Retry key | 1–128 printable ASCII bytes |
| Observations per atomic envelope | 256 |
| Gaps per atomic envelope | 64 |

Inputs are caller-owned `String` or `Vec` values. Constructors reject oversize
collections before secondary model allocation, preserve accepted content exactly,
and never include rejected input in `ModelError`.

### Identity

`SeriesId`, `ProducerId`, `ObservationId`, and `ArtifactId` are distinct nominal
families over validated RFC 9562 `UUIDv7` bytes. Parsing accepts only canonical
lowercase hyphenated text and checks version 7 plus the RFC variant. Construction
from bytes performs the same version/variant checks. There is no generation,
clock, randomness, serde, or cross-family equality.

Lexical UUID byte order is available only as the final observation-identity
tie-breaker. It is never producer order or freshness evidence.

### Values and content

`ExactValue` covers exact real bits, signed `i64`, unsigned `u64`, Boolean, state,
text, artifact reference, and explicit unavailable content. `RealBits` compares,
hashes, and orders the underlying `u64`; signed zero and NaN payloads survive and
no arithmetic is provided. Exact text is not Unicode-normalized. State always has
both a bounded class/vocabulary and bounded member.

Unavailable is a value with an optional bounded opaque reason. It is not an
absent observation, bad quality, sequence gap, or no-change assertion.

An artifact reference combines nominal `ArtifactId` with externally supplied
immutable `ContentIdentity`: lowercase format, canonical full-range `u128`
version, and exact 32-byte SHA-256 digest. The crate does not compute that digest
or define the bytes to which it applies.

## Time, quality, and independent order domains

`Timestamp` is a normalized signed Unix floor-second and nanosecond fraction in
`0..1_000_000_000`. Negative fractions use Euclidean form: Unix `-1 ms` is second
`-1`, nanosecond `999_000_000`. Every `i64` Unix millisecond converts exactly.
Conversion back rejects sub-millisecond precision and values outside `i64`.

`ObservationTimes` retains optional source, receive, and policy-effective times.
It imposes no chronology because effective time is policy evidence.

Normalized quality is one closed level—unknown, good, uncertain, bad, or
not-evaluated—plus independent stale, invalid, substituted, overridden,
out-of-service, and communication-failure flags. Ordered opaque native status is
separate; unknown and repeated tokens remain unchanged.

`ProducerEpoch` and `ProducerSequence` cover all of `u128`. Canonical decimal
parsing rejects signs, whitespace, overflow, and leading zeros except `0`.
`ProducerPosition` orders epoch then sequence and is the only producer-order
authority. The deterministic raw observation key is exactly:

1. effective time;
2. receive time;
3. observation ID.

Source time and producer position are not hidden raw-order tie-breakers.

## Collection semantics

`SeriesMetadata` immutably owns series ID, producer ID, and one closed mode:

- **sampled:** independent samples; no held value is inferred;
- **change-only:** explicit no-change is allowed, but carry is not inferred beyond
  its represented interval;
- **cumulative:** each value is an exact reading; no delta or reset is inferred;
- **interval:** every observation has one explicit non-empty half-open interval;
- **event:** occurrence evidence with no inferred hold.

A mode change requires a new or explicitly reviewed series identity; mutation is
not modeled. Observation interval metadata is required only for interval mode and
forbidden for every other mode.

`Gap` is a non-empty half-open producer-sequence range within one epoch and has a
closed sanitized reason. It says nothing about time completeness, no-change, or
value availability. `NoChange` is one non-empty half-open time interval, valid
only for change-only series, and structurally contains no observations or gaps.

`CollectionEnvelope` is the only atomic collection constructor. Observed evidence
must contain an observation or gap. Validation rejects more than 256 observations
or 64 gaps, duplicate observation IDs, mixed producer-position presence,
non-increasing positions, misordered or overlapping gaps, positioned observations
inside gaps, and mode/interval mismatches. Validated fields remain private and all
cross-item traversals are bounded and deterministic.

## Retry comparison

`RetryQualification` combines explicit series and producer scope, an opaque retry
key, and externally supplied `ContentIdentity`. Debug output redacts the key.
Comparison is closed and exact:

- same scope, key, and content: `Equivalent`;
- same scope and key but different content: `Conflict`;
- any different scope or key: `Distinct`.

The model does not derive keys or HMACs, hash retry content, set a durable horizon,
or treat transport redelivery as idempotency.

## Evidence ownership

M00-PR02 tests constructor boundaries, exact retention, ordering tuples, and
cross-item invariants. They are implementation tests, not an independent oracle.
The [M00-PR03 continuation](continuation-m00-pr03.md) owns independent oracle,
golden, and fixture-builder evidence without adding runtime or wire authority.
