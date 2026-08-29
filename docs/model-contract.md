# Canonical native model contract

## Authority and boundary

M00-PR02 defines the original dependency-free public model in `och-core`, and
M00-PR03 supplies independent evidence for that contract. M00-PR04 is its
explicit reviewed successor for store identity, bounded series declaration
revision/retirement authority, historical resolution, and registry-issued active
envelope binding. All pre-existing exact value, time, quality, order, collection,
gap/no-change, envelope, and retry semantics remain unchanged. The model is native
Historian authority rather than a serialization of a Studio, Engine, transport,
or persistence schema. Future adapters must preserve supported values exactly and
reject or report unsupported unsigned, unavailable, or collection-mode extensions
instead of truncating them.

The model does not generate identities, normalize content, hash bytes, define a
wire format, schedule work, retain retry state, persist registry state or data,
answer queries, or integrate a platform. MIT OR Apache-2.0 remains the repository
license.

## Bounds and exact primitives

| Contract | Native bound |
| --- | ---: |
| Exact text | 0–4,096 Unicode scalar values |
| State class/member, native status token, unavailable reason | 1–256 printable ASCII bytes |
| Content format | 1–64 printable non-space ASCII bytes, no uppercase letters |
| Native status | absent or 1–16 ordered opaque tokens |
| Retry key | 1–128 printable ASCII bytes |
| Declaration reference | 1–1,024 non-control Unicode scalar values |
| Observations per atomic envelope | 256 |
| Gaps per atomic envelope | 64 |
| Series and retained declaration revisions | exact finite caller-supplied registry limits |

Inputs are caller-owned `String` or `Vec` values. Constructors reject oversize
collections before secondary model allocation, preserve accepted content exactly,
and never include rejected input in `ModelError`. Every accepted caller-provided
string and vector is rebuilt through a boxed string or slice after validation;
its recorded capacity therefore equals its logical byte or item length, including
zero, and caller-controlled spare capacity is not retained.

### Identity

`StoreId`, `SeriesId`, `ProducerId`, `ObservationId`, and `ArtifactId` are distinct nominal
families over validated RFC 9562 `UUIDv7` bytes. Parsing accepts only canonical
lowercase hyphenated text and checks version 7 plus the RFC variant. Construction
from bytes performs the same version/variant checks. There is no generation,
clock, randomness, serde, or cross-family equality.

Lexical UUID byte order is available only as the final observation-identity
tie-breaker. It is never producer order or freshness evidence.

## Series declaration and lifecycle authority

`SeriesRegistry` is a caller-owned pure model scoped to one `StoreId`. Its exact
limits bound total live-plus-retired series and total retained declaration
revisions. It does not preallocate from those limits, evict, persist, recover, or
perform authorization. It is deliberately non-cloneable so an active authority
cannot be forked before one copy observes retirement; callers compare or publish
only immutable snapshots. A zero limit is valid and refuses the corresponding
first mutation. Tombstones retain their series slot permanently.

Initial registration issues per-series `DeclarationRevision` one. The registry
retains every accepted immutable `SeriesDeclaration` in ascending revision order.
An exact replay of the initial request or latest successful revision request is
idempotent; a different repeated registration, stale expected revision, or
unchanged revision payload is refused without mutation. Every accepted correction
requires the exact active predecessor and issues the next revision. The total
history bound is checked before mutation.

`SeriesBinding` contains the provider and provider-scoped source locator of one
logical point. It cannot be revised. Reusing a `SeriesId` for a different logical
point is refused; the old identity must be terminally retired and a new `SeriesId`
registered. Provider, locator, quantity, unit, and application references use one
compact exact `DeclarationReference`; it is bounded, rejects controls, and is not
normalized or interpreted.

`SeriesDeclarationPayload` revisions govern producer identity, collection mode,
usable value family, exact absent/resolved/unresolved quantity and unit evidence,
and an optional application reference. Producer and collection-mode correction
is therefore explicit and historical: it changes only the next declaration and
does not mutate an old `SeriesMetadata` or envelope. The logical provider/source
binding remains immutable. `ValueFamily` corresponds to the usable `ExactValue`
variants; `Unavailable` is explicit absence admissible for every family, not an
underlying family of its own.

Creation is the evidence on revision one; each correction retains distinct
`DeclarationEvidence`; terminal `SeriesRetirement` retains its own evidence and
the last active revision. An exact retirement retry is idempotent. Any different
retirement retry, registration, revision, or bind after retirement is refused.
Every old declaration remains resolvable for historical interpretation but no
historic declaration can authorize new evidence.

Only `SeriesRegistry::bind` constructs a `DeclaredCollectionEnvelope`. It requires
the ordinary envelope's `SeriesMetadata` to equal the current declaration's exact
series, producer, and collection mode and requires every usable observation value
to match the current value family. The wrapper holds the store, an immutable
declaration snapshot, and the original already-valid envelope. Its constructor is
private; neither an envelope nor a caller-selected revision can self-authorize.
All registry refusal paths preserve equality with the pre-call state. Registry
iteration and snapshots order series by nominal `SeriesId` and declarations by
revision.

The current `och-runtime` does not consume `SeriesRegistry` or
`DeclaredCollectionEnvelope`. Its existing volatile `SeriesMetadata` equality
check remains a read-optimization invariant only and gains no declaration,
historical, or durable-admission authority.

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

A bare `SeriesMetadata` provides no mutation operation. Outside declaration
authority, a mode change still requires a new or explicitly reviewed series
identity. M00-PR04 supplies that explicit review path through a new immutable
declaration revision and exact registry binding; historical envelopes remain
governed by their old revisions. Observation interval metadata is required only
for interval mode and forbidden for every other mode.

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
cross-item invariants. Retained-capacity behavior remains owned by those
implementation tests; it is not an oracle or wire fact.

M00-PR03 adds one dependency-free integration-test target at
`crates/och-core/tests/m00_independent_evidence.rs`. Its raw fixture builders and
contract-literal oracle are separate modules that do not import `och_core`; only
the top-level adapter constructs and calls the public model. The adapter compares
actual accessors, ordering, validation errors, and retry classifications with
primitive expected facts computed by the oracle. Twelve atomic negative builders
each prove exactly one independent violation before comparison, and a complete
35-variant inventory covers every current sanitized `ModelError`.

M00-PR04 adds `series_oracle.rs`, a primitive bounded lifecycle state machine that
does not import implementation types or constants. The top-level public adapter
runs the same registration, revision replay/refusal, binding, retirement, capacity,
and deterministic-order script against both models after every transition. It
also proves equality-preserving refusals and independently inventories every new
sanitized lifecycle error.

The checked-in `crates/och-core/tests/fixtures/m00-pr03-evidence-v1.txt` ledger has
22 stable case rows, ASCII/LF schema 1, deterministic order, canonical decimal
and lowercase hex facts, and no update or bless mechanism. Its header explicitly
denies wire, persistence, and API-compatibility authority. A freshness test
renders it from the pure oracle, while separate actual-versus-oracle tests prevent
a golden-only self-assertion. The
[M00-PR03 evidence record](continuation-m00-pr03.md) inventories the coverage and
unchanged next boundary.
