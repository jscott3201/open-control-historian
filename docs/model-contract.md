# Canonical native model contract

## Authority and boundary

M00-PR02 defines the original dependency-free public model in `och-core`, and
M00-PR03 supplies independent evidence for that contract. M00-PR04 is its
explicit reviewed successor for store identity, bounded series declaration
revision/retirement authority, historical resolution, and registry-issued active
envelope binding. M00-PR05 is the reviewed successor for source projection,
capture/batch provenance, and the final bounded declaration-authorized canonical
admission record. M02-PR01a changes no core model: it consumes that final record
as the only runtime command input and scopes each runtime/latest view to one
explicit `StoreId`. M02-PR01b0 likewise changes no core semantics: `och-store`
encodes complete admissions into Journal V1 and decodes them only into
non-authorizing inspection evidence. M02-PR01b1 changes no core semantics either:
it makes that exact byte format the sole runtime active-journal path and adds
mechanical durable cutoff evidence around it. M02-PR02a again leaves core
unchanged while persisting the complete bounded `SeriesRegistry` snapshot and
making one manifest the outer committed cutoff. M02-PR02b also leaves core
unchanged: `och-store` persists the exact core `RetryQualification` comparison
inputs inside a bounded two-tier runtime persistence policy. All pre-existing exact value,
time, quality, order, collection, gap/no-change, envelope, and retry semantics
remain unchanged. The model is native
Historian authority rather than a serialization of a Studio, Engine, transport,
or persistence schema. Future adapters must preserve supported values exactly and
reject or report unsupported unsigned, unavailable, or collection-mode extensions
instead of truncating them.

The core model does not generate identities, normalize content, hash bytes,
define a wire format, schedule work, select a retry horizon, persist itself, answer
queries, or integrate a platform. `och-store` persists a snapshot only by replay
through these public core semantics and exact comparison. MIT OR Apache-2.0
remains the repository license.

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
| Source observation contexts per admission | exact envelope count, at most 256 |
| Source gap contexts per admission | exact envelope count, at most 64 |
| Series and retained declaration revisions | exact finite caller-supplied registry limits |

Inputs are caller-owned `String` or `Vec` values. Constructors reject oversize
collections before secondary model allocation, preserve accepted content exactly,
and never include rejected input in `ModelError`. Every accepted caller-provided
string and vector is rebuilt through a boxed string or slice after validation;
its recorded capacity therefore equals its logical byte or item length, including
zero, and caller-controlled spare capacity is not retained.

### Identity

`StoreId`, `SeriesId`, `ProducerId`, `ObservationId`, `ArtifactId`, and `EvidenceId`
are distinct nominal families over validated RFC 9562 `UUIDv7` bytes. `EvidenceId`
is intentionally one family shared by source system, endpoint, run, snapshot,
source observation, raw record, and normalized record roles; containing structs
preserve those roles. Parsing accepts only canonical
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

`SeriesBinding` contains the provider, optional opaque source projection, and
provider-scoped source locator of one logical point. Its two-argument
`SourceReference` constructor retains a projection-absent PR04 declaration, but
canonical source admission requires the projection-bearing constructor. It cannot
be revised. Reusing a `SeriesId` for a different logical
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

`och-runtime` admits only complete `CanonicalAdmission` values. Its sole blocking
store writer owns one non-cloneable live `SeriesRegistry` and exposes bounded
register, revise, retire, and current-active bind operations through the same
ordering gate as append publication. Runtime code does not reinterpret lifecycle
semantics or expose a mutable registry. Its volatile `SeriesMetadata` equality
check remains a read-optimization invariant only and gains no declaration,
historical, or durable-admission authority.

## Source/capture provenance and canonical admission

`SourceSchemaIdentity` and non-zero `SourceSchemaVersion` retain a bounded native
schema reference without importing Studio serialization. `SourceIntervalKind` is
only `Observed` or `NoChange` and carries no timestamp; the real no-change
`TimeInterval` remains in `CollectionEnvelope`. Studio V1 maps losslessly to
version one, while JSON names, tags, casing, and codec choices are non-semantic.

`CaptureLifecycle` validates and retains system → endpoint → capture run →
snapshot links. System evidence holds the exact declaration provider/projection,
endpoint evidence holds the exact locator, run evidence holds start and optional
completion (`completion >= start`), and snapshot evidence holds the exact
`ArtifactReference`. `Timestamp` represents Studio Unix milliseconds losslessly.
Every lifecycle and record role uses the shared nominal `EvidenceId`; identities
must be unique within one admission.

Every observed envelope observation has exactly one ordered
`SourceObservationContext` and linked raw/normalized record pair. Each context
explicitly names the canonical `ObservationId`, which must equal the envelope
observation at the same position before retention; source `EvidenceId` remains a
distinct identity and is never inferred from it. Before retaining the compact
lineage, admission validates provider/projection/locator, optional
application reference, and exact absent/resolved/unresolved quantity and unit
evidence against the governing declaration. It retains the original `u8` source
record ordinal, source observation identity, optional distinct provenance
artifact, new/redelivered transport evidence, optional observation source
idempotency, raw identity/snapshot link/artifact/optional idempotency, and
normalized identity/content/raw and observation links. Raw idempotency content
must equal raw artifact content. Source identity is never inferred from
`ObservationId`.

Observed gaps have exactly one ordered `SourceGapEvidence` for each canonical
gap, with the same epoch and half-open producer range plus an independent closed
source reason. This source reason is not coerced into `GapReason`. No-change
admission structurally holds no observations, gaps, or record lineages while
retaining schema and the shared capture lifecycle.

Only `CanonicalAdmission::observed` and `CanonicalAdmission::no_change` create the
final record. They consume one non-cloneable registry-issued
`DeclaredCollectionEnvelope`, verify exact request retry series/producer scope,
and retain the declaration snapshot, original envelope, `RetryQualification`,
batch metadata, lifecycle, and closed evidence. Authorization happens when the
active registry issues the consumed binding; correction or retirement cannot
issue another binding for an old declaration. The final admission is cloneable
immutable evidence. Source transport redelivery and both source idempotency
records remain evidence only and are never derived from, equated with, or used to
classify `RetryQualification`.

`CanonicalAdmission` is the exact native semantic input accepted by the
store-scoped runtime and the only semantic record Journal V1 encodes.
`IngressCommand` adds no bypass constructor or second validation authority: it
owns one admission, retry coalescing reads `admission.retry()`, and volatile
publication reads `admission.envelope()`. The runtime rejects a foreign StoreId
after closed-state precedence and before retry/capacity without mutating slots or
latest state. The runtime adds only resource policy, append/publication staging,
and mechanical handled/durable evidence; it cannot reinterpret the admission or
turn decoded reopen evidence back into authorization. A future adapter may split
one Studio batch into per-series admissions by copying the shared lifecycle and
preserving original ordinals; cross-series atomicity and active binding
uniqueness are deliberately absent.

## Journal V1 semantic framing

`och-store` consumes only an already-authorized `CanonicalAdmission`. It writes
every publicly reachable canonical field without inference, normalization,
generated identity, compression, dictionary substitution, or unstable hashing.
The exact fixed header, frame prefix, payload grammar, canonical big-endian byte
order, string/count prefixes, 8 MiB payload ceiling, and CRC-32C parameters are
specified in [Journal V1](journal-v1-format.md).

Decode treats all bytes as hostile. The declared payload length must fit both
the hard ceiling and `DecodeLimitsV1` before strings or vectors are allocated;
each inner count and string length is checked before its allocation. Unknown
magic, version, kind, flag, tag, impossible count/length, invalid primitive,
cross-field mismatch, duplicate evidence identity, truncation, trailing bytes,
and checksum failure are closed sanitized errors.

`DecodedAdmissionV1` retains the complete declaration snapshot, envelope,
retry evidence, source batch/lifecycle, lineages, gaps, and append sequence for
inspection and deterministic re-encoding. It deliberately cannot construct or
convert into `CanonicalAdmission`, bind new evidence, resolve missing declaration
history, mutate a registry, or be submitted to runtime.

M02-PR01b1 places those frames in one fixed generation-one active journal paired
with a two-slot mechanical checkpoint. M02-PR02a adds a stable store lock, a
header-v2 compatibility fence in the unchanged 28-byte layout, two manifest
slots, and three complete registry snapshot slots. M02-PR02b adds Manifest V2
within the same 128-byte slots and three reusable Retry State V1 slots.
Admission-frame bytes remain
Journal V1. The runtime computes the exact frame size
without allocating, reserves count and bytes atomically, then allocates and
encodes under that retained reservation. The sole blocking writer assigns append
sequence and validates frame and declaration StoreId against the journal header.
Durable order is append, volatile publication, journal sync, alternate checkpoint
write, checkpoint sync, retry snapshot publication, Manifest V2 publication
naming that exact cutoff and the current registry/retry snapshots, then one
atomic runtime projection/receipt transition and waiter wake. The checkpoint
retains only store and journal identity, slot generation, append sequence, end
offset, and CRC. The
public cutoff exposes slot generation separately from journal generation. Two
valid consecutive slots must advance append sequence and end offset strictly; a
recomputed checksum cannot legitimize a non-progressing cutoff. The checkpoint is
neither registry nor retry authority.

Manifest open bounds the exact non-recursive inventory, selects only strict
manifest candidates, restores the referenced registry solely by public
register/revise/retire replay, and requires exact snapshot equality. Every
recovered declaration must resolve historically. A nonempty premanifest V1 or V2
store requires an explicit matching snapshot; an exact header-only store may
bootstrap empty. New binding uses the current active registry, while append
requires exact equality with `resolve(series, revision)` and therefore preserves
already-issued historical evidence after correction or retirement. Returned
reopen records remain `DecodedAdmissionV1` inspection evidence and do not rebuild
volatile latest. A referenced Retry State V1 snapshot, rather than decoded
journal history, restores the bounded completed-retry projection. A legacy
Manifest V1 restores empty retry tiers and deliberately receives no backfill.
Persistence remains bounded to this active generation and manifest cutoff;
rotation, immutable history, broad recovery, latest projection, and an unbounded
or time-based durable retry horizon remain absent. Exact bytes and publication
law are specified in [Manifest V1/V2](manifest-v1-format.md) and
[Retry State V1](retry-state-v1-format.md).

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

M02-PR02b applies that exact classification inside a store-owned, finite
persistence policy without changing the core type. The replay tier retains the
original append identity and exact first manifest commit; an equivalent request
returns that outcome immediately without current/latest-state publication. FIFO
overflow moves the oldest replay entry to a non-replayable guard. Equivalent
guard hits return `RetryExpired`, while changed content in either tier remains a
conflict. Only eviction from both tiers makes the scope/key fresh. Ordering is
durable append sequence only, and replay, conflict, or expired hits never refresh
it. The sole blocking store writer mutates this projection; runtime ingress holds
only the immutable committed snapshot installed atomically with the receipts it
covers.

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
57-variant inventory covers every current sanitized `ModelError`.

M00-PR04 adds `series_oracle.rs`, a primitive bounded lifecycle state machine that
does not import implementation types or constants. The top-level public adapter
runs the same registration, revision replay/refusal, binding, retirement, capacity,
and deterministic-order script against both models after every transition. It
also proves equality-preserving refusals and independently inventories every new
sanitized lifecycle error.

M00-PR05 adds `source_oracle.rs`, which represents the source/capture crosswalk
using standard-library primitives only. It independently validates schema, links,
source binding, scope, counts, ordinals, unique evidence IDs, record links,
idempotency content, interpretation evidence, and gaps. The top-level public
adapter constructs the corresponding real `CanonicalAdmission` and compares a
complete normalized retained-state record for exact structural equality; focused
public tests separately exercise refusals and exact bounds. The exhaustive error
map and nominal identity inventory include every M00-PR05 addition.

The checked-in `crates/och-core/tests/fixtures/m00-pr03-evidence-v1.txt` ledger has
22 stable case rows, ASCII/LF schema 1, deterministic order, canonical decimal
and lowercase hex facts, and no update or bless mechanism. Its header explicitly
denies wire, persistence, and API-compatibility authority. A freshness test
renders it from the pure oracle, while separate actual-versus-oracle tests prevent
a golden-only self-assertion. The
[M00-PR03 evidence record](continuation-m00-pr03.md) inventories the coverage and
unchanged next boundary.
