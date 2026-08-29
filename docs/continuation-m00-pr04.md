# M00-PR04 pre-M02 alignment and series-declaration authority record

## Accepted predecessor baseline

The accepted M00/M01 predecessor is the six merged narrower slices: workspace and
dependency law, the original dependency-free exact canonical model, independent
evidence for that model, caller-executor runtime lifecycle, fixed bounded volatile
ingress, and fixed bounded volatile latest publication. This record does not
retroactively claim that those slices satisfied every original roadmap exit.

In particular, the predecessor had no store identity, canonical declaration
revision, retirement, source binding, value-family declaration, quantity/unit
evidence, historical declaration resolution, or declaration-authorized envelope.
Its runtime registry remains a volatile 16-series latest-observation optimization,
not canonical lifecycle authority.

## Delivered authority transition

M00-PR04 is one dependency-free `och-core` successor. It preserves every existing
identity, exact value/content, timestamp, quality/status, producer-order,
collection-mode, gap/no-change, envelope, and retry-comparison contract while
adding:

- nominal validated `StoreId` in the existing RFC 9562 UUIDv7 family;
- compact bounded external declaration references;
- immutable logical provider/source-locator binding;
- revisioned producer, collection mode, value family, quantity/unit evidence,
  optional application reference, and transition evidence;
- one pure caller-owned store-scoped `SeriesRegistry` with explicit total-series
  and total-retained-revision limits, no cloning/forking, no eviction, and no
  unbounded input/history;
- monotonically issued per-series declarations, exact idempotent initial/latest
  retries, stale/unchanged refusal, and retained historical resolution;
- irreversible retirement with exact idempotent retry and retained tombstones;
- deterministic nominal-series and revision ordering;
- registry-only binding of an already-valid `CollectionEnvelope` to the exact
  current active declaration and usable value family.

Provider/source binding is immutable. Rebinding a different logical point requires
retirement plus a new `SeriesId`. Producer and collection mode are revisionable,
but only the registry-issued wrapper identifies which immutable declaration
governs an envelope. A bare `SeriesMetadata` or caller-selected historic revision
cannot authorize evidence.

The constructor of `DeclaredCollectionEnvelope` remains private. It contains the
issuing `StoreId`, an immutable declaration snapshot, and the original envelope.
Every typed refusal is sanitized and equality-preserving. Historic declarations
remain resolvable after correction or retirement but can never bind new evidence.

## Evidence

Focused tests cover store identity, reference/revision bounds, all usable value
families plus universal unavailable content, initial/revision idempotence, exact
expected-revision ordering, no-mutation refusals, metadata and family mismatch,
terminal retirement, historical resolution, new-identity rebinding, deterministic
snapshot ordering, and exact series/revision/tombstone capacity boundaries.

The independent evidence target adds a primitive `series_oracle` state machine
that imports no product implementation. The public adapter compares outcome and
complete normalized state after every scripted transition, including each
refusal. The existing exhaustive sanitized `ModelError` inventory and schema-v1
golden inventory now include every M00-PR04 error and the fifth nominal identity
family. Existing envelope/value/order/gap/no-change/retry tests remain unchanged,
and focused regression evidence proves bare envelopes and retry classification do
not depend on the new registry.

## Runtime and persistence boundary

`och-runtime`, Tokio use, ingress, latest publication, snapshots, receipts, and
shutdown behavior are unchanged. The runtime neither accepts
`DeclaredCollectionEnvelope` nor consults `SeriesRegistry`; it gains no lifecycle
or durable-admission authority. This slice adds no persistence, serialization,
journal, manifest, filesystem, query, adapter, Studio/Engine dependency, or new
crate dependency.

## Required M00-PR05 successor and M02 hard stop

This section records the gate as it stood at M00-PR04 acceptance. M00-PR05 now
closes it; the delivered crosswalk and remaining M02 boundary are authoritative in
[the M00-PR05 record](continuation-m00-pr05.md).

M00-PR05 must define and independently evidence the canonical source/capture and
batch provenance crosswalk against the pinned current Studio contract. It must
classify every interpretation-critical Studio observation field as canonical,
adapter-owned, or explicitly non-semantic, including provider projection/locator,
source artifact, capture lifecycle/run/snapshot/record evidence, application
resource, quantity/unit resolution, and transport-redelivery distinction.

M02-PR01 journal bytes and durable receipts are blocked until M00-PR05 is accepted.
M02 may not serialize a bare `CollectionEnvelope`, infer a declaration revision,
or silently omit source/capture evidence. M02-PR02 persistent registry/manifests
must preserve the exact declaration, revision, terminal-retirement, and historical
interpretation contract proved here.

## Explicit deferred ledger

- Non-semantic codec/decimal representation hints remain deferred; they cannot
  change interpretation of already accepted exact values.
- Retention and priority classification policy remains deferred and must not be
  inferred from declaration or collection mode.
- Persistent registry and durable retry state remain owned by M02-PR02.
- Journal frames, group commit, recovery, and durable receipts remain owned by
  M02-PR01, after M00-PR05 closes the source/capture crosswalk.
- The Studio adapter, tenant/site authorization, `SeriesKey` mapping, and its
  program resolution remain Studio-owned future integration work.
- The original broad in-memory query/rollup/building-fixture oracle remains an
  acknowledged predecessor gap requiring a named future successor.
- M01 priority handling and an explicit byte-budget admission contract remain
  acknowledged predecessor gaps; the current fixed command-count bound does not
  claim either property.

None of these gaps is silently moved into completed M00/M01 authority. This record
and live product state are tracked repository authority; the ignored `_roadmap/`
directory remains local and unpublished.
