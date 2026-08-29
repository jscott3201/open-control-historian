# M00-PR05 source/capture crosswalk and canonical-admission record

## Authority and pinned input

M00-PR05 is the second accepted pre-M02 successor after M00-PR04. It preserves
the accepted six-slice M00/M01 predecessor baseline and the complete M00-PR04
declaration lifecycle. This record crosswalks Open Control Studio
`main@e629620d7f1104197755b4b6d2566ac9a1286a4f`; it does not import Studio code,
types, serialization, or dependencies.

M00-PR04 remains the only declaration/revision/retirement authority.
`och-runtime` remains a volatile non-consumer of the registry,
`DeclaredCollectionEnvelope`, and `CanonicalAdmission`; it gains no lifecycle,
source-provenance, or durable-admission authority.

## Delivered native contract

`SourceReference` now has an optional bounded opaque projection component. The
two-argument projection-absent constructor remains for existing PR04 callers,
while the projection-bearing constructor is required for canonical admission.
The native type deliberately does not freeze Studio's current `File`, `Bacnet`,
`Modbus`, `Haystack`, and `Mqtt` variants; tests prove each maps losslessly to a
bounded reference and that a future value can do the same. Changing provider,
projection, or locator changes the logical binding and therefore requires
terminal retirement plus a new `SeriesId`.

One nominal validated `EvidenceId` family represents Studio `EvidenceRef` and
`EvidenceId` for system, endpoint, run, snapshot, raw record, normalized record,
and source-observation evidence. Role-specific structs preserve roles and exact
links without fabricating distinct UUID families. Existing `ArtifactReference`
and `ContentIdentity` represent Studio artifact identity plus
format/version/SHA-256 content exactly. Existing `Timestamp` losslessly represents
Unix milliseconds; capture completion remains optional and cannot precede start.

`SourceSchemaIdentity` plus non-zero `SourceSchemaVersion` retain the batch schema.
`SourceIntervalKind::{Observed, NoChange}` contains no timestamp payload and must
match the bound `CollectionEnvelope`; canonical `NoChange` continues to own its
real half-open `TimeInterval`.

`CaptureLifecycle` retains and validates:

- system evidence identity plus exact provider/projection;
- endpoint evidence identity, system link, and exact locator;
- capture-run evidence identity, endpoint link, start, and optional completion;
- snapshot evidence identity, run link, and exact snapshot `ArtifactReference`.

For every canonical observation, observed admission requires one ordered source
context that explicitly names the canonical `ObservationId` plus one linked
raw/normalized pair. The named identity must equal the envelope observation at
the same position, so counts and ordinals cannot associate swapped or unrelated
lineage. Admission also validates transient source,
application, quantity, and unit interpretation against the exact governing
declaration before removing that duplication. The retained lineage includes:

- exact canonical `ObservationId` association and original `0..=255` source
  record ordinal;
- source-observation `EvidenceId`, optional distinct provenance
  `ArtifactReference`, `New`/`Redelivered`, and optional source idempotency
  `{RetryKey, ContentIdentity}`;
- raw-record `EvidenceId`, snapshot link, `ArtifactReference`, and optional raw
  idempotency whose content must equal the raw artifact content;
- normalized-record `EvidenceId`, raw link, `ContentIdentity`, and source
  observation-evidence link.

Source observation evidence identity is never inferred from Historian
`ObservationId`. Transport redelivery and both source idempotency records remain
independent source evidence; none is derived from or equated with
`RetryQualification`.

Observed source gaps are exactly one-for-one and in order with the canonical
gaps. They retain the exact producer epoch/range plus one closed source reason:
communication failure, source unavailable, producer reset, filtered, or unknown.
That source reason is not lossily mapped onto the existing canonical `GapReason`.
Gaps inherit the shared lifecycle and fabricate no raw or normalized record.
No-change structurally retains zero observations, gaps, and lineages while still
retaining schema and the complete capture lifecycle.

All lifecycle and per-record `EvidenceId` values must be unique within one
admission. Counts, exact envelope-order `ObservationId` association, fixed maxima
(256 observation contexts and 64 gap contexts), strict ordinal order,
lifecycle/record links, raw-idempotency content, declaration interpretation,
interval class, gap ranges, and retry series/producer scope are validated before
compact retention. Every refusal returns one closed sanitized
`ModelError`, creates no admission, and does not mutate the registry or immutable
declaration snapshot.

Only a registry-issued `DeclaredCollectionEnvelope` can enter
`CanonicalAdmission`. The issued capability is non-cloneable and is consumed once;
the final admission is cloneable immutable evidence. Binding is the authorization
event: the registry cannot issue an old revision or issue any binding after
retirement. A capability already issued while active retains its exact historical
declaration when it is later consumed, so correction cannot reinterpret the
already-authorized envelope.

## Exact Studio field classification

| Studio field | Historian ownership |
| --- | --- |
| `SourceObservation.evidence` | Retained shared-family `EvidenceId` |
| optional `ResourceRef` | Exact match to declaration application reference, then duplicate omitted |
| provider/projection/locator | Exact immutable declaration binding; projection required |
| optional source artifact | Retained distinct `ArtifactReference` |
| value/native status/quality/times/position | Existing exact `Observation` |
| quantity/unit | Exact declaration tri-state match, then duplicate omitted |
| redelivery and optional source idempotency | Retained independently from Historian retry |
| batch schema V1 | Bounded schema identity plus version one |
| batch interval | Closed observed/no-change classification; no invented timestamp |
| system/endpoint/run/snapshot lifecycle | Retained exact identities, links, timing, source tuple, and snapshot artifact |
| raw/normalized record pair | Retained exact identities, links, content, optional raw idempotency, and original ordinal |
| gaps | Exact canonical ranges plus independent closed source reason |
| organization/site/actor and `SeriesKey` | Future Studio adapter/program mapping |
| JSON tags, casing, and serialization | Non-semantic adapter/codec behavior |
| declaration revision and retirement | Historian registry authority; absent from Studio |

A future adapter may split a Studio batch into per-series admissions by copying
the shared lifecycle and retaining original ordinals. This contract adds neither
cross-series atomicity nor active identical-binding uniqueness. Program mapping
owns that policy; `SeriesId` plus declaration revision keeps each canonical record
unambiguous.

## Evidence

Focused tests cover projection/schema/identity representation, optional capture
completion and ordering, exact lifecycle links and binding, observed and
no-change admissions, optional provenance artifacts and both idempotencies,
redelivery/retry independence, count/order/identity/link/interpretation/scope
refusals, exact observation/gap maxima and one-over refusals, gap/no-change reason
distinctions, registry non-mutation, immutable binding, and declaration-bound
snapshot retention.

The independent `source_oracle` uses standard-library primitives only. It
represents schema, the full lifecycle, source binding, Unix milliseconds,
artifacts/content, interpretation tri-states, transport and source idempotency,
raw/normalized links, original ordinal, gaps, and retry scope. It independently
computes each M00-PR05 violation and a complete normalized retained-admission
record. The top-level public-model adapter constructs the corresponding real
`CanonicalAdmission`, normalizes every retained declaration, source/capture,
retry, lineage, and gap field through public accessors, and compares that record
for exact structural equality. The nominal identity and complete 57-variant
sanitized error inventories include this successor.

## M02-PR01 input and hard boundary

M00-PR05 closes the source/capture prerequisite. The only complete native semantic
input M02-PR01 may serialize is `CanonicalAdmission`: exact store and governing
declaration snapshot, original validated envelope, request-scoped
`RetryQualification`, source schema/interval, capture lifecycle, and closed
observed/no-change lineage. M02-PR01 must not accept a bare envelope, infer a
declaration revision, reinterpret a historical observation, discard required
source/capture fields, or equate source transport/idempotency with Historian retry.

This completion authorizes planning M02; it does not claim M02 complete. Journal
frames, group commit, recovery, and durable receipts remain M02-PR01 work.
Persistent registry/manifests and durable retry state remain M02-PR02 work.

## Deferred ledger

- Non-semantic codec and decimal representation hints remain deferred; they
  cannot change interpretation of accepted exact values.
- Retention and priority classification remain deferred and cannot be inferred
  from declaration, source evidence, or collection mode.
- Journal frames, group commit, recovery, and durable receipts remain M02-PR01.
- Persistent registry, manifest, and durable retry state remain M02-PR02.
- Studio adapter code, organization/site/actor authorization, `SeriesKey`
  mapping, and active identical-binding program policy remain Studio-owned future
  integration work.
- The original broad in-memory query/rollup/building-fixture oracle remains an
  acknowledged predecessor gap requiring a named future successor.
- M01 priority handling and an explicit byte-budget admission contract remain
  acknowledged predecessor gaps; the fixed command-count limit proves neither.

No deferred item is silently claimed complete or moved into M00/M01. The ignored
`_roadmap/` directory remains local and unpublished.
