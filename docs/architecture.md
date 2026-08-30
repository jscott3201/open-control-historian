# Canonical model and bounded-runtime architecture

## Present topology

M00 established the reviewed dependency-free canonical model. M01-PR01 added a
separate native lifecycle root, M01-PR02 connected it inward through bounded
volatile ingress, and M01-PR03 added bounded runtime-local latest publication.
M00-PR04 added bounded canonical series declaration authority, M00-PR05 added
bounded source/capture provenance and canonical admission, M02-PR01a established
the store-scoped runtime input, M02-PR01b0 froze Journal V1 semantic framing, and
M02-PR01b1 connected that single path to one bounded active-journal durable
vertical, M02-PR02a roots its range, mechanical cutoff, and complete canonical
registry history in one bounded manifest, and M02-PR02b roots a bounded durable
retry replay/guard projection in Manifest V2. M02-PR02c adds one bounded
store-owned rotation/seal transition: Generation Catalog V1 and Manifest V3 bind
immutable raw-Journal generations while the same global append sequence continues
in deterministic successor active journals. M02-PR03a adds conservative
manifest-root suffix recovery, fixed Recovery State V1, and Manifest V4 without
disk-pressure or degraded-operation policy:

```text
default workspace selection
        |
        v
  och-runtime (native) ----> och-store (native) ----> och-core (native)
  Tokio coordinator            active journal              ^
  16 slots + byte bounds       Journal V1 + checkpoints     |
  one control gate             manifest + registry/retry    |
  safe-boundary rotation       catalog + raw seals          |
       |                                                    |
       v                                      future adapters (not created yet)
  tokio rt + sync only

  och-policy (tooling): cargo_metadata + parsing support
```

[`och-core`](../crates/och-core/) owns exact platform-independent contracts for
identity, values/content, time, quality/status, producer ordering, collection
modes, interval/gap/no-change evidence, bounded atomic envelopes, series
declaration revisions and retirement, registry-issued active-declaration binding,
source/capture lineage, declaration-authorized canonical admission, and retry
comparison. It retains no product dependencies. Its only executable remains a
baseline example used to verify buildability and measure a native binary bound;
that example is not a runtime or supported product command.

[`och-runtime`](../crates/och-runtime/) owns the async facade around one immutable
`StoreId`, one fixed 16-command count window, explicit exact encoded-byte limits,
one Tokio coordinator, one dedicated blocking store writer, one fixed reaper,
and one fixed volatile latest registry. `HistorianRuntime::open` returns only
after active-artifact create/open, retained writer lock, bounded scan/recovery
convergence, store evidence publication, and coordinator readiness. There is no
competing public volatile start path. `IngressCommand` owns exactly one
already-authorized `CanonicalAdmission` plus resource class and barrier demand;
there is no bare-envelope command path.

Submission first obtains the exact Journal V1 frame length without allocating
the frame. Under the existing state authority it applies closed/store/retry/count
and protected/normal/bulk byte-capacity rules, retains the slot and reservation,
then allocates and encodes outside the lock and verifies the exact length.
Priorities reserve capacity and may demand a barrier but never reorder semantic
FIFO. Equivalent outstanding retries share handled and durable stages. Completed
equivalents replay the original exact proof from the immutable committed replay
tier; replay overflow becomes a non-replayable guard, and only FIFO eviction from
both bounded tiers makes the key fresh. A separate synchronous
read handle captures immutable store-scoped snapshots of at most 16 nominal
series. Multiple runtimes are independent; the retained file lock excludes a
second active writer for the same artifacts. Tokio remains admitted only on the
direct runtime edge with default features disabled and `rt` plus `sync` enabled.
Successful recovery remains `RuntimeHealth::Healthy`; immutable inspection adds
only the latest bounded path/content-free recovery report. Fresh stores report
none, and existing runtime open and error enums remain compatible.

Core remains the sole declaration and source-admission semantic authority. The
blocking store writer now owns the one non-cloneable live `SeriesRegistry`, and
the runtime exposes only bounded register/revise/retire operations plus
registry-issued current-active envelope binding. Those operations and the
append-to-publication handshake share one async control gate and the existing
bounded writer channel, so no second ordering authority exists. Lifecycle and
bind callers must first obtain one of 16 nonblocking control-admission permits;
the permit is held through gate acquisition and the writer response, making
cancellation reclaim capacity without an unbounded mutex-waiter population. Retry
classification reads the admission's exact `RetryQualification` against
outstanding work and the immutable committed retry projection, while volatile
publication reads only its validated envelope. The exact `SeriesMetadata` bind
remains a runtime-local read optimization invariant and cannot authorize a
declaration or reinterpret an old revision. Source/declaration evidence stays
owned by the command until append/publication returns it to the bounded slot,
and the slot and exact byte reservation remain retained until durability or
terminal stop. Reopen evidence is decoded and bounded but cannot authorize
submission, registry, or latest state.

Because the registry is reachable only on the blocking writer, synchronous
ingress performs resource/framing admission rather than historical lookup. An
unknown or altered historical declaration is a terminal authority mismatch: no
handled/durable success is emitted, both receipt stages resolve `WriterStopped`,
and the runtime fail-stops without journal or latest mutation.

[`och-store`](../crates/och-store/) owns version-one semantic bytes for complete
already-authorized admissions. A fixed 28-byte header scopes a journal to one
exact `StoreId`; each independent admission frame carries its own magic, version,
closed kind, zero flags, positive append sequence, bounded payload length,
complete canonical payload, and CRC-32C. Integer fields are big-endian and
strings and counts are explicitly length-prefixed. Decode checks the declared
payload against both the fixed 8 MiB maximum and a caller-selected lower limit
before any field allocation. It produces only store-owned non-authorizing
inspection evidence, never a registry-issued declaration or `CanonicalAdmission`.

The store also owns one bounded recognized inventory in an existing directory: a
never-renamed stable store lock, one retained read/write lock for the current
generation, the legacy generation-one active pair or one deterministic successor
pair, two reusable Manifest V1/V2/V3/V4 slots, three reusable complete registry,
retry, recovery, and Generation Catalog V1 slots, one fixed rotation intent, fixed
staging names, and at most 64 immutable raw-Journal sealed artifacts. Manifest
stores use active-header version 2 in the unchanged 28-byte layout; every
admission frame remains Journal V1. An old header-v1 decoder rejects the fence.
Create-new synchronizes active genesis, an empty registry snapshot, an empty
retry snapshot, and Manifest V2 generation one before readiness. The sole
blocking writer assigns one store-global strict append sequence across all
generations, explicitly seeks to journal end, and validates both frame and
declaration StoreId against the header. A barrier performs journal sync, writes
the alternate CRC-protected checkpoint slot, then synchronizes that checkpoint
publishes/verifies the bounded Retry State V1/V2 candidate, and publishes a manifest naming
the exact cutoff, registry, and retry snapshot before exposing durability. The
checkpoint contains only store/journal identity, slot generation, append
sequence, end offset, and checksum; it is not registry or retry authority.

At a safe nonempty size/count/age boundary, runtime first completes the ordinary
durable receipt batch. Store then persists a non-authoritative intent, streams the
exact fully durable active bytes into an immutable raw-Journal artifact, verifies
framing/declarations/range/length/checksum, creates and synchronizes an empty
successor at the prior global sequence floor, publishes the next bounded catalog,
and publishes Manifest V3 last. Only then does it adopt the successor and clean
the redundant predecessor/intent. Catalog capacity is exactly 64 and never
reclaims or overwrites sealed history.

Open-existing acquires the stable lock before selection or mutation, validates a
bounded non-recursive inventory, selects only strict consecutive manifest
candidates, restores the referenced registry solely through public core
lifecycle replay, and requires exact snapshot re-encoding. The selected manifest
cutoff must equal the mechanical checkpoint, and every recovered declaration
must resolve exactly from retained history. A nonempty premanifest store requires
an explicit matching snapshot; exact header-only V1/V2 stores may bootstrap
empty. Manifest V2/V3 and V4 with a retry reference restore only that referenced
canonical snapshot. A legacy Manifest V1 restores empty retry tiers without
scanning or backfilling retained Journal V1 records. Its recovery-only V4
successor canonically preserves the all-zero absent retry body; the first new
durable append establishes retry generation one in V2 or report-preserving V4.
Decoded evidence never authorizes registry or retry state, and latest restarts
empty.
Normal open validates bounded catalog bytes and sealed length/header metadata,
not every sealed payload byte. A narrow exact-intent path converges only to the
prior root before Manifest V3 or the new root after it; missing or mismatched
evidence refuses unchanged. Each V3 root binds the active generation and floor
to the checked successor and cutoff of its last sealed entry, and consecutive
catalog roots must preserve the exact older prefix and append one entry. Active
pairs and sealed finals must equal the selected root apart from narrowly
verified intent redundancy. A strict catalog prefix left by interruption after
ordinary manifest adoption is verified and removed; forked or unrelated
catalogs refuse. Both manifest slots are retained as independent
missing/valid/corrupt/unsupported/identity/I/O outcomes before selection. Any
damaged possible-newer authority refuses rather than falling back, including a
metadata-only successor at an equal active cutoff.

Only after registry, retry, catalog/seal metadata, inventory, active
identity/header/checkpoint/cutoff, retained declarations, and the narrow rotation
law validate does root-scoped active scan expose a strictly post-cutoff suffix.
At sequence floor zero its first complete frame must be numbered one; normal
higher-generation floors retain exact successor numbering. A first frame numbered
two refuses unchanged rather than becoming removable suffix evidence.
Accepted recovery synchronizes truncation without adoption, publishes fixed
96-byte Recovery State V1, then commits Manifest V4. Later metadata and rotation
commits preserve that report reference. Interrupted precommit evidence refuses;
a renamed V4 converges to one report-bound root. Normal open still does not scan
sealed payloads. `och-store` still owns no final native segment encoding,
destructive repair, stale-restore acceptance, reclamation, latest projection, or
query behavior.

Exact contracts are in [Manifest V1/V2/V3/V4](manifest-v1-format.md),
[Retry State V1/V2](retry-state-v1-format.md),
[Generation Catalog V1](generation-catalog-v1-format.md), and
[sealed raw Journal V1](sealed-journal-v1-format.md), and
[Recovery State V1](recovery-state-v1-format.md).

[`och-policy`](../tools/och-policy/) is private repository tooling. It appears in
the full workspace so clippy and tests cover it, while root `default-members`
selects all three native roots and no tooling. Consequently the tool's Cargo
metadata/parsing dependencies do not masquerade as native product dependencies.

## Direction and ownership

Package roles are explicit rather than inferred from directory names:

- **native** owns platform-independent product contracts and implementation;
- **adapter** will own edge/platform integration and may depend inward on native;
- **tooling** owns repository policy, generation, or validation and is outside
  the product closure.

The permitted future product edge is `adapter -> native`. A `native -> adapter`
or `native -> tooling` path is a dependency inversion and fails policy. Adapters
also cannot be selected implicitly through workspace defaults. No placeholder
adapter crates exist today because an empty package would imply unsupported
platform scope without proving behavior.

Within `och-core`, modules follow semantic ownership rather than runtime layers:

- `identity`, `bounded`, and `value` retain exact validated primitives;
- `time`, `quality`, and `position` retain independent evidence domains;
- `observation` defines immutable series modes, observations, and raw order;
- `series` owns immutable source binding, revisioned interpretation metadata,
  bounded declaration history, terminal tombstones, and active envelope binding;
- `source` owns bounded schema/capture/record provenance and consumes one active
  declaration binding into the final immutable canonical admission;
- `collection` performs bounded atomic cross-item validation;
- `retry` compares explicit scope, key, and external content identity;
- `error` exposes only closed sanitized validation failures.

Invalid scalar ranges are excluded by constructors. Invariants involving series
mode or multiple items are enforced only by `CollectionEnvelope`, whose evidence
fields are private. Declaration lifecycle is enforced only by the non-cloneable
`SeriesRegistry` authority;
the constructor of `DeclaredCollectionEnvelope` is private and its value is not
cloneable, so a bare envelope or historic declaration cannot self-authorize or
fork the issued capability. `CanonicalAdmission` has no public bypass constructor.
The model does not create IDs, hash bytes, infer time or
producer order, infer held values/deltas/resets, or translate native extensions.
See the [canonical model contract](model-contract.md).

## Lifecycle, ingress, and failure ownership

The caller owns the active executor. Open uses `Handle::try_current`, never
constructs a Tokio runtime, and fails without panic when no executor is active.
Filesystem work is isolated on one long-lived standard-library blocking thread;
the Tokio coordinator only exchanges bounded messages and awaits readiness or
completion. A fixed reaper owns the blocking-thread join. Every coordinator exit,
panic, cancellation, and publication failure sets the shared stop signal and
nonblocking-wakes the store worker even while another sender remains retained.
Startup cancellation, runtime Drop, or shutdown cancellation signals fail-stop
without joining in Drop; the reaper supplies observable eventual lock release.
Graceful shutdown awaits
both coordinator and reaper. Closed errors sanitize early exit, cancellation,
panic, and path-free store I/O evidence; the release profile's `panic=abort`
means panic classification is test/debug evidence, not a release recovery
guarantee.

One short private synchronous mutex linearizes submission, snapshot capture,
publication swap, receipt assignment, reservation release, stop, and shutdown;
no guard crosses an await. A fixed slot table and FIFO index ring bound queued plus
in-flight-and-pending-durability distinct work at 16. Exact encoded bytes are
counted before allocation and held under the configured global/class ceilings.
Equivalent outstanding retries share one two-stage state and do not retain the
duplicate admission. Closure takes precedence, then the exact runtime StoreId and
Journal V1 framing are enforced before outstanding retry, durable replay/guard,
and count/byte capacity. The outstanding tier ends only at durable completion or
terminal stop. At a successful barrier, one mutex-held batch transition installs
the writer-committed immutable retry projection, resolves every covered receipt,
releases reservations, and only then wakes waiters.

Only an envelope with at least one observation and explicit positions on all its
observations is publication-eligible. Core validation makes the final observation
the greatest positioned candidate. `ProducerPosition` alone selects replacement;
arrival, timestamp, UUID, raw-order, retry, quality, and value never do. The first
eligible candidate binds exact `SeriesMetadata` by `SeriesId`. Greater position
replaces, lower and equal-identical candidates are no-ops, while metadata mismatch,
equal-position/different-observation conflict, and a seventeenth new eligible
series fail closed without eviction or partial visibility. All five collection
modes may publish exact observation evidence without implying hold, current value,
freshness, interpolation, delta/reset, or interval extension.

The Tokio coordinator is the sole ingress consumer and the blocking worker is the
sole mutable store-I/O owner. The worker appends first; the coordinator then stages
and atomically swaps the complete volatile publication decision before returning
the worker's publication acknowledgement. `WriterHandled` exposes append identity
only after both append and publication decision, but is explicitly non-durable.
Readers see only a complete old or complete new latest view. Runtime latest state
retains only the current snapshot; caller-held old snapshots account for
caller-owned volatile memory, not runtime history.

The blocking worker preserves FIFO and groups handled appends until the first of
configured time, record, or byte bounds, explicit/immediate demand, protected
demand, or shutdown. Durable order is append, volatile publication, journal
sync, alternate checkpoint slot write, checkpoint sync, exact Retry State V1/V2
count/write/sync/readback/publication, Manifest V2/V3/V4 publication naming the exact
cutoff and current registry/retry slots, atomic ingress projection/receipt batch
transition, then waiter wake.

A durable receipt names store, exact journal generation, append sequence, frame
end, mechanical checkpoint generation, manifest generation, registry
generation/slot, optional retry generation/slot, sequence floor, and optional
catalog identity. Rotation never rewrites an already returned receipt. A legacy V1 open truthfully
reports no retry reference. A timeout never
synchronizes while the newest append still awaits the coordinator's publication
acknowledgement; after acknowledgement an elapsed deadline may flush immediately.
The receipt claims only the active artifacts under the documented platform
contract.

Graceful shutdown closes admission, drains accepted work FIFO, resolves each
publication decision, forces a final barrier, seals latest, and joins coordinator
and blocking worker through the reaper. Outliving read handles continue to capture
that sealed view. `WriterHandled` includes ineligible and stale no-ops and proves
no durability or query result. Runtime Drop, cancelled shutdown, task
cancellation/panic/early exit, write/sync/checkpoint/publication fault, and
admission-lock poison close admission, single-assign unresolved stages to
`WriterStopped`, report sanitized fault health unless a typed rotation demand is
already established, and advance no false cutoff. Previously acquired snapshots
remain immutable. An append I/O failure that may have changed bytes terminally
poisons that open store authority; later append, sequence assignment, and sync
refuse until drop plus validated reopen. A preparation rollback likewise stops and wakes any equivalent
receipt that coalesced during the preparation window while releasing exact bytes.
Caller-supplied content identity is trusted only for core retry classification
and is never recomputed from admission or envelope content.

## Intentionally absent

There is currently no async/blocking admission wait, eviction, subscription/wait
API, mutable read guard, final native segment format, sealed-history read/query
API, retention/reclamation, unbounded/time-based retry, manifest-backed latest
reconstruction, disk-pressure/degraded recovery policy, query
engine, network service, SQL layer, cloud/object provider, embedded database,
memory mapping, Studio/Engine link, adapter, or donor-code compatibility layer.

Those omissions keep the reviewed canonical model independent of lifecycle and
platform choices and prevent large implementation dependencies from becoming
architectural facts before their contracts are reviewed. Lifecycle history is in
the [M01-PR01 brief](implementation-brief-m01-pr01.md), bounded ingress is recorded
by [M01-PR02](continuation-m01-pr02.md), and bounded publication/read ownership is
recorded by [M01-PR03](continuation-m01-pr03.md).
The canonical declaration transition and its pre-M02 hard stop are recorded by
[M00-PR04](continuation-m00-pr04.md). The accepted source/capture crosswalk and
exact future journal input boundary are recorded by
[M00-PR05](continuation-m00-pr05.md). The store-scoped canonical-admission runtime
transition and accepted split before journal bytes are recorded by
[M02-PR01a](continuation-m02-pr01a.md). The exact Journal V1 bytes and historical
durable hard stop are recorded by [M02-PR01b0](continuation-m02-pr01b0.md).
The complete generation-one active-journal durable vertical and its PR02 handoff
are recorded by [M02-PR01b1](continuation-m02-pr01b1.md).
The manifest-rooted canonical registry transition and its exact successor ledger
are recorded by [M02-PR02a](continuation-m02-pr02a.md).
The manifest-rooted durable retry horizon and its exact compatibility boundary
are recorded by [M02-PR02b](continuation-m02-pr02b.md).
The bounded raw-Journal rotation/seal transition and its exact successor boundary
are recorded by [M02-PR02c](continuation-m02-pr02c.md).
The conservative manifest-root recovery transition and its PR03b deferrals are
recorded by [M02-PR03a](continuation-m02-pr03a.md).
