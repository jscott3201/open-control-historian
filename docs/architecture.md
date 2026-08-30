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
registry history in one bounded manifest, M02-PR02b roots a bounded durable
retry replay/guard projection, and M02-PR02c adds one bounded store-owned
rotation/seal transition. The durable-format reset now places one current V1
contract for each artifact family behind a fixed Store Format V1 marker.
M02-PR03a adds a manifest-rooted transaction that reports and removes only one
proven terminal invalid/torn active suffix. M02-PR03b1 adds store-only logical
transaction preflight plus typed observed pressure and sticky reopen custody.
Manifest V1 and Generation Catalog V1 bind immutable raw-Journal generations while the
same global append sequence continues in deterministic successor active journals:

```text
default workspace selection
        |
        v
  och-runtime (native) ----> och-store (native) ----> och-core (native)
  Tokio coordinator            active journal              ^
  16 slots + byte bounds       Journal V1 + checkpoints     |
  one control gate             manifest + registry/retry    |
  safe-boundary rotation       catalog + raw seals          |
  recovery inspection         recovery + pressure custody  |
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
fixed 32-byte Store Format V1 marker, never-renamed stable store lock, one retained
read/write lock for the current generation, the generation-one active pair or one
deterministic successor pair, two reusable 160-byte Manifest V1 slots, three
reusable complete registry and retry slots, three Generation Catalog V1 slots,
three fixed 128-byte Recovery State V1 slots, one fixed rotation intent, fixed
staging names, and at most 64 immutable
raw-Journal sealed artifacts. Every active and sealed journal uses the exact
28-byte Journal Header V1 and every admission frame remains Journal V1.
Create-new publishes the marker, active genesis, an empty registry snapshot, a
mandatory empty Retry State V1 snapshot, and Manifest V1 generation one before readiness. The sole
blocking writer assigns one store-global strict append sequence across all
generations, explicitly seeks to journal end, and validates both frame and
declaration StoreId against the header. A barrier performs journal sync, writes
the alternate CRC-protected checkpoint slot, then synchronizes that checkpoint,
publishes/verifies the bounded Retry State V1 candidate, and publishes Manifest V1 naming
the exact cutoff, registry, and retry snapshot before exposing durability. The
checkpoint contains only store/journal identity, slot generation, append
sequence, end offset, and checksum; it is not registry or retry authority.

At a safe nonempty size/count/age boundary, runtime first completes the ordinary
durable receipt batch. Store then persists a non-authoritative intent, streams the
exact fully durable active bytes into an immutable raw-Journal artifact, verifies
framing/declarations/range/length/checksum, creates and synchronizes an empty
successor at the prior global sequence floor, publishes the next bounded catalog,
and publishes Manifest V1 last. Only then does it adopt the successor and clean
the redundant predecessor/intent. Catalog capacity is exactly 64 and never
reclaims or overwrites sealed history.

Open-existing first validates a bounded non-recursive inventory and the Store
Format V1 marker without mutation. Markerless, historical, malformed, and mixed
stores return path-free `UnsupportedStoreFormat` before stable-lock creation or
acquisition. After the fence passes, open acquires the stable lock, repeats
validation, and selects only strict consecutive manifest
candidates, restores the referenced registry solely through public core
lifecycle replay, and requires exact snapshot re-encoding. The selected manifest
cutoff must equal the mechanical checkpoint, and every recovered declaration
must resolve exactly from retained history. Every current Manifest V1 has a
mandatory Retry State V1 reference; there is no premanifest adoption, history
backfill, format migration, or compatibility decoder. Decoded
evidence never authorizes registry or retry state, and latest restarts empty.
Normal open validates bounded catalog bytes and sealed length/header metadata,
not every sealed payload byte. A narrow exact-intent path converges only to the
prior root before Manifest V1 or the new root after it; missing or mismatched
evidence refuses unchanged. Each Manifest V1 root binds the active generation and floor
to the checked successor and cutoff of its last sealed entry, and consecutive
catalog roots must preserve the exact older prefix and append one entry. Active
pairs and sealed finals must equal the selected root apart from narrowly
verified intent redundancy. A strict catalog prefix left by interruption after
ordinary manifest adoption is verified and removed; forked or unrelated
catalogs refuse. After every other authority family is proven under both retained
locks, a root-aware dry scan may classify only a terminal short prefix, malformed
exact prefix, truncated declared frame, or invalid complete frame ending exactly
at EOF. Recovery publishes its report, truncates and synchronizes exactly to the
unchanged manifest/checkpoint cutoff, then commits an otherwise byte-identical
next manifest. A valid post-root frame, valid-plus-torn bytes, later candidate
bytes, identity/sequence mismatch, interior corruption, or ambiguity refuses
unchanged. `och-store` still owns no final native segment encoding, broad repair,
reclamation, latest projection, stale-restore custody, runtime degraded mode, or query behavior.
At a store-owned create/write/resize/truncate/sync/rename/publish/remove boundary,
only `StorageFull` and `QuotaExceeded` are typed as pressure. The first such live
failure makes the direct active journal or composed manifest store require reopen;
all later mutation and authorization requests refuse before I/O or model mutation.
Inspection remains path-free and reports only the last in-memory/mechanical and
committed evidence plus volatile write custody. Reopen remains exactly the
current conservative PR03a path.
Exact contracts are in [Store Format V1](store-format-v1.md),
[Manifest V1](manifest-v1-format.md), [Retry State V1](retry-state-v1-format.md),
[Generation Catalog V1](generation-catalog-v1-format.md),
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
sync, alternate checkpoint slot write, checkpoint sync, exact Retry State V1
count/write/sync/readback/publication, Manifest V1 publication naming the exact
cutoff and current registry/retry slots, atomic ingress projection/receipt batch
transition, then waiter wake.

A durable receipt names store, exact journal generation, append sequence, frame
end, mechanical checkpoint generation, manifest generation, registry
generation/slot, mandatory retry generation/slot, sequence floor, and optional
catalog identity. Rotation never rewrites an already returned receipt. A timeout never
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
reconstruction, broad repair or stale-restore event model, runtime pressure-degraded
latest/receipt mode, query
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
The current-only artifact epoch is recorded by the
[durable-format reset](continuation-m02-v1-durable-format-reset.md). The
manifest-rooted terminal-suffix transaction and deferred successor boundary are
recorded by [M02-PR03a](continuation-m02-pr03a.md).
Store-only pressure custody and its runtime-policy handoff are recorded by
[M02-PR03b1](continuation-m02-pr03b1.md).
