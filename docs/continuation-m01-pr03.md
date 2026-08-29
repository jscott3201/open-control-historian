# M01-PR03 volatile latest publication and snapshot evidence record

## Delivered outcome

M01-PR03 extends the existing one-writer `och-runtime` with one fixed,
runtime-local, volatile registry of exact latest observation evidence. It preserves
the frozen `och-core` model, caller-owned executor, one private writer, fixed
16-command ingress, outstanding-only retry coalescing, existing receipt outcomes,
and direct Tokio `rt` plus `sync` edge. It adds no dependency or feature.

## Public contract

- `MAX_PUBLISHED_SERIES` is a separately owned fixed value of 16, independent of
  `MAX_OUTSTANDING_COMMANDS`. A full registry never evicts an entry.
- `HistorianRuntime::read_handle` returns a cloneable synchronous
  `LatestReadHandle` only after writer readiness. It shares registry state but
  contains no executor, Tokio primitive, task handle, wait, or subscription.
- `LatestReadHandle::snapshot` captures an immutable, cheaply cloneable
  `LatestSnapshot`. Snapshots provide length, emptiness, `SeriesId` lookup, slice,
  and iteration access. Enumeration order is unspecified and carries no arrival
  or latest-order meaning.
- `PublishedObservation` exposes exact bound `SeriesMetadata`, exact
  `Observation`, and guaranteed `ProducerPosition`. It is observation evidence,
  never a current or held value.
- `LatestReadError` is one sanitized unavailable result. Its display and debug
  forms expose no synchronization, task, queue, identity, value, or panic detail.

A runtime internally retains only its current snapshot of at most 16 entries and
bounded staging for one update. Callers may retain old immutable snapshots; that
caller-owned volatile memory is not runtime history, persistence, durability, or
restart recovery.

## Eligibility, identity, and ordering

Only a validated envelope containing at least one observation with explicit
positions on all observations is eligible. Frozen core validation already makes
positions all-or-none and strictly increasing within an envelope, so its final
observation is the sole candidate. No-change evidence, gap-only observed evidence,
and observations without positions are handled no-ops: they bind no metadata,
consume no registry capacity, and do not check an existing binding.

The key is `SeriesId` alone. First eligible publication binds the exact
`SeriesMetadata`, including producer and collection mode. Every later eligible
candidate for that key must match exactly. `ProducerPosition` alone has latest
authority:

- a greater position replaces the exact published observation;
- a lower position is a stale handled no-op;
- equal position plus exact-equal `Observation` is an idempotent handled no-op;
- equal position plus different `Observation` is a publication fault.

Arrival/FIFO position, timestamps, UUID/observation identity, raw-order key, retry
identity, quality, and value never select latest. All five collection modes may
publish positioned observations. Mode does not infer freshness, hold/current
value, interpolation, deltas, cumulative resets, or interval extension.

At 16 entries, existing-key updates and no-ops plus all ineligible work remain
valid. A seventeenth new eligible series, eligible metadata mismatch, or
equal-position/different-observation conflict fails closed: no eviction or partial
publication occurs, ingress closes, the faulting and all unresolved receipts
become `WriterStopped`, and future snapshot capture is unavailable.

## Atomicity and lifecycle

The existing private synchronous state mutex is the single authority for snapshot
capture, complete-snapshot swap, slot release, terminal receipt assignment, stop,
and shutdown. No guard crosses an await and notifications occur after unlock. The
writer clones one bounded candidate view outside the final critical section. It
then commits the whole view before assigning `WriterHandled` and releasing the
outstanding retry key. A racing reader therefore sees the complete old or complete
new snapshot, and capture after an advancing receipt observes the advance unless a
later abnormal failure has made reads unavailable.

`WriterHandled` now means the writer consumed the command and completed its
publication decision. It does not mean that every command published, that evidence
remains retained, or that anything is persistent, durable, queryable, or
restart-safe. Equivalent outstanding retries still discard the incoming duplicate
envelope and share only the first work item's terminal receipt; only that first
envelope participates in publication.

Graceful shutdown atomically closes admission, FIFO-drains accepted work, completes
each publication decision before its receipt, seals the final snapshot, and joins.
Read handles may outlive the consumed runtime and keep capturing the sealed view.
Ordinary Drop, cancelled shutdown, task cancellation/abort/panic/early exit,
publication fault, or state-lock poison stops unresolved receipts and makes future
captures unavailable. Snapshots acquired before failure remain immutable. A read
handle alone never keeps the writer alive, and runtime instances never share state.
Panic classification remains test/debug evidence because release uses
`panic = "abort"`.

## Deterministic evidence

Current-thread tests use private one-shot gates and bounded yields without clocks,
sleep, randomness, network, filesystem, or external processes. They cover:

- empty available readiness, cloned handles, isolated runtimes, opaque debug/error
  text, handle Drop, and graceful outliving sealed reads;
- multi-observation final-candidate selection with adversarial timestamps and UUIDs,
  greater/lower/equal behavior, held-old-snapshot immutability, and all five modes;
- no-change, gap-only, and unpositioned no-ops before and after exact metadata bind;
- equal-position conflicts, producer mismatch, mode mismatch, queued receipt stop,
  closed future ingress, and old-snapshot validity;
- 16 sequential series, valid existing/ineligible work at capacity, and a
  fail-closed seventeenth eligible series without partial state;
- a deterministic pre-swap gate proving old/new visibility and
  publication-before-handled ordering;
- 1,024 equivalent positioned retries retaining one work item/shared receipt and
  publishing only the admitted first envelope;
- graceful drain/seal plus abnormal Drop, cancelled shutdown, cancellation, abort,
  panic, early exit, poison, and test-only pre-/post-swap failure behavior.

## Excluded

There is no configurable capacity, registration, eviction, async/blocking wait,
subscription, mutable read guard, public lock/queue/task state, persistence,
journal/storage, durable history, restart recovery, snapshot query/filter engine,
wire/serialization/network behavior, adapter, extra task/thread, global singleton,
or new receipt/publication disposition.
