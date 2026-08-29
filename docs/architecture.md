# Canonical model and bounded-runtime architecture

## Present topology

M00 established the reviewed dependency-free canonical model. M01-PR01 added a
separate native lifecycle root, and M01-PR02 connects it inward to the still-frozen
model through bounded volatile ingress:

```text
default workspace selection
        |
        v
  och-core (native) <---- och-runtime (native)       och-policy (tooling)
  canonical model         caller-owned executor      cargo_metadata + parsing
  no dependencies         one writer + 16 slots      support
                                   |
                                   v
                           tokio rt + sync only
        ^
        |
  future adapters (not created yet)
```

[`och-core`](../crates/och-core/) owns exact platform-independent contracts for
identity, values/content, time, quality/status, producer ordering, collection
modes, interval/gap/no-change evidence, bounded atomic envelopes, and retry
comparison. It retains no product dependencies. Its only executable remains a
baseline example used to verify buildability and measure a native binary bound;
that example is not a runtime or supported product command.

[`och-runtime`](../crates/och-runtime/) owns async writer lifecycle and one fixed
volatile admission window per instance. Its public runtime starts one private task
on the caller's active Tokio executor and returns after private state and ingress
readiness. A custom cloneable ingress synchronously admits at most 16 outstanding
distinct commands, including in-flight work, and exposes no Tokio type. Exact
core retry classification coalesces only equivalent outstanding work; conflicts,
full capacity, and closure recover the incoming command. Multiple runtimes are
independent; there is no global singleton, restart path, latest registry, or
exposed writer state. Tokio remains admitted only on the direct runtime edge with
default features disabled and `rt` plus `sync` enabled.

[`och-policy`](../tools/och-policy/) is private repository tooling. It appears in
the full workspace so clippy and tests cover it, while root `default-members`
selects both native roots and no tooling. Consequently the tool's Cargo
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
- `collection` performs bounded atomic cross-item validation;
- `retry` compares explicit scope, key, and external content identity;
- `error` exposes only closed sanitized validation failures.

Invalid scalar ranges are excluded by constructors. Invariants involving series
mode or multiple items are enforced only by `CollectionEnvelope`, whose evidence
fields are private. The model does not create IDs, hash bytes, infer time or
producer order, infer held values/deltas/resets, or translate native extensions.
See the [canonical model contract](model-contract.md).

## Lifecycle, ingress, and failure ownership

The caller owns the active executor. Startup uses `Handle::try_current`, never
constructs a Tokio runtime or thread, and fails without panic when no executor is
active. A startup guard aborts the writer if startup is cancelled. Graceful
shutdown retains the join while it is awaited, so cancellation drops the handle
and requests abort rather than detaching the task. Ordinary Drop never blocks or
promises completion. Closed errors sanitize early exit, cancellation, and panic;
the release profile's `panic=abort` means panic classification is test/debug
evidence, not a release recovery guarantee.

One short private synchronous mutex linearizes submission with shutdown; no guard
crosses an await. A fixed slot table and FIFO index ring bound queued plus in-flight
distinct work at 16. Equivalent retries share one fixed terminal state and do not
retain the duplicate envelope. Retry comparison precedes the full check, but
closure takes precedence once graceful shutdown has atomically closed admission.
The retry window ends at terminal completion and has no durable horizon.

The private writer is the sole queue consumer. Graceful shutdown closes admission,
drains accepted work FIFO, resolves receipts, and joins. `WriterHandled` means the
volatile command was consumed and dropped only. Runtime Drop, cancelled shutdown,
task cancellation/panic/early exit, and admission-lock poison close admission and
single-assign every unresolved receipt to `WriterStopped`; handled state cannot be
overwritten. Caller-supplied content identity is trusted only for core retry
classification and is never recomputed from envelope content.

## Intentionally absent

There is currently no async/blocking admission wait, configurable capacity,
same-series evidence replacement, public queue status, latest-value publication,
registry, snapshot/read handle, journal, segment, store, persistence or wire
format, query engine, network service, SQL layer, cloud/object provider, embedded
database, memory mapping, Studio/Engine link, adapter, or donor-code compatibility
layer.

Those omissions keep the reviewed canonical model independent of lifecycle and
platform choices and prevent large implementation dependencies from becoming
architectural facts before their contracts are reviewed. Lifecycle history is in
the [M01-PR01 brief](implementation-brief-m01-pr01.md), bounded ingress is recorded
by [M01-PR02](continuation-m01-pr02.md), and publication/read ownership remains in
the [M01-PR03 continuation](continuation-m01-pr03.md).
