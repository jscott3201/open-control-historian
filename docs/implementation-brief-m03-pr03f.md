# M03-PR03f native evidence-instrumentation implementation brief

## Objective and authority fence

Add one bounded, disabled-by-default instrumentation prerequisite at existing
current-V1 native seams so a separately reviewed private harness can later bind
to actual writer, durability, receipt, pressure, and crash behavior.

This slice is not that harness. It adds no tooling package, report writer,
measurement command/result, V2 source site, V2 byte/name/state machine, product
activation, dependency, lockfile change, threshold, budget, or SLO. Store Format
V1 remains the only implemented and accepted durable format.

## Feature and API containment

Both `och-store` and `och-runtime` declare the exact non-default feature
`m03-pr03e-native-harness` with explicit empty defaults. Runtime forwards only to
the store feature. Explicit `--workspace --all-features` therefore compiles the
seam through accepted Cargo feature unification, while normal/default builds
contain no session, recorder, fault plan, event state, or behavior branch.

The rustdoc-hidden runtime module is the only intended future tool-facing facade.
Its closed concrete API owns one process-local `Instant` origin, one exact
optional fault target, fixed-capacity preallocated records, bounded snapshots,
and parent-waitable crash readiness. Store exposes only the rustdoc-hidden
feature surface required across the existing native dependency edge. Neither
surface is supported product API.

## Closed current source binding

The native-owned registry contains only typed current-V1 boundaries for:

- handled visibility and active Journal V1 append write;
- journal sync, checkpoint write/sync/adoption, Retry State V1 publication, and
  ordinary Manifest V1 preparation/rename/postcommit/adoption;
- committed runtime inspection and atomic durable-batch receipt resolution;
- current sole-writer rotation decision/delay; and
- the existing first-wins typed storage-pressure transition into
  `ReopenRequired` custody.

Every descriptor has one fixed source-site identity, owner, operation class,
mutation/partial-write/pressure applicability, nonzero occurrence bound, and
closed successor/terminal law. IDs are an enum rather than strings; unknown,
dynamic, wildcard, path-based, and future-V2 identities cannot be represented.

Events contain only numeric/enumerated fixed-size fields. Begin and finish are
explicit. The measured path uses no callback, channel, payload/path allocation,
environment activation, virtual filesystem, or asynchronous telemetry. Event
overflow never overwrites. Overflow, invalid nesting, lock contention, poison,
or checked arithmetic failure makes the copied snapshot structurally failed or
incomplete without changing product success, commit, or receipt semantics.

## Fault, pressure, and crash law

One plan may target exactly one nonzero occurrence with a legal pre-operation
error, applicable real nonzero partial write, or crash-after-success action.
Injected `StorageFull` and `QuotaExceeded` errors pass through existing store I/O
classification, first-wins reopen custody, runtime fail-stop health, unresolved
receipt handling, shutdown evidence, and validated reopen. Instrumentation does
not fabricate runtime pressure.

Crash-after-success first records the exact successful boundary and arms a
bounded in-memory gate. The same native executing path blocks inside boundary
finish before returning to its caller. Native code performs no control/report
file I/O and does not terminate itself. A test/harness-owned supervisor may
publish a fixed frame to an inherited pipe; the parent verifies liveness, kills,
waits/reaps, and owns
reopen and fingerprint/report work. Parent and child clocks are never combined.

## Proof obligations

Focused tests prove exact feature/default/forwarding metadata and unchanged
native closure; feature-on sessionless and observation-only equivalence of V1
bytes, inventory, inspection, and receipt outcomes; exact ordered source events
and durability-batch correlation; real pre-operation/partial-write pressure
custody for both standard pressure kinds; bounded overflow without overwrite;
registry and illegal-plan closure; and real child readiness, parent kill/reap,
and current-V1 reopen. Existing V1 rotation, recovery, pressure, and fault suites
remain required.

## Successor and hard stops

Acceptance authorizes only a separately reviewed complete private harness that
consumes this seam. Every `PR03E-M01` through `PR03E-M11` row remains
`UNSATISFIED`. No harness/report/measurement, Linux native result, accepted
workspace limit, RSS/writer-delay/eager-open/total-runtime threshold, budget, or
SLO exists. Complete measured Linux evidence and a fresh owner checkpoint still
precede any separately reviewed V2 product proposal, whose actual code must rerun
the full matrix.
