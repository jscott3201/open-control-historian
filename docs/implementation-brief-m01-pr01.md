# M01-PR01 runtime lifecycle contract and implementation brief

## Outcome

This delivery adds `och-runtime` as a second default native root without changing
`och-core`. It provides the smallest real caller-executor Tokio substrate: one
private mutable writer task per `HistorianRuntime`, readiness after writer-state
initialization, consuming graceful shutdown and join, and nonblocking abort-only
Drop. Multiple instances are independent.

## Public contract

- `HistorianRuntime::start().await` uses `Handle::try_current` and returns
  `StartError::NoActiveRuntime` rather than panicking when no caller Tokio runtime
  is active.
- Startup returns only after the private writer initializes and reports
  readiness. Cancelling startup drops a guard that aborts the retained task.
- `shutdown(self).await` sends the sole private shutdown signal and keeps the
  join handle owned while awaiting it. `Ok(())` means normal task termination was
  joined. With no data commands, drain is vacuous and proves no durability or
  ingress property.
- Cancelling shutdown drops the still-owned handle, whose Drop requests abort.
  Plain handle Drop is also nonblocking, best-effort abort only and promises no
  graceful completion.
- Closed `StartError` and `ShutdownError` values distinguish early exit,
  cancellation, and panic without exposing Tokio's `JoinError` or panic payload.
  The release profile uses `panic=abort`, so panic classification is debug/test
  evidence rather than a release recovery mechanism.
- A failed or stopped instance has no restart operation; callers construct a new
  independent instance.

## Dependency and policy contract

`och-runtime` depends directly on Tokio 1.53.1 with default features disabled and
only `rt` and `sync`. It has no `och-core` dependency because this slice has no
model command. Policy schema 2 keeps `tokio` forbidden globally, proves
`och-core` dependency-free, and admits only the exact direct resolved package
edge `och-runtime -> tokio`. The structured exception also requires one normal,
non-optional, unconditional manifest declaration with defaults disabled and
exactly `rt` plus `sync`, then verifies that Tokio's resolved unified feature set
is exactly the same. Aliases do not evade identity checks; malformed, duplicate,
unused, transitive, declaration-mismatched, resolved-feature-broadened,
non-native-source, and non-forbidden-target exceptions fail closed.

## Deterministic evidence

Current-thread Tokio unit tests use private `cfg(test)` gates and bounded yields,
with no clocks, macros, network, randomness, filesystem writes, or external
processes. They cover absent executor, readiness ordering, startup and shutdown
cancellation cleanup, graceful joined cleanup, plain Drop, early exits, task
cancellation and panic mapping, two isolated instances, and repeated hostile
sequences. Policy fixtures and actual-workspace integration tests cover the
exception laws and exact native graph. Platform evidence is the current-thread
harness on the local host plus the repository's hosted Linux PR gate; this slice
makes no multi-thread-runtime, WASM, or other platform compatibility claim.

## M01-PR02 evolution

M01-PR01's vacuous-drain and no-core-dependency statements below describe that
historical delivery boundary. M01-PR02 now adds the reviewed inward
`och-runtime -> och-core` edge, fixed bounded envelope ingress, shared terminal
receipts, and real graceful draining while preserving caller executor ownership,
one private writer, startup readiness, sanitized lifecycle errors, joined
shutdown, and abort-only Drop. Receipt handling still proves no durability.

## Excluded from M01-PR01

There is no public writer command, observation/envelope ingress, queue capacity,
backpressure/coalescing, receipt, retry durability, state publication, registry,
storage, journal, persistence, query, wire/serialization, hashing, UUID generation,
adapter, library-owned executor/thread, blocking lifecycle API, or compatibility
claim. The [M01-PR02 delivery record](continuation-m01-pr02.md) now owns the
implemented ingress contract, while [M01-PR03](continuation-m01-pr03.md) owns the
next publication/read boundary.
