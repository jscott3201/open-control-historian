# M02-PR03b2 runtime pressure lifecycle implementation brief

## Objective

Consume M02-PR03b1 store custody without changing store or durable semantics. On
the first already-typed active-journal or composed-manifest pressure error, the
runtime retains bounded path/content-free evidence and the composed inspection,
sets `RuntimeHealth::StoragePressure`, and then fail-stops ingress before any
receipt or control response is released.

## Public contract

- `RuntimePressureEvidence` copies only source family, the existing store
  operation enum, `std::io::ErrorKind`, and optional raw OS error.
- `RuntimeInspection` exposes composed `StoreWriteState`, first-wins pressure
  evidence, reservation counts, and sticky pressure health.
- Production evidence is projected only from
  `ManifestStoreError::StoragePressure` or nested
  `ActiveJournalError::StoragePressure`; raw codes do not control health and
  `ReopenRequired` cannot fabricate evidence.
- Generic failures remain `Faulted`, catalog exhaustion remains
  `RotationRequired`, and graceful shutdown remains `Stopped`.

## Ordering and lifecycle

Every live store-terminal path copies the latest composed inspection and records
terminal health before idempotent ingress stop, then sends any append, barrier,
registry, bind, or shutdown response. Stop preserves already resolved handled and
durable outcomes, resolves only unresolved stages as `WriterStopped`, releases all
reservations, and makes future latest capture unavailable. Caller-held immutable
snapshots remain usable.

Consuming shutdown gives Tokio panic/cancellation truth precedence. For an
ordinary writer exit with retained pressure, it drops the store sender, awaits the
existing fixed reaper and lock release, and returns
`ShutdownError::StoragePressure` with the exact retained evidence. Drop remains
nonblocking.

## Deterministic seam and exclusions

One runtime-private `cfg(test)` hook injects an already-classified pressure event
at append, flush, or registry response boundaries and can hold the blocking worker
after response. It performs no I/O classification and is absent from production.
There is no store production change, physical capacity probe, durable pressure
state, V1 byte/artifact change, new queue/writer/dependency, receipt/latest enum
change, retry/clear API, latest reconstruction, or continued degraded-ingress mode.
