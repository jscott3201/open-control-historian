# M02-PR03b1 store-only pressure custody continuation

## Delivered boundary

`och-store` now exports one copyable `StoreWriteState` used by direct active and
composed manifest inspection. `StorageFull` and `QuotaExceeded` from owned
mutation boundaries produce typed first-failure evidence; raw OS codes remain
diagnostic only. A live handle then remains `ReopenRequired`, while non-pressure
mutation failures remain distinct `Faulted` custody. Inspection stays usable and
contains no path, content, string, or handle.

Manifest wrapping preserves journal-owned pressure as
`ManifestStoreError::Active(ActiveJournalError::StoragePressure(..))`; manifest
publication pressure uses its own evidence. Later append, synchronization,
rotation, lifecycle, bind, and authorization calls return store-level
`ReopenRequired` before I/O or registry mutation. Open/genesis/recovery failures
that cannot return a handle are still typed on the original error.

## Preflight and evidence

Active append/checkpoint work and manifest durability, lifecycle, rotation,
genesis, and recovery prepare all knowable exact bounded records, generations,
slots, and relationships before their first mutation. Logical bound/refusal paths
remain writable and mutation-free. Tests cover the classification matrix,
partial write, journal/checkpoint barriers, active-to-manifest propagation,
registry and manifest publication stages, lifecycle and postcommit rotation
cleanup, recovery truncate/sync, genesis, hostile repetition, sanitized
inspection, no false cutoff/commit, and reopen convergence/refusal.

No physical capacity probe or reservation exists, and no V1 byte or inventory
contract changed. Default tests use deterministic fault seams; no `/dev/full` or
provisioned-filesystem claim is required.

## Deferred successor

M02-PR03b2 owns runtime degraded health, latest preservation, receipt, shutdown,
and recovery policy. Stale-restore custody, broad repair, retention/reclamation,
segments, query, providers, adapters, and platform-wide physical guarantees also
remain absent.
