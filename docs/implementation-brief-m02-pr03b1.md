# M02-PR03b1 store-only pressure custody implementation brief

## Objective

Add dependency-free deterministic logical transaction preflight and typed
observed storage/quota pressure to public `ActiveJournal` and composed
`ManifestStore`. The first normalized mutating-boundary pressure error returns
bounded operation evidence and puts the live handle in sticky `ReopenRequired`
custody. Validated current-V1 reopen is the only recovery path.

## Contract

- Normalize only `ErrorKind::StorageFull` and `ErrorKind::QuotaExceeded` at
  store-owned create/write/resize/truncate/sync/rename/publish/remove boundaries.
- Retain optional raw OS codes only as diagnostics; raw code never controls flow.
- Keep reads, metadata, seeks, locks, and open-existing failures generic.
- Expose `Writable`, `ReopenRequired`, or `Faulted` through path-free active and
  manifest inspection.
- Preserve exact first pressure evidence (`Active(StoragePressure(..))` for
  journal-owned work); all later mutation/authorization calls return the
  no-evidence store-layer `ReopenRequired` before I/O or model mutation.
- Precompute every bounded exact frame/checkpoint/registry/retry/catalog/recovery/
  manifest record, slot, generation, and transaction relationship knowable before
  the transaction's first mutation.

Logical preflight is not a physical free-space, block, quota, inode, device, or
future-availability promise. It adds no aggregate quota. Pressure custody is
volatile and changes no Store/Journal/Manifest/Retry/Catalog/Recovery V1 bytes,
versions, artifacts, inventory bounds, or authority.

## Recovery and handoff

Pressure never proves rollback or absence after write, sync, rename, or cleanup.
The handle therefore cannot retry or clear itself. After external remediation and
drop, open either converges through the existing PR03a law or refuses unchanged.
Runtime degraded/latest/receipt/shutdown policy remains M02-PR03b2; current runtime
generic fail-stop mapping is intentionally unchanged.
