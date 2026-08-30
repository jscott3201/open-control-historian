# M02-PR03a continuation: conservative recovery and bounded evidence

## Delivered boundary

M02-PR03a adds store-owned conservative recovery without disk-pressure behavior.
Open now reads both manifest slots into independent classifications and never
uses an older parseable root when a missing, corrupt, unsupported,
identity-mismatched, or semantically invalid possible newer authority remains.
Equal active cutoffs do not authorize fallback because registry, retry, catalog,
and report transitions can advance independently.

After newest-root selection, store validates the complete registry, retry state,
catalog and sealed metadata/header inventory, active StoreId/header/checkpoint/
cutoff, retained declarations, prior recovery report, and narrow rotation law
before recovery mutation. Decoded suffix or sidecar evidence never authorizes
state. Identity mismatch/stale restore remains one fail-closed class.

## Recovery and durable formats

Root-scoped active open retains the existing journal ownership seam but no longer
uses permissive global convergence for manifest stores. It scans against the
manifest cutoff read-only, removes and synchronizes only bytes strictly after
that cutoff, and clears only a strictly newer mechanical checkpoint slot. It
does not adopt valid suffix frames or checkpoint forward. Committed/interior
corruption and malformed suffix evidence followed by later bytes refuse
unchanged.

Recovery State V1 is exactly 96 bytes in three reusable slots. Its fixed fields
are store/recovery/source-root/journal/checkpoint generations, exact cutoff,
removed bytes, closed class/action tags, bounded operation count, reserved zeros,
and CRC-32C. Manifest V4 is exactly 192 bytes and binds that artifact after the
unchanged V2 retry and optional V3 catalog bodies. Exact V1/V2/V3 decoding and
old-writer fail-closed behavior remain.

Publication is active truncate/sync, optional stale checkpoint clear/sync,
recovery state publish, then Manifest V4 publish as the only commit point.
Precommit fault evidence yields the prior root or typed non-mutating refusal; a
renamed V4 yields exactly one recovered root. Ordinary append, registry, retry,
and rotation publications preserve the report reference. Completed action does
not repeat on reopen, and original retry outcomes retain their embedded commit
semantics.

## Diagnostics and runtime

Store exposes non-exhaustive immutable recovery class/action/report types,
`ManifestStore::open_with_diagnostics`, and additive classification accessors.
Reports contain no paths, raw observations/declarations/retry content, handles,
or strings. `ManifestStoreInspection` forwards the report through `WorkerReady`
and `InspectionShared` into `RuntimeInspection`. Successful runtime recovery is
still `RuntimeHealth::Healthy`; fresh stores report no recovery action. Existing
open signatures and exhaustive error/health variants are unchanged.

## Bounds and deterministic evidence

- recognized inventory: at most 89 fixed-pattern files;
- manifests: two reusable slots; V1/V2 128 bytes, V3 160 bytes, V4 192 bytes;
- recovery: three reusable 96-byte slots and one staging name;
- active scan: configured maximum at most 512 MiB and 4,096 records;
- sealed history: at most 64 catalog entries; normal open reads only sealed
  metadata and 28-byte headers, never sealed payloads; and
- retry and registry capacities/comparison semantics are unchanged.

Focused tests cover canonical/hostile Recovery State V1 and Manifest V4 with an
independent primitive-only oracle; valid and mechanically checkpointed suffix
rollback; retry-authority refusal before mutation; missing/corrupt/unsupported
equal-cutoff successors; receipt/replay preservation; report persistence through
append and rotation; runtime `Healthy` forwarding; suffix synchronization fault
points; and recovery-state/manifest publication fault points. Existing lock,
rotation, retry, registry, child-process, latest-empty restart, no-false-commit,
catalog-capacity, and sealed-open-bound regressions remain in the PR gate.

The exact final local command outcomes are returned in the worktree result. The
required commands are Rust 1.98.0 format, strict `och-store` and `och-runtime`
clippy, all three native package tests, workspace doctests, `git diff --check`,
and `./scripts/gate.sh pr`. The release gate was not requested and must not be
claimed.

## Deferred ledger

M02 is not complete. PR03b still owns disk-space preflight, ENOSPC normalization,
degraded/read-only/reopen-required health, ingress throttling/shedding, and
pressure policy. Stale restore acceptance and automatic producer-epoch changes
remain blocked on future custody evidence. Final native segments, query, latest
reconstruction, retention/reclamation, adapters, Studio/Engine/provider work,
and an unbounded/time-based retry horizon remain absent. M03 is not unblocked by
this slice.

The platform statement remains standard-library same-directory file creation,
sync, rename, directory sync, and retained file locking. It does not claim
universal power-loss durability, macOS `F_FULLFSYNC`, Windows qualification,
physical free-space behavior, or safety against an adversarial directory writer.
