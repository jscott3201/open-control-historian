# M02-PR03a implementation brief: conservative manifest-root recovery

## Bounded outcome

M02-PR03a completes only the recovery/corruption half of PR03. On open,
`och-store` must either prove the newest committed root and perform one narrowly
bounded suffix recovery, or refuse without mutation. It does not add disk-space
preflight, ENOSPC normalization, degraded/read-only runtime health, stale-restore
acceptance, retention, reclamation, or a repair command.

`och-core`, Journal V1 frames, active-header V2, checkpoints, registry snapshots,
Retry State V1/V2, Generation Catalog V1, raw seals, prior Manifest V1/V2/V3
bytes, retry comparison, and returned embedded `ManifestCommit` evidence remain
unchanged.

## Authority and non-abandonment law

The stable store lock remains the first mutable ownership boundary. Both
manifest slots are read into independent outcomes before candidate selection:
missing, valid, corrupt, unsupported, identity-mismatched, or I/O-refused. Any
damaged possible-newer outcome refuses. One older parseable root is not fallback
authority, including when the active cutoff is equal and only registry/retry/
catalog metadata may have advanced.

The selected newest manifest is not actionable until all of the following prove
exactly:

- manifest generation/reference progression;
- complete registry replay/re-encoding and every retained historical declaration;
- retry slot, capacities, canonical bytes, owning root, FIFO replay/guard shape,
  and every embedded original commit;
- catalog bytes, prefix/advance law, sealed artifact length and header metadata,
  and active/sealed generation inventory;
- active StoreId/header version, generation/floor, exact checkpoint slot and
  manifest cutoff; and
- any prior Recovery State V1 reference and the narrow rotation law.

Decoded journal, retry, catalog, or sidecar bytes never authorize semantic state.
Identity mismatch and stale restore share a deterministic fail-closed diagnostic
class because this repository has no custody evidence that could distinguish or
accept a restore.

## Root-scoped active scan and mutation

Manifest open uses a new internal root-scoped active policy, not permissive
premanifest `Converge`. It reads and locks the active pair, requires the exact
manifest checkpoint slot, scans bounded Journal V1 bytes against that cutoff,
and returns a pending recovery plan without mutation. Manifest code validates all
remaining authority before applying the plan.

The scan may classify bytes only strictly after the manifest end. A valid suffix,
a torn final suffix, or one malformed final frame candidate is removable. Any
committed/interior corruption, boundary-straddling frame, identity/sequence
mismatch, or malformed candidate followed by later bytes refuses unchanged. The
accepted action truncates and synchronizes exactly to the manifest end. If a
strictly newer mechanical checkpoint slot exists, it is cleared and synchronized;
the selected checkpoint is never advanced or reinterpreted. Recovered records
expose only the committed prefix.

## Durable report and Manifest V4

Recovery State V1 is one exact 96-byte, dependency-free, CRC-32C-protected record
in three reusable slots plus one staging artifact. It carries only StoreId,
recovery/source-manifest/journal/checkpoint generations, append/end cutoff,
removed-byte count, closed class/action tags, and a bounded operation count. It
contains no path, raw content, handle, free-form string, or history. The exact
layout is in [Recovery State V1](recovery-state-v1-format.md).

Manifest V4 is exactly 192 bytes. It preserves V1/V2/V3 decoding and bytes, the
V2 retry body, and the optional V3 catalog body, then binds recovery slot,
generation, exact 96-byte length, checksum, reserved zeros, and a checksum over
bytes 0..188. Old binaries fail closed on the new version/inventory. The exact
layout is in [Manifest V1/V2/V3/V4](manifest-v1-format.md).

Accepted publication order is suffix synchronization, optional stale mechanical
slot clearing, Recovery State staging/sync/readback/canonical comparison/rename/
directory sync, then Manifest V4 staging/sync/readback/canonical comparison/
rename/directory sync. V4 is the only report commit point. Ordinary append,
registry, retry, and rotation manifests preserve the reference. A later recovery
may advance it by exactly one into a different slot.

Faults before V4 may reopen to the prior root or typed non-mutating interrupted
publication. A renamed valid V4 reopens to one recovered root. Completed recovery
does not repeat. A precommit staging/final recovery candidate remains
non-authoritative and does not permit guessing or silent cleanup.

## Diagnostics and compatibility

Public additions are immutable and additive:

- non-exhaustive `RecoveryClassification` and `RecoveryAction`;
- non-exhaustive `RecoveryReport` with primitive accessors;
- `ManifestStoreInspection::recovery_report`;
- `RuntimeInspection::recovery_report`;
- `ManifestStore::open_with_diagnostics` and `ManifestStoreOpenError`; and
- classification accessors on legacy store/runtime startup errors.

Existing `ManifestStore::open`, `HistorianRuntime::open`, `ManifestStoreError`,
`StartError`, and `RuntimeHealth` signatures/variants are unchanged. Successful
runtime recovery remains `Healthy`; a fresh no-recovery store reports no recovery
report.

## Bounds and evidence plan

The recognized inventory rises from 85 to 89 fixed files. Recovery bytes are
fixed at 96; Manifest V4 is fixed at 192; there remain two manifest and three
metadata slots, at most 4,096 active records, at most 512 MiB configured active
bytes, and at most 64 sealed generations. Normal open reads sealed length/header
metadata only and never scans sealed payloads.

Focused evidence covers independent primitive oracles, hostile lengths/version/
reserved/checksum/trailing bytes, valid/torn suffixes, checkpoint-ahead suffixes,
committed/interior and ambiguous suffix corruption, damaged/missing/unsupported
possible-newer manifests, invalid referenced authority before mutation, retry
receipt preservation, one-time report persistence across append/rotation, runtime
health/report forwarding, and deterministic suffix/report/manifest fault points.
The final worktree runs store/runtime/core tests, doctests, strict package clippy,
format/diff checks, and `./scripts/gate.sh pr`. The release gate is explicitly not
part of this slice.

## Deferred boundary

M02 remains incomplete. PR03b owns disk-pressure preflight and normalization,
degraded/read-only/reopen-required behavior, and pressure policy. Stale restore
remains refused until later custody evidence exists. M03 remains blocked; this
slice does not claim production readiness, universal power-loss behavior,
Windows qualification, physical free-space evidence, or adversarial directory
writer safety.
