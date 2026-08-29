# M02-PR01b1 continuation: active-journal durability

## Live outcome

M02-PR01b1 delivers the first complete active-journal durable vertical and
supersedes the non-durable product boundary recorded by M02-PR01a/b0. The only
public runtime construction path is now async filesystem-backed
`HistorianRuntime::open(StoreOptions)`. `och-runtime` has the ordinary inward
dependency `och-runtime -> och-store -> och-core`; no package or third-party
dependency was added, `och-core` remains dependency-free, and Tokio remains the
same exact direct `rt` plus `sync` exception.

The runtime consumes only `CanonicalAdmission`, computes exact Journal V1 size
without allocating the frame, reserves one of 16 slots plus class/global bytes,
then prepares and sends FIFO work to the sole dedicated blocking writer. Handled
and durable receipt stages are independent. A slot, retry key, canonical evidence,
and encoded-byte reservation remain outstanding until a covering durable cutoff
or terminal stop. Resource priority never semantically reorders records.

`och-store` owns the fixed generation-one journal/checkpoint artifact pair,
retained writer lock, bounded scan, strict append sequence, StoreId comparisons,
append, journal synchronization, alternate checkpoint publication, and checkpoint
synchronization. The checkpoint is mechanical only. Reopen returns bounded
non-authorizing decoded evidence and latest restarts empty; no completed retry
cache, registry state, or canonical capability is reconstructed.

## Lifecycle and exact boundary

Readiness follows fixed-artifact creation/open, lock, scan, recovery convergence,
and genesis synchronization. The Tokio coordinator performs no blocking I/O. The
blocking worker is joined by one fixed reaper; Drop and cancelled shutdown signal
fail-stop without blocking, while graceful shutdown drains accepted FIFO work,
forces a final barrier, seals latest, and awaits both coordinator and reaper.

Durable ordering is append → journal sync → alternate checkpoint-slot write →
checkpoint sync → durable receipt/reservation release. Any write, sync,
checkpoint, publication, or worker fault advances no false cutoff. An invalid
durable prefix, non-progressing consecutive checkpoint, or ambiguous nonzero
checkpoint refuses. Public cutoffs distinguish mechanical checkpoint generation
from journal generation. Missing or existing zero-byte checkpoint genesis is
initialized only for an exact valid header-only journal; every nonzero wrong
checkpoint length refuses unchanged. A proven terminal invalid unacknowledged suffix
may be truncated and synchronized; a complete malformed frame with later bytes
refuses unchanged, while a proven valid suffix is synchronized and checkpointed
before readiness. Timeout barriers never cover an append awaiting publication
acknowledgement. A potentially mutating append I/O failure terminally poisons the
open journal handle until validated reopen. Every coordinator fault signals and
wakes the blocking worker even while the runtime retains another sender, so the
fixed reaper can release the writer lock without waiting for runtime Drop.

This durable claim is exact and narrow: the active journal and mechanical
checkpoint cover the named append on the qualified platform/filesystem contract.
It is not an immutable-history, manifest, long-term retry, registry-bootstrap,
or universal physical-power-loss claim.

## Exact evidence

Focused store and runtime tests cover:

- create-new/open-existing, missing/existing/layout/store mismatch, header/frame
  and declaration scope, same-process and real child-process writer exclusion;
- nonallocating exact frame preflight, prepared-length equality, count and
  protected/normal/bulk global byte boundaries, active byte/record boundaries;
- FIFO, outstanding retry equivalent/conflict precedence through durability,
  complete command recovery, cancellation without revocation, and latest rules;
- handled-before-durable staging and time/record/byte/immediate/protected/shutdown
  barrier triggers with group-sync reduction and unpublished-frame exclusion;
- injected short/partial write, journal sync, checkpoint write/sync, publication,
  task and worker failures with no false durable cutoff, including terminal
  post-write handle poisoning and coordinator fault/cancel/panic worker reaping;
- committed-prefix reopen, valid suffix adoption, terminal torn-suffix truncation,
  malformed-plus-later-candidate refusal without mutation, durable interior
  corruption refusal, recomputed-CRC non-progressing checkpoint refusal, and
  missing/zero-byte-checkpoint interrupted-genesis recovery plus nonzero-short
  checkpoint refusal without mutation;
- real child-process kill after durable receipt and after handled-before-barrier,
  followed by bounded truthful reopen with empty latest;
- graceful final barrier/join, 16-cycle observable Drop/reaper lock release, path
  and canonical-content redaction, deterministic inspection and checkpoint
  generations, StoreId scope, coordinator fault health, and preparation-rollback
  receipt wakeup.

The required local evidence commands are:

```console
cargo +1.98.0 fmt --all -- --check
cargo +1.98.0 clippy -p och-store --all-targets --locked -- -D warnings
cargo +1.98.0 clippy -p och-runtime --all-targets --locked -- -D warnings
cargo +1.98.0 test -p och-store --locked
cargo +1.98.0 test -p och-runtime --locked
cargo +1.98.0 nextest run -p och-store --locked --profile ci --no-tests=fail
cargo +1.98.0 nextest run -p och-runtime --locked --profile ci --no-tests=fail
cargo +1.98.0 test --workspace --doc --locked
git diff --check
./scripts/gate.sh pr
```

The release gate is deliberately not part of this record.

## Local platform evidence

Local evidence was collected on 2026-08-29 with Darwin 25.6.0 arm64, macOS 26.7
(build 25G220), Rust 1.98.0 (`88d9e12ae`, host
`aarch64-apple-darwin`, LLVM 22.1.8), and the repository on the journaled APFS
Data volume. It covers real standard-library file locking, directory/file sync,
cross-process exclusion, process termination, reopen, and synchronized recovery
on that target. Linux qualification remains owned by hosted PR CI. Neither this
local evidence nor hosted CI is generalized into a universal physical-power-loss
guarantee.

## PR02 handoff and deferred ledger

PR02a inherits one stable active journal generation/range, exact mechanical
checkpoint/durable cutoff, bounded path-free inspection, and a forced final
barrier. It must not create a parallel active journal path. Its next authority
transition owns manifest-backed generation publication, successor rotation and
handoff, immutable artifact/segment boundaries, and the bootstrap contract that
connects durable registry/retry state to those ranges.

Still explicitly deferred:

- registry persistence/bootstrap and manifest-backed durable retry horizon;
- successor rotation, active-generation publication, immutable artifacts,
  segment handoff, physical reclamation, and retention/priority execution;
- broad recovery/corruption/full-disk events and cross-platform qualification;
- query, rollup, network/API service, adapters, and Studio/Engine work;
- Arrow/Parquet/DataFusion or other analytical/storage dependency choices.

No deferred item is implied by the generation-one active-journal receipt.
