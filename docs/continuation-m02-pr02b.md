# M02-PR02b continuation: manifest-rooted durable retry horizon

## Delivered authority transition

M02-PR02b adds one bounded, dependency-free, durable retry projection without
changing `och-core`. The blocking `ManifestStore` remains the sole mutable
authority. It persists exact core `RetryQualification` evidence in a FIFO replay
tier followed by a FIFO expired/conflict guard. Runtime ingress holds only the
immutable committed projection used for classification.

Each replay outcome retains the original append sequence/end offset and the full
public manifest commit that first made it durable. An equivalent later
submission receives an immediately terminal replay receipt with those exact
handled and durable results and does not publish or claim current/latest state.
Replay overflow promotes the oldest outcome to a non-replayable guard;
guard-equivalent admission returns `RetryExpired`, and changed content in either
tier remains `RetryConflict`. Only eviction from both tiers makes the scope/key
fresh. Hits never refresh FIFO position.

`RetryPersistenceOptions` requires positive replay and guard capacities, caps
their combined count at 4,096, and is part of validated `StoreOptions`. The
explicit default is 256/256; focused evidence uses deliberately small limits.
There is no clock, TTL, LRU, filesystem ordering, hash-derived identity, second
queue, or second mutable retry authority.

## Atomic append handoff

The worker retains qualification and append identity only after the existing
volatile publication acknowledgement, preserving writer order. At a barrier the
store synchronizes journal/checkpoint, derives the retry candidate against the
anticipated manifest commit, counts and publishes/verifies Retry State V1,
publishes Manifest V2 naming the exact cutoff/registry/retry state, and returns
the committed immutable projection.

Runtime then performs one bounded transition under the existing ingress mutex.
It verifies the complete pending range and exact projected transition, installs
the retry snapshot, resolves every covered durable receipt and releases its
reservation, and only then wakes waiters. Outstanding equivalent requests share
their original receipt through that transition. A failure after possible store
mutation remains terminal fail-stop and installs neither a false durable result
nor an uncommitted projection.

Pending evidence is rejected in constant time before traversal or tier copying
unless its count is positive, at most 4,096, and exactly spans the newly durable
append suffix. Once journal synchronization advances the cutoff, every later
transition, generation, slot, encoding, publication, or cleanup refusal
terminally faults the live store.

## Format and compatibility boundary

Retry State V1 uses three reusable final slots and one staging name, magic
`OCHRET01`, a versioned/checksummed 64-byte header, exact configured capacities
and counts, ordered replay then guard payloads, and a 2 MiB hard ceiling. The
fixed nonrecursive inventory bound is exactly 14. Every candidate retry slot is
unreferenced by both valid manifests. Decode is bounded, rejects duplicate
scope/key and noncanonical order/outcome coverage, and requires exact canonical
re-encoding.

Open also validates every referenced retry artifact, including an older valid
manifest candidate, under its exact owning commit. Retained sequences form one
contiguous suffix ending at the root cutoff, guards imply a full replay tier,
embedded outcomes cannot be newer than the root, entries sharing a retry
generation share one exact commit, generation transitions follow publication,
and only generation one may be empty. Thus recomputed checksums cannot authorize
future outcomes or unreachable tier shapes.

Manifest V2 preserves the 128-byte size and bytes 0..92 of Manifest V1. Bytes
92..124 name the retry slot, generation, length, and checksum with reserved-zero
gaps; bytes 124..128 remain the manifest checksum. V1 bytes and decode remain
supported. Public `ManifestCommit` exposes an optional retry slot/generation:
absent for legacy V1 and concrete for V2. Replay outcomes retain only that public
identity, so an originating manifest is exact without retry checksum recursion.

New-store genesis publishes an empty retry snapshot and Manifest V2. A valid
legacy V1 manifest opens with empty tiers and no history scan or backfill.
Registry-only commits can remain V1 until the first new durable append, which
publishes retry generation one and V2; every later manifest preserves a retry
reference. Pre-PR02b keys therefore keep the former no-restart-horizon contract
until new V2 completions establish them. V2 reopen restores the referenced
snapshot directly rather than reconstructing it from retained journal frames.

Invalid, foreign, staged, options-mismatched, or unreferenced retry evidence
refuses strictly. Old PR02a binaries fail closed on Manifest V2 or retry inventory.
Stable store and active-journal locks remain unchanged. This implementation was
written independently; no donor source or wire format was copied.

## Accepted evidence

Deterministic evidence covers:

- exact empty and one-replay Retry State V1 plus Manifest V2 bytes against a
  primitive-only oracle that imports no product crate or production codec;
- exact byte counting before payload allocation, exact-bound success, one-under
  refusal, 2 MiB/count limits, and hostile magic/version/header/declared length,
  reserved/checksum/store/slot/generation/capacity/count/order/trailing evidence;
- in-process and reopen replay, changed-content conflict, replay promotion,
  exact `RetryExpired`, guard conflict, FIFO guard eviction, fresh admission only
  after both tiers, and non-refreshing hits;
- durable replay precedence over count and exact-byte saturation while existing
  outstanding coalescing/cancellation behavior remains unchanged;
- exact original replay outcome after later registry/manifest commits reuse both
  manifest slots, without latest mutation or rebuild;
- V2 genesis/reopen, legacy V1 no-backfill and first-append V2 transition,
  `StoreId`/options mismatch, and invalid/unreferenced snapshot refusal;
- constant-time pending-count/delta boundaries, post-sync transition fail-stop,
  and checksummed future, gapped, non-full, same-generation-divergent, skipped-
  generation, empty-generation, current-root, and older-root semantic refusals;
- retry write, artifact-sync, readback, publish, directory-sync, and following
  manifest publication faults with no false returned commit/projection, while
  prior registry/manifest/journal fault regressions remain intact.

## Exact deferred ledger

- M02-PR02c owns rotation, sealing, successor journal generations, immutable
  segments, and generic verified immutable artifact publication.
- M02-PR03a owns broad manifest/retry candidate fallback, convergence, repair,
  and structured corruption/recovery events. PR02b remains deliberately strict.
- M02-PR03b owns logical disk preflight, real write/sync pressure evidence, and
  degraded operation. No physical free-space guarantee is claimed here.
- A named M03 successor owns manifest-backed latest projection. Latest remains a
  volatile read optimization and restarts empty; retry replay never mutates it.
- Later milestones own immutable-history query, rollups, retention/reclamation,
  adapters, Studio/Engine integration, network/database layers, and operations.
- An unbounded, time-based, or externally coordinated idempotency horizon is not
  implied by this bounded two-tier contract.

No dependency, unsafe code, `och-core` change, rotation, latest rebuild, query
behavior, donor source, or `_roadmap/` publication is part of PR02b.

## Validation

The implementation worktree passed:

- `cargo +1.98.0 test -p och-store --locked`: 51 passed;
- `cargo +1.98.0 test -p och-runtime --locked`: 53 passed;
- `cargo +1.98.0 test -p och-core --locked`: 71 passed;
- `cargo +1.98.0 test --workspace --doc --locked`: 3 passed;
- `git diff --check`;
- `./scripts/gate.sh pr`: 180 nextest tests passed with zero skipped,
  3 doctests passed, the three-native-root/five-package dependency policy
  passed, rustdoc passed, 92 repository files passed hygiene checks, and license,
  source, ban, and whitespace checks passed.

The release gate was deliberately not run because it is outside this slice.
