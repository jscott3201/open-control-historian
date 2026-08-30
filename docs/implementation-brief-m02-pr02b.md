# M02-PR02b implementation brief: durable two-tier retry horizon

## Exact baseline and authority

- Repository: `open-control-historian`
- Base: `2d74126191583fed8ee580eb47f88d5c87c167e9`
- Delivery branch: `feat/m02-pr02b-durable-retry`
- Prerequisites: accepted M02-PR01a, M02-PR01b0, M02-PR01b1, and M02-PR02a
- Owner decision: durable retry uses an exact outcome replay tier, then a
  bounded expired/conflict guard; a scope/key becomes fresh only after both
  tiers have evicted it.

## One objective

Add one dependency-free, manifest-rooted, bounded durable retry authority. The
blocking `ManifestStore` remains the sole mutable authority. Runtime ingress
holds only an immutable committed projection used for exact classification and
installs a newly committed projection atomically with receipt completion.

## Retry contract

- `RetryPersistenceOptions` requires positive replay and guard capacities,
  limits their sum to `4,096`, and limits one exact retry snapshot to `2 MiB`.
  Runtime `StoreOptions` carries the validated options; the explicit default is
  256 replay entries and 256 guard entries.
- Ordering is FIFO by durable append completion/append sequence. It uses no
  clock, TTL, LRU refresh, hash-derived identity, or filesystem order. Replay,
  conflict, and expired hits never refresh an entry.
- A replay entry retains the exact `RetryQualification`, original append
  identity, and full public manifest-backed durable commit that first made the
  outcome durable. Replay overflow promotes the oldest entry to a guard that
  retains the exact qualification and original append sequence/order, but no
  replayable receipt. Guard overflow expires the oldest entry.
- Classification uses `RetryQualification::classify` exactly. Existing ingress
  precedence remains: closed, store mismatch, Journal V1 framing/measurement,
  outstanding retry, durable replay, guard, command-count capacity, then byte
  capacity. Outstanding equivalent requests continue sharing the original
  receipt through the durable batch transition.
- Durable replay returns `SubmissionDisposition::Replayed` with an immediately
  terminal receipt carrying the original append identity and exact original
  durable commit. It makes no current/latest-state claim. Guard-equivalent
  admission refuses with typed `RetryExpired` and retains the command; changed
  content in either retained tier remains `RetryConflict`.
- The writer is the only mutator. Ingress never owns a second mutable retry
  queue, writer, cache, or lifecycle authority.

## Atomic publication and handoff

- The writer retains retry qualification and append identity only after the
  existing `Published` acknowledgement and orders pending completions FIFO.
- A barrier synchronizes journal and checkpoint, derives a bounded retry
  candidate using the anticipated manifest commit, exactly counts and
  publishes/verifies its snapshot, publishes Manifest V2 naming the cutoff,
  registry, and retry state, and only then returns the committed manifest and
  immutable retry projection.
- Runtime replaces per-slot durable completion with one bounded batch transition
  under the existing ingress mutex. It verifies every covered awaiting
  slot/outcome, installs the committed projection, resolves all receipts,
  releases reservations, and only then wakes waiters. There is no successful
  durable receipt without replay authority.
- Any failure after possible store mutation remains terminal fail-stop and
  installs neither a false durable result nor a retry projection.

## Format, inventory, and compatibility law

- Add exactly three reusable `retry-state-v1-slot-{0,1,2}.och` artifacts plus
  `retry-state-v1.staging`. A candidate slot is unreferenced by both valid
  manifests. The fixed nonrecursive inventory bound rises exactly from 10 to 14.
- Retry State V1 uses magic `OCHRET01`, versioned checksummed big-endian bytes,
  `StoreId`, snapshot generation, configured capacities, exact ordered replay
  and guard counts, reserved-zero fields, and the hard count/byte bounds. Exact
  bytes are counted before payload allocation. Decode is bounded and canonical:
  re-encode/equality is required, duplicate scope/key across or within tiers is
  refused, and sequence/order/outcome coverage must be canonical.
- Pending append evidence is preflighted in constant time before traversal or
  retained-tier copying: it is nonempty, at most `4,096`, and exactly equals the
  append-sequence delta. Every retry snapshot referenced by either valid
  manifest candidate must additionally be reachable under that exact owning
  root: bounded embedded commits, a contiguous retained suffix, full replay
  before any guard, exact within-generation commit equality, canonical
  cross-generation progress, newest outcome/root equality, and an empty state
  only at retry generation one.
- Manifest V2 remains exactly 128 bytes and preserves Manifest V1 bytes and
  decoding. Offsets 0..92 are unchanged; byte 92 is the retry slot, bytes 93..96
  are zero, 96..104 are retry generation, 104..112 are retry length, 112..116
  are retry checksum, 116..124 are zero, and 124..128 remains the manifest
  checksum. V2 requires a valid referenced retry state and exact configured
  capacities. V1 has no retry reference and restores empty tiers without
  scanning/backfilling prior Journal V1 history.
- New-store genesis publishes and verifies an empty retry snapshot and Manifest
  V2. A valid legacy V1 manifest may open with empty tiers. Its first durable
  append publishes retry generation one and Manifest V2. Registry-only commits
  may remain V1 until then; after V2, every manifest retains a retry reference.
- Public manifest commit/inspection exposes an optional retry reference for
  legacy V1 and concrete metadata for V2. New replay outcomes are V2 and refer
  to their originating manifest by known generation/slot fields without
  checksum recursion.
- Old PR02a binaries fail closed once retry artifacts or Manifest V2 exist.
  Stable and journal locks remain unchanged. Reopen restores V2 retry state
  without a new retained-history scan. Invalid, foreign, unreferenced, or staged
  retry evidence refuses strictly; PR03a owns fallback, repair, and convergence.

## Required evidence

- A primitive-only oracle independently proves Retry State V1 and Manifest V2
  bytes without importing production codec logic.
- Hostile parsing covers magic, version, header and declared lengths, reserved
  bytes, checksum, store/slot/generation/capacity/count/order/trailing data, plus
  exact-size and one-over allocation preflight.
- In-process and reopen matrices prove replay, changed-content conflict,
  promotion, exact `RetryExpired`, guard conflict, FIFO guard eviction, freshness
  only after both tiers, and non-refreshing hits.
- Saturation precedence, outstanding coalescing through the atomic transition,
  and cancellation retention remain deterministic.
- Original outcomes remain exact after later registry/manifest commits reuse
  both manifest slots; no latest state is mutated or rebuilt.
- Compatibility evidence covers V1 no-backfill, first V1-to-V2 durable append,
  V2 genesis/reopen, store/options mismatch, and invalid or unreferenced retry
  artifacts.
- Retry write, sync, readback, publish, directory-sync, and subsequent manifest
  publication faults return no false receipt/projection and preserve the prior
  registry, manifest, and journal fault regressions.
- Any refusal after journal sync advances the cutoff terminally faults the live
  store, including transition, generation, candidate, encoding, publication,
  and cleanup errors.

## Tracked records

Update `AGENTS.md`, `README.md`, `docs/architecture.md`,
`docs/model-contract.md`, and `docs/manifest-v1-format.md`; add
`docs/retry-state-v1-format.md` and `docs/continuation-m02-pr02b.md`. Donor
behavioral inspiration is recorded only if code is copied. This implementation
is independent and rejects donor identity, format, xxhash dependency,
changed-content-as-new policy, and missing guard behavior.

## Explicit exclusions

- no `och-core` changes;
- no dependency or unsafe-code additions;
- no rotation, sealing, successor journal, immutable segment, or artifact
  publication behavior;
- no broad manifest fallback, convergence, recovery event, or repair behavior;
- no disk-pressure policy, latest rebuild, query, retention, adapter,
  Studio/Engine, or `_roadmap/` change.

## Invariants and replan triggers

Preserve every existing core, registry, journal, checkpoint, latest, and volatile
outstanding-retry semantic. Stop before broadening if the atomic batch handoff
requires a second mutable authority or queue, V1 history backfill, rotation or
broad recovery, a dependency or unsafe code, unbounded input/history/allocation,
an outcome that cannot be serialized truthfully within bounds, a candidate slot
referenced by either valid manifest, or more than this one source-of-truth
transition.

## Acceptance commands

```console
cargo +1.98.0 test -p och-store --locked
cargo +1.98.0 test -p och-runtime --locked
cargo +1.98.0 test -p och-core --locked
cargo +1.98.0 test --workspace --doc --locked
git diff --check
./scripts/gate.sh pr
```

The release gate is outside this slice.
