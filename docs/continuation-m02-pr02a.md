# M02-PR02a continuation: manifest-rooted registry authority

> Historical delivery record. Its durable-format compatibility and opening
> claims are superseded by the current-only V1 durable-format reset.

## Delivered authority transition

M02-PR02a makes one checksummed bounded manifest the committed description of
the still-generation-one active Journal V1 range, its exact mechanical
checkpoint cutoff, and one complete canonical `SeriesRegistry` snapshot. It
supersedes the PR01b1 premanifest boundary without changing any `och-core`
semantic or Journal V1 admission-frame byte.

`och-store::ManifestStore` is the sole blocking owner of the stable store lock,
retained active-journal lock, active journal/checkpoint, non-cloneable live
registry, two manifest slots, and three registry slots. Registry restoration
replays only public `SeriesRegistry::register`, `revise`, and `retire`, then
requires exact snapshot and canonical byte equality. Decoded frames remain
inspection evidence and are compared against restored historical authority;
they never create it.

`och-runtime` now routes register, revise, retire, current-active bind, and
append through the same bounded writer. Lifecycle and bind callers first cross a
fixed 16-request nonblocking admission bound; accepted permits are held through
the async control gate and one writer response, then reclaimed on completion,
error, or cancellation. The gate is held across that request or the complete
append-to-volatile-publication handshake, and the existing fixed-capacity channel
remains the only writer queue. No mutable registry, raw manifest, path,
descriptor, or decoded-record authority is public.

## Compatibility and bootstrap

Manifest stores retain the 28-byte journal header layout but require header
version 2. Only the two-byte header version changes; admission frames remain
Journal V1. `JournalHeaderV1::decode` rejects V2, forcing a PR01b1 binary to fail
closed after upgrade. Both stable store and existing journal locks are held while
a premanifest V1 header is rewritten and synchronized as V2 before the first
manifest is published.

An exact header-only V1 or V2 store may bootstrap an empty registry. Any
nonempty V1 or interrupted V2 premanifest store requires an explicit caller
snapshot with the configured bounds and matching `StoreId`. Public core replay
must reconstruct it exactly, and every recovered journal declaration must match
one retained `(SeriesId, DeclarationRevision)` exactly. Missing, incomplete,
altered, or differently scoped proof refuses without a manifest.

## Commit and admission law

Lifecycle success is returned only after a complete candidate snapshot is
written, synchronized, read back, decoded/replayed, published to a slot
unreferenced by both valid manifests, and named by a newly synchronized manifest.
Core refusals are non-mutating. Once candidate filesystem mutation may have
occurred, publication refusal fail-stops that live authority; it cannot report a
false commit.

New envelope authority is issued only by `SeriesRegistry::bind` against the
current active declaration. Append is deliberately historical instead: the
admission's immutable declaration must equal `resolve(series, revision)`. An
already-issued revision-one admission therefore remains appendable after a later
correction or terminal retirement, while unknown or altered history refuses
before journal bytes or volatile publication. Registry history is reachable only
on the blocking writer after synchronous resource admission; therefore such a
mismatch intentionally fail-stops the runtime and resolves both receipt stages as
`WriterStopped`. It never reports handled or durable success. A future typed
per-command rejection would be a distinct receipt/API transition, not an
implicit change in this slice.

Mechanical durable order is now append → volatile publication → journal sync →
checkpoint sync → manifest publication → durable receipt/reservation release.
`DurableCommit` carries the outer manifest commit while retaining access to the
mechanical cutoff. A checkpoint/manifest mismatch is a strict non-mutating reopen
refusal in this slice.

## Accepted evidence

Deterministic evidence covers:

- exact empty registry and genesis manifest bytes against a primitive-only
  oracle that imports no product crate or production codec;
- exact counting preflight before registry payload allocation, including a
  one-byte-under-limit refusal and exact-bound success;
- hostile magic/version/length/count/reserved/checksum/store/slot/reference and
  cutoff evidence, fixed-inventory and staging refusals, and hard limits;
- a valid foreign-store registry with an otherwise valid manifest reference and
  checksum refusing unchanged before registry authority is exposed;
- V1/V2 header-only bootstrap, required nonempty proof, incomplete proof
  refusal, exact history comparison, and V1-to-V2 old-decoder rejection;
- initial/revised/retired history replay, idempotent operations, stale/capacity
  refusal, tombstone restart, active bind refusal after retirement, and retained
  historical append authority;
- stable store locking across a real child process while the journal lock
  remains the migration backstop;
- read-only invalid-inventory refusal that preserves unrelated names and bytes
  without creating the stable lock;
- injected registry and manifest write, artifact-sync, publish, and
  directory-sync failures with no returned success, terminal live fail-stop,
  and deterministic interrupted/final-slot reopen classification;
- one hostile concurrent retirement/append ordering through the sole runtime
  control gate and writer, with both committed outcomes surviving restart;
- a held control gate with mixed lifecycle/bind callers proving the exact
  16-request admission boundary, typed overflow, FIFO revision visibility, and
  full capacity recovery;
- the existing exact journal/checkpoint/latest/retry regressions, including
  volatile-empty latest and no restored completed-retry cache.

## Exact deferred ledger

- M02-PR02b owns durable retry outcome replay, followed by a bounded
  expired/conflict guard, with a key fresh only after both tiers expire. PR02a
  neither restores nor adds a completed retry cache.
- M02-PR02c owns rotation, sealing, successor journal generations, immutable
  segments, and generic verified immutable artifact publication.
- M02-PR03a owns broad candidate fallback, convergence, repair, and structured
  corruption/recovery events. PR02a remains deliberately strict.
- M02-PR03b owns logical disk preflight, real write/sync pressure evidence, and
  degraded operation. No physical free-space guarantee is claimed here.
- A named M03 successor owns manifest-backed latest projection. Latest remains a
  volatile read optimization and restarts empty.
- Later milestones own immutable-history query, rollups, retention/reclamation,
  adapters, Studio/Engine integration, network/database layers, and operations.
- Persisted identity remains the existing `StoreId`, declaration `ProducerId`,
  and per-record producer epochs. There is no store-global producer epoch.

No dependency, unsafe code, `och-core` change, retry persistence, rotation,
latest rebuild, query behavior, or `_roadmap/` publication is part of PR02a.

## Validation

The implementation worktree passed:

- `cargo +1.98.0 test -p och-store --locked`: 39 passed;
- `cargo +1.98.0 test -p och-runtime --locked`: 49 passed;
- `cargo +1.98.0 test -p och-core --locked`: 71 passed;
- `cargo +1.98.0 test --workspace --doc --locked`: 3 passed;
- `git diff --check`;
- `./scripts/gate.sh pr`: 164 nextest tests passed with zero skipped,
  3 doctests passed, the three-native-root/five-package dependency policy
  passed, rustdoc passed, 88 repository files passed hygiene checks, and license,
  source, ban, and whitespace checks passed.

The release gate was deliberately not run because it is outside this slice.
