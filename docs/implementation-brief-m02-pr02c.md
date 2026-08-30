# M02-PR02c implementation brief: bounded journal rotation and sealing

> Historical delivery record. Its durable-format compatibility and opening
> claims are superseded by the current-only V1 durable-format reset.

## Exact baseline and authority

- Repository: `open-control-historian`
- Base: `2bf1c3aab066c8732302b4b26061f732e81b7b8f`
- Delivery branch: `feat/m02-pr02c-rotation-seal`
- Prerequisites: accepted M02-PR01a, M02-PR01b0, M02-PR01b1,
  M02-PR02a, and M02-PR02b
- Owner decision: rotation is one dependency-free `och-store` authority
  transition; the runtime may demand it only at the existing sole-writer,
  publication-safe boundary.

## One objective

Replace terminal active-journal `RotationRequired` behavior with one bounded,
manifest-rooted transition that seals the exact fully durable active range,
publishes a successor active journal, and preserves every already returned
durable and replay outcome exactly. This is a raw Journal V1 archival fence,
not the native immutable segment or query contract planned for M03.

## Identity, range, and receipt law

- One strictly increasing append sequence is store-global and continues across
  active journal generations. Journal end offsets and checkpoint generations
  are local to their generation and reset only with a successor.
- Generation one retains the existing fixed active artifact names. Successor
  generations use deterministic, bounded, generation-derived names; no caller
  path or text enters artifact identity.
- `ActiveJournal` create/open binds the exact journal generation and exclusive
  append-sequence floor. A frame that cannot fit an empty active journal is a
  typed refusal and never starts an empty rotation loop.
- Each durable or replayed receipt retains its exact original
  `ManifestCommit`. Rotation neither rewrites those commits nor emits a fake
  append outcome. Latest state is never mutated by rotation and still restarts
  empty.

## Sealed Journal V1 artifact and catalog

- A sealed artifact is an immutable raw Journal V1 range, not a queryable
  native segment. It is streamed from the exact durable source cutoff with
  fixed memory, synchronized, read back through bounded framing validation,
  and published without later product mutation.
- Readback verifies `StoreId`, source journal generation, exclusive sequence
  floor, inclusive sequence cutoff, source end cutoff, every Journal V1 frame,
  declaration-history resolution, artifact length, and complete-byte CRC-32C.
  No public API exposes a path, mutable descriptor, or payload handle.
- Generation Catalog V1 is fixed-width, checksummed, canonically sorted, and
  capped at 64 sealed generations. Each entry binds source journal generation,
  exclusive sequence floor and inclusive cutoff, source end cutoff, registry
  generation coverage, sealed artifact length/checksum, and closed format.
- Catalog-full rotation refuses without mutation, retains all artifacts, never
  reclaims sealed history, and closes or requires reopen according to the
  existing terminal capacity contract.
- Inventory remains nonrecursive, recognized, and hard bounded. Normal open
  reads only bounded names, catalog/header metadata, active data, and already
  bounded authority artifacts; it does not scan sealed payload bytes.

## Manifest V3 and retry compatibility

- Manifest V1 and V2 remain exactly 128 bytes and preserve their existing
  decode/open behavior. Manifest V3 is exactly 160 bytes and binds the active
  journal generation, explicit sequence floor and cutoff, generation-local
  checkpoint/end evidence, registry, optional retry reference, and catalog
  slot/generation/length/checksum.
- The public `ManifestCommit` gains optional catalog identity and sufficient
  active-generation evidence for exact new-generation durable outcomes. Legacy
  commits remain exact with absent catalog identity.
- Retry persistence supports retained outcomes spanning journal generations.
  Existing Retry State V1 bytes and decoding remain supported. Retry State V2
  is introduced only where necessary to retain each embedded original V1/V2/V3
  `ManifestCommit` exactly.
- Retry validation is globally monotonic by append sequence and per generation
  by checkpoint/end offset. Older generations are covered only through exact
  catalog entries. Migration never manufactures an outcome or rewrites an old
  commit. An unchanged V1 retry snapshot may remain referenced by the empty
  successor V3 root; the first successor append emits V2.

## Rotation transaction and narrow convergence

Rotation requires no unpublished append, flushes and completes any ordinary
durable batch first, and requires the current manifest cutoff to equal the
active durable cutoff with a nonempty active range. Publication order is:

1. persist and synchronize one fixed bounded rotation intent;
2. stream-build, synchronize, read back, and publish the sealed artifact;
3. create, synchronize, and read back the empty successor at the prior global
   append-sequence floor;
4. publish and verify the next catalog snapshot, plus a retry candidate only
   when the retained evidence semantically requires one;
5. publish and directory-synchronize alternate Manifest V3 last as the only
   commit point;
6. atomically adopt the successor;
7. narrowly remove redundant predecessor duplicates and the intent.

The intent is not authority. Open implements only deterministic convergence for
this transaction. If the prior manifest remains current, exact candidates must
prove redundant derivatives before removal and the prior root stays authoritative.
If a V3 manifest is current, its catalog and successor must verify before the
predecessor duplicate and intent are removed. Missing, mismatched, or ambiguous
evidence refuses unchanged. Broad manifest fallback, repair, and recovery events
remain M02-PR03a work.

## Runtime rotation behavior

- Before append, an age or fit demand first flushes prior pending durability,
  rotates at the safe boundary, resets active-generation age, then appends.
- After append publication makes a size, count, or age demand true, the worker
  flushes that append normally and then rotates. Age applies only after at
  least one active record, preventing an empty age loop.
- The one writer, one control gate, 16-command count bound, exact byte
  reservations, group barriers, outstanding retry coalescing, and volatile
  latest semantics remain unchanged. Successful rotation returns health to
  `Healthy`.
- Inspection exposes only bounded sanitized generation inventory facts:
  active generation, sealed count, and exact covered sequence/byte evidence.

## Required evidence

- Primitive-only independent oracles prove exact Manifest V3, Generation
  Catalog V1, sealed artifact, and Retry State V2 bytes without importing
  production codecs or implementation logic.
- Hostile parsing covers magic, version, declared/header length, reserved bytes,
  store/generation/order/range/count/checksum/trailing data, exact bounds, and
  allocation preflight.
- V2-to-V3 first rotation and repeated successor append/reopen preserve every
  record exactly once, strict global sequences, and generation-local offsets.
- Replay and guard entries spanning generations return their original exact
  commits; registry revision, retirement, and historical admission stay exact;
  latest restart stays empty.
- Catalog entry 64 succeeds, entry 65 refuses without overwrite, deletion, or
  reclamation.
- Fault evidence covers intent, sealed write/sync/readback/publish/directory
  sync, successor create/sync, catalog publication, manifest publication and
  directory sync, adoption, and cleanup. A precommit interruption converges
  only to the prior root; a postcommit interruption converges only to V3; no
  returned durable receipt is lost.
- Existing publication, lock, capacity, group-barrier, child-process kill,
  hostile inventory, and no-false-commit regressions remain green. Bounded open
  evidence proves normal open does not scan sealed payload bytes.

## Tracked records

Update `AGENTS.md`, `README.md`, `docs/architecture.md`,
`docs/model-contract.md`, `docs/journal-v1-format.md`,
`docs/manifest-v1-format.md`, and `docs/retry-state-v1-format.md`; add exact
Generation Catalog V1 and sealed Journal V1 format records plus
`docs/continuation-m02-pr02c.md`. No `_roadmap/` file is tracked or changed.

## Explicit exclusions

- no `och-core`, dependency, or unsafe-code change;
- no final native segment encoding, immutable-history query, or latest rebuild;
- no retention, reclamation, broad recovery/fallback/repair, or recovery event;
- no disk-pressure or physical-free-space claim;
- no adapter, Studio, Engine, donor source, or `_roadmap/` change;
- no universal power-loss, macOS `F_FULLFSYNC`, Windows, or adversarial
  directory-writer guarantee.

## Replan triggers

Stop before broadening if prior/new-root convergence cannot be proved, the
catalog/input/history becomes unbounded, an old retry outcome cannot retain its
exact original commit, a dependency/core/query/native-segment contract is
required, or the work becomes more than this one reviewable authority transition.

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
