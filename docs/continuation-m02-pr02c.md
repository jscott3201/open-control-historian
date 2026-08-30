# M02-PR02c continuation: bounded raw-Journal rotation and seal authority

## Delivered boundary

M02-PR02c replaces terminal generation-one rotation demand with one bounded,
dependency-free `och-store` authority transition. `och-runtime` detects
size/count/age demand only at the sole-writer boundary, completes the ordinary
durable receipt batch, asks store to rotate, then continues with an empty
successor. `och-core`, canonical admission semantics, latest publication, and the
16-command/control bounds are unchanged.

One store-global append sequence is strictly increasing across generations.
Journal end offsets, active record/byte bounds, and checkpoint-slot generations
are local to one generation. Generation one preserves the legacy active names;
successors use fixed 20-digit generation-derived names. An active open binds its
exact generation and exclusive sequence floor. A frame that cannot fit an empty
active returns typed `FrameTooLarge` without creating rotation artifacts.

## Committed generation authority

Rotation accepts only an exact fully durable nonempty active range with no
unpublished append. Its fixed order is intent, streamed sealed candidate,
candidate synchronization and full readback, final sealed publication and
directory sync, synchronized empty successor, Generation Catalog V1 publication,
Manifest V3 publication and directory sync, atomic adoption, then narrow cleanup.
Manifest V3 is the commit point; the 96-byte intent never is.

The sealed artifact is an immutable raw Journal V1 file, not an M03/native
segment. Store streams it with a fixed 64 KiB buffer and verifies exact StoreId,
header version, framing, global sequence range, declaration history, durable end,
length, and complete checksum before catalog/manifest authority. Product code
does not mutate or delete a published seal and exposes no raw path or handle.

Generation Catalog V1 uses three reusable slots, fixed 64-byte header and entries,
canonical generation/range order, and a hard maximum of 64 sealed generations.
Each entry binds generation, exclusive floor, inclusive cutoff, source end,
registry generation, artifact length/checksum, and raw-Journal format. Entry 65
returns `GenerationCatalogFull` before intent or artifact mutation; no history is
overwritten or reclaimed. Bounded inspection exposes active generation, sealed
count, covered floor/cutoff, and sealed bytes without paths or content.

Manifest V3 is exactly 160 bytes. Exact 128-byte Manifest V1/V2 encodings and
decode/open remain supported. V3 retains their registry/retry fields and adds
active sequence floor plus full catalog slot/generation/length/checksum identity.
Every public `ManifestCommit` therefore carries the exact optional catalog
identity needed by new-generation outcomes; legacy commits retain zero floor and
no catalog.

## Retry and runtime behavior

Exact Retry State V1 bytes and decode remain unchanged. The empty first successor
V3 preserves its existing V1 snapshot/reference because rotation creates no retry
outcome. The first successor append emits Retry State V2 when retained outcomes
span generations. V2 appends a fixed 48-byte generation/catalog extension to
each replay entry; legacy outcomes retain zero/absent fields and every embedded
original `ManifestCommit` remains byte-semantically exact. Validation is global
by append sequence, local by generation for offset/checkpoint evidence, and uses
the current exact catalog to cover older ranges.

Before append, an age/fit demand flushes prior pending work, rotates, resets the
generation age, then appends. After append publication, reaching size/count/age
demand forces the ordinary barrier, releases that durable receipt batch, then
rotates at the safe boundary. Age never rotates an empty generation. Successful
rotation leaves health `Healthy`; catalog-full reports `RotationRequired`, stops
new ingress, retains every artifact, and requires reopen/operator progression.
Volatile latest is never changed by rotation and still restarts empty.

## Narrow convergence and compatibility

Before Manifest V3, open verifies any present sealed/successor/catalog/manifest
candidates are exact redundant derivatives of the still-current prior root,
removes them and the intent, and retains that root. After Manifest V3, open first
verifies its registry, retry, catalog, sealed metadata/header, and successor, then
removes redundant predecessor/intent/slot evidence. Missing, mismatched, partial,
or ambiguous evidence refuses unchanged. This is not broad manifest fallback,
corruption repair, or a recovery event model.

Every V3 root binds its active generation to the checked successor of the last
sealed generation, its sequence floor to that sealed cutoff, and its registry to
the entry's retained authority. Distinct catalogs held by consecutive manifests
must retain the older entries exactly and append one entry that exactly describes
the older manifest's journal generation, sequence range/end, artifact length,
and registry generation. The newer manifest preserves registry/retry references
and binds the exact empty successor with checked journal generation, prior
cutoff as floor/cutoff, 28-byte end, and checkpoint generation one. This retained
pair proof covers first and later rotation after intent cleanup. Active/checkpoint
pairs and sealed finals must equal the selected root except for narrowly verified
intent evidence. After an ordinary manifest commit, the only accepted
unreferenced catalog is a canonically decoded strict prefix of a referenced newer
catalog; open verifies and removes that crash-window duplicate idempotently.
Forked, future, unrelated, or extra recognized generation evidence refuses
unchanged.

Normal open reads bounded manifest/catalog/retry/registry artifacts plus sealed
file metadata and 28-byte headers. It does not scan every sealed payload byte.
Old binaries fail closed on V3/generation inventory. Existing V1/V2 stores open
unchanged and their first rotation is the only transition to V3.

## Evidence

Independent primitive-only oracles compare exact Manifest V3, Generation Catalog
V1, raw sealed Journal V1, and Retry State V2 bytes without importing production
codec logic. Hostile parser evidence covers version, length, reserved, scope,
generation, count, order, range, format, checksum, and trailing input. Focused
tests prove first and repeated rotation/reopen, strict global sequences with local
offset reset, exact cross-generation retry replay, historical declaration and
retirement preservation, latest-empty restart, the exact 64/full boundary,
too-large empty-active refusal, and bounded normal open.

The deterministic fault matrix covers intent, seal write/sync/readback/publish and
directory sync, successor create/sync, catalog publication, manifest publication
and directory sync, adoption, and cleanup. Every injected operation reports no
rotation success. Precommit cases leave only the prior manifest authority or
refuse ambiguous evidence unchanged; postcommit cases converge only to verified
Manifest V3. Existing lock, child-process, group barrier, kill/reopen, hostile
inventory, publication, capacity, and no-false-commit regressions remain part of
the PR gate.

Exact-head local evidence on Rust 1.98.0:

- `cargo +1.98.0 clippy -p och-store -p och-runtime --all-targets --locked -- -D warnings`
  passed;
- `cargo +1.98.0 test -p och-store --locked` passed 70 tests;
- `cargo +1.98.0 test -p och-runtime --locked` passed 54 tests;
- `cargo +1.98.0 test -p och-core --locked` passed 68 unit/integration tests
  and 3 compile-fail doctests;
- `cargo +1.98.0 test --workspace --doc --locked` passed 3 doctests;
- `git diff --check` passed; and
- `./scripts/gate.sh pr` passed 200 nextest tests with 0 skipped, 3 doctests,
  strict workspace clippy, rustdoc, repository checks for 97 files, dependency
  policy for 3 native roots and a 5-package closure, and license/source/ban
  checks.

The release gate was not requested and was not run.

## Platform and deferred ledger

The platform contract remains safe standard-library file I/O with same-directory
exclusive staging creation, file synchronization, rename, and directory
synchronization. It does not claim universal power-loss durability, macOS
`F_FULLFSYNC`, Windows qualification, physical free-space, or safety against an
adversarial directory writer.

Still absent and separately reviewed: final native segment encoding, sealed
history/query APIs, latest reconstruction, retention/reclamation, disk-pressure
policy, broad fallback/repair and recovery events, adapters, Studio/Engine links,
and an unbounded or time-based retry horizon. No dependency, unsafe code,
`och-core`, or `_roadmap/` change belongs to this slice.
