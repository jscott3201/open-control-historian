# M03-PR03c Native Segment V1 resource-evidence implementation brief

## Objective

Add one private tooling package and reproducible protocol that independently
prototype exact existing `OCHSEG01` construction and eager raw/segment validation
under a 160 MiB absolute peak-RSS target with zero external sort workspace. This
is the separately reviewed resource-plan successor to M03-PR03b, not Store Format
V2 product implementation.

Current native product behavior and every durable format remain unchanged. No
file under `crates/` changes. Store Format V1 remains the only implemented epoch.

## Delivered boundary

- `och-v2-evidence` is private role-`tooling`, outside `default-members` and every
  native root. It depends inward only on `och-core` and `och-store` and adds no
  third-party package.
- Fixture generation may use current canonical and Journal V1 public contracts.
  Measured stream build and validation do not call full-buffer
  `build_segment_v1` or `parse_segment_v1`.
- The prototype uses two 64-byte frame metadata arrays, one 96-byte global
  observation array, at most two maximum-frame buffers, 128 KiB scratch, and no
  series table or external sort file.
- Virtual emission fixes complete segment length and trailer CRC before physical
  output. Physical output must reproduce that identity exactly.
- Eager validation independently checks every source, segment, index, linkage,
  and trailer byte while retaining one pair at a time.
- The complete algorithm, formulas, CLI, storage ceilings, and acceptance stop
  are in the [resource plan](m03-pr03c-segment-resource-plan.md).

## Exactness and hostile evidence

Focused tests compare the streamed minimum byte-for-byte with a primitive oracle
that does not call a segment encoder. Two separate tests create real current-V1
stores, rotate committed sources, obtain current public
`ManifestStore::build_segment_candidate_v1` evidence, copy only the raw source
outside each store, and exact-compare the tooling output with product candidate
bytes. One case is the 1-frame/1-series/0-observation minimum. The second is a
bounded 4-frame/2-series/8-observation product fixture that exercises canonical
series blocks, append order, and recent order; it is intentionally smaller than
the 256-frame measurement fixture. No native constructor or API is added.

Hostile tests cover truncation, trailing bytes, checksum, version/flags/reserved,
count/layout, order/coverage, repaired-trailer foreign StoreId,
generation/range/registry, frame corruption,
source linkage, append/recent redistribution, repeated refusal, 64-pair sequential
state drop, bounded reads, controlled-ledger return, and partial-file cleanup.
Errors remain path- and content-free.

The `prepare-root` command runs the same evidence-root fence without writing a
report. The measurement script calls it before creating `reports/`. Tests prove
that boundary refuses a real V1 store without changing its direct inventory or
bytes and that the store reopens, while a missing safe root remains creatable.
Metadata, identity, and set text use one `limit + 1` bounded reader and have
oversized-input regressions. RSS equality with the absolute target reports
`rss_below_target=false`.

## Measurement status and hard stop

Darwin release measurements may be recorded as exploratory evidence in the
[continuation](continuation-m03-pr03c.md). They cannot satisfy Linux acceptance.
The future V2 implementation remains blocked until Linux x86_64 maximum-bound
memory and latency evidence is reviewed at a fresh owner checkpoint. Writer and
open latency ceilings are `UNKNOWN`.

The standalone tool proves no native writer transaction, crash convergence,
pressure custody, receipt behavior, durable segment authority, query integration,
fallback, deletion, repair, or migration.

## Acceptance commands

```console
cargo +1.98.0 fmt --all -- --check
cargo +1.98.0 clippy -p och-v2-evidence --all-targets --locked -- -D warnings
cargo +1.98.0 nextest run --locked -p och-v2-evidence
cargo +1.98.0 test -p och-v2-evidence --doc --locked
cargo +1.98.0 run --locked -p och-policy -- check --manifest-path Cargo.toml
git diff --check
./scripts/gate.sh pr
./scripts/measure-v2-evidence.sh \
  --cases min,representative,max-records,max-series,max-observations \
  --repetitions 3
```

The release gate is not requested. Ordinary hosted Linux PR execution proves only
functional gates, not resource acceptance.
