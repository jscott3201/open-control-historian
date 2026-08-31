# M03-PR03c Native Segment V1 resource-evidence continuation

## Delivered result

M03-PR03c adds a private tooling-only streaming/multipass prototype for exact
unchanged Native Segment V1 bytes and stream-hostile raw/segment validation. It
adds no product call path, V2 name or byte, native dependency, durable authority,
or behavior under `crates/`.

The checked representation has no separate series table. Its conservative base
is exactly `118,095,920` bytes, leaving `49,676,240` bytes beneath the absolute
`167,772,160`-byte target for decoded/runtime/allocator/code overhead. External
sort workspace is exactly zero. Build reads `3*r - 56`, validation reads
`2*r - 28 + SegmentLen`, and 64-pair validation drops complete pair state after
every pair.

## Deterministic byte evidence

- Minimum fixture: 1 frame, 1 series, 0 observations.
- Minimum-observed fixture: 1 / 1 / 1.
- Representative seed-1 fixture: 256 frames, 32 series, 16,384 observations,
  `5,916,188` raw bytes, and `7,503,556` segment bytes.
- Maximum records: 4,096 frames.
- Maximum series: 4,096 frames across 4,096 series.
- Maximum observations: 4,096 * 256 = 1,048,576 observations,
  `368,336,924` raw bytes, and `469,199,044` segment bytes.
- Maximum bytes: an exactly constructible `536,870,912`-byte valid source with
  4,096 frames and 65,536 observations; its segment is `543,361,192` bytes and
  there is no composite-maxima claim.
- `open-64`: 64 representative pairs for sequential-state proof.

The minimum matches an independent primitive oracle. Existing current-V1 public
store evidence also matches byte-for-byte after real committed rotations through
`ManifestStore::build_segment_candidate_v1`: one 1-frame/1-series/no-observation
case and one 4-frame/2-series/8-observation case covering canonical series blocks,
append order, and recent ordering. The latter is intentionally smaller than the
256-frame measurement fixture. These are tooling tests, not a new native API.

## Exploratory measurements

The following **PROTOTYPE MEASUREMENT / EXPLORATORY_ONLY** record came from
Darwin 25.6.0 arm64, Apple M5, 10 logical CPUs, 16 GiB physical memory, APFS,
16 KiB pages, Rust/Cargo 1.98.0, release profile, and dirty-tracked base revision
`66c6ef052f28c15194f68d9f7708aa98ed865364`. `/usr/bin/time -l` reported maximum
RSS in its documented byte unit. Each of three samples used a fresh process;
warm-process was not measured and filesystem-cold remained `UNKNOWN`/uncontrolled.
Raw sanitized reports remain uncommitted under `target/`.

| Case/operation | Elapsed seconds min / median / p95 / max | Peak RSS bytes min / median / p95 / max |
| --- | --- | --- |
| `min` build | 0.00 / 0.00 / 0.00 / 0.00 | 1,884,160 / 1,884,160 / 1,884,160 / 1,884,160 |
| `min` validate | 0.00 / 0.00 / 0.00 / 0.00 | 1,900,544 / 1,900,544 / 1,933,312 / 1,933,312 |
| `representative` build | 0.16 / 0.17 / 0.17 / 0.17 | 3,964,928 / 3,981,312 / 4,014,080 / 4,014,080 |
| `representative` validate | 0.16 / 0.16 / 0.17 / 0.17 | 4,046,848 / 4,063,232 / 4,063,232 / 4,063,232 |
| `max-records` build | 0.09 / 0.10 / 0.10 / 0.10 | 2,637,824 / 2,654,208 / 2,670,592 / 2,670,592 |
| `max-records` validate | 0.10 / 0.10 / 0.10 / 0.10 | 2,654,208 / 2,670,592 / 2,670,592 / 2,670,592 |
| `max-series` build | 0.11 / 0.11 / 0.11 / 0.11 | 2,621,440 / 2,654,208 / 2,654,208 / 2,654,208 |
| `max-series` validate | 0.09 / 0.10 / 0.10 / 0.10 | 2,637,824 / 2,686,976 / 2,686,976 / 2,686,976 |
| `max-observations` build | 12.09 / 15.43 / 35.01 / 35.01 | 103,530,496 / 103,727,104 / 103,972,864 / 103,972,864 |
| `max-observations` validate | 10.22 / 11.15 / 11.70 / 11.70 | 103,972,864 / 104,431,616 / 104,448,000 / 104,448,000 |
| `max-bytes` build | 10.81 / 11.05 / 11.14 / 11.14 | 9,715,712 / 9,748,480 / 9,748,480 / 9,748,480 |
| `max-bytes` validate | 13.14 / 13.55 / 15.41 / 15.41 | 9,601,024 / 9,781,248 / 9,994,240 / 9,994,240 |

All measured maxima were below `167,772,160` bytes; the largest observed value
was `104,448,000` bytes for max-observation validation. Every tool report recorded
zero logical and allocated external-sort workspace, controlled state returned to
zero, and fixture/final logical and allocated high-water bytes separately. These
Darwin numbers do not set writer/open SLOs or satisfy Linux acceptance.

A separate release functional run generated 64 distinct representative pairs and
`validate-set --set open-64` validated all 64 sequentially with
`controlled_bytes_after=0` and no external workspace. It was not an RSS sample.

Exact completed commands were:

```console
./scripts/measure-v2-evidence.sh \
  --cases min,representative,max-records,max-series,max-observations \
  --repetitions 3
OCH_V2_EVIDENCE_ROOT=target/v2-evidence-max-bytes \
  ./scripts/measure-v2-evidence.sh --cases max-bytes --repetitions 3
```

## Remaining hard stop

Linux x86_64 release evidence must still exercise maximum legal bounds, report
all elapsed and maximum-RSS samples, stay below 160 MiB, and return to the owner
with measured latency. Numeric writer-delay and eager-open latency ceilings remain
`UNKNOWN`. A fresh owner checkpoint must accept both memory and latency before any
Store Format V2 implementation starts.

The tool does not prove native sole-writer pre-intent integration, Manifest-last
publication, crash/cleanup convergence, pressure custody, receipts, or final
production latency. M03-PR03b's complete future native fault and authority matrix
remains mandatory.

## Authority and exclusions

Current Store Format V1 remains sole implemented authority. There is no V2
decoder/opener/publication, native streaming module, runtime scheduling, durable
segment authority, migration, compatibility, raw fallback/deletion, query
integration, full transaction simulator, memory map, external database/cloud,
compression framework, or benchmark dependency in this successor.
