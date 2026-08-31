# M03-PR03c Native Segment V1 bounded-resource plan

## Status and evidence vocabulary

This document is the separately reviewed **tooling prototype and measurement
plan** prerequisite for a possible Store Format V2 implementation. It changes no
native crate, current Store Format V1 behavior, durable format, authority, or
publication path. Current product code remains Store Format V1-only.

The statements below use four labels deliberately:

- **FACT** describes an existing format, bound, or checked implementation fact.
- **FORMULA** is arithmetic derived from those facts.
- **PROTOTYPE MEASUREMENT** is a result from the private standalone tool and its
  named platform/profile; it is not a native-store result.
- **PRODUCT INFERENCE** is forbidden unless a later native implementation proves
  it independently. Nothing in this document is such an inference.

The prototype target is an absolute process peak RSS below **160 MiB**, exactly
`167,772,160` bytes, with **zero external sort workspace**. Linux x86_64 maximum-
bound standalone measurements are accepted by
[M03-PR03d](m03-pr03d-linux-resource-evidence.md) as tooling evidence only. Darwin
results remain exploratory. Writer and eager-open SLOs remain `UNKNOWN`, and a
separate native evidence plan, measured native results, and fresh owner checkpoint
remain mandatory before V2 product implementation.

## Exact unchanged artifact arithmetic

**FACT:** Native Segment V1 remains the exact existing `OCHSEG01` grammar in
[Native Segment V1](native-segment-v1-format.md). For raw source length `r`, frame
count `f`, series count `s`, and observation count `o`:

```text
SegmentLen = 192 + 64*s + (r - 28) + 48*f + 96*o + 4
```

**FORMULA:** At all independent current maxima this is `637,993,128` bytes:

```text
192 + 64*4,096 + (536,870,912 - 28)
    + 48*4,096 + 96*1,048,576 + 4
```

The fixture generator does not claim that all maxima occur in one constructible
canonical source. `max-bytes` instead constructs exactly 4,096 valid frames whose
complete source is exactly `536,870,912` bytes; its actual observation count and
segment length are reported when generated. `max-observations` separately proves
4,096 frames with 256 observations each and refuses if its actual source exceeds
the current 512 MiB source bound.

## Checked representation ledger

**FACT:** The measured path allocates no complete raw-source or segment-output
`Vec`. Its file-only interfaces and counted-read wrapper enforce a maximum single
read request of one current maximum frame. Compile-time size assertions fix these
prototype records:

- two `FrameMeta` arrays, each `4,096 * 64` bytes at maximum: append order and
  `(SeriesId, append sequence)` order;
- one global `ObservationWork` array, exactly `1,048,576 * 96` bytes at maximum;
- no separate series table: series directory rows and contiguous recent slices
  are derived by runs over those two globally sorted arrays;
- at most two frame-sized byte buffers, each at most `8,388,632` bytes, for one
  input frame and its canonical re-encoding; and
- one fixed `128 KiB` comparison/I/O scratch region.

All input-derived counts, lengths, products, sums, conversions, seeks, and slice
bounds are checked before allocation or access. Metadata vectors request the exact
validated sidecar count, then the tool reads and accounts each actual `Vec`
capacity and refuses any allocator capacity above the corresponding hard bound.
Frame, re-encode, and scratch capacities receive the same check. Successful build
and validation reports read the controlled-state counter only after dropping pair
state and require the actual value to be zero. Private fixture metadata, segment
identity, and fixture-set text share one reader that allocates independently of
file metadata, reads at most the respective `limit + 1` bytes, and rejects excess
before semantic work.

**FORMULA:** The conservative controlled base is exactly:

```text
2*8,388,632 + 1,048,576*96 + 2*4,096*64 + 128KiB
= 118,095,920 bytes (112.625 MiB)
```

This leaves exactly `49,676,240` bytes (47.375 MiB) beneath the 160 MiB target for
decoded-frame structures, decoder/re-encoder transient allocation, allocator and
thread runtime, executable/code pages, and other measured process overhead. There
is no hidden series-table delta. If later implementation adds one, it must debit
that representation honestly and repeat Linux acceptance.

## Prototype algorithm and I/O formulas

### Streaming source preflight

The first sequential raw pass checks the exact Journal Header V1, file length and
CRC-32C, frame prefix and CRC, strict global append range, StoreId and one-series
scope, canonical semantic decode/re-encode, source counts, and source checksum.
Only one decoded frame plus bounded metadata is live. Observation evidence goes
directly into the sole global 96-byte work array; there are no per-series
observation vectors.

The append metadata is copied once and sorted by exact SeriesId bytes then append
sequence. That total order derives series blocks, frame ordinals, global append
locations, and series count. The global observation array is sorted by the exact
total key:

```text
SeriesId ascending,
RawObservationOrderKey descending,
append sequence descending,
observation ordinal ascending
```

### Virtual identity and streamed output

A first canonical virtual emission computes checked layout, complete length, and
trailer CRC without an output allocation or external file. A second emission
writes the exact header, derived series rows, reread complete frames in canonical
series order, append rows, observation rows, and trailer to one evidence-only
temporary final. Its length and CRC must equal the virtual identity before rename.
No sort run, side index, database, memory map, or external workspace exists.

**FORMULA:** one build reads exactly `3*r - 56` raw bytes: `r` for semantic
preflight and `r - 28` for each of virtual and physical block emission. It writes
exactly `SegmentLen` segment bytes plus one bounded textual identity sidecar. The
evidence final is removed before staging, so an old final and new full temporary
do not coexist.

### Stream-hostile eager validation

Validation repeats raw semantic preflight, then reads the complete segment once in
section order. It independently reconstructs and exact-compares the header,
series rows, frame blocks, append directory, recent directory, complete trailer,
and evidence identity. Every segment frame is decoded and byte-identically
re-encoded; fixed scratch compares it with the exact raw frame. Generation, range,
registry generation, StoreId, raw length/checksum, frame order/coverage, offsets,
and counts must all match.

**FORMULA:** one validation reads `2*r - 28` raw bytes and exactly `SegmentLen`
segment bytes, and writes no artifact. Sequential fixture-set validation drops the
complete pair state before opening the next pair; only the bounded set-name text
persists.

## Storage and 64-pair ceilings

For one retained evidence pair, logical payload storage is `r + SegmentLen` plus
at most 2,560 bytes of bounded private metadata. External sort workspace remains
zero. At independent format maxima the raw plus segment payload is exactly
`1,174,864,040` bytes.

For 64 maximum-sized pairs, exact pair-only payload storage is
`75,191,298,560` bytes (about 70.03 GiB). Retaining one additional maximal active
journal during construction makes the exact logical total `75,728,169,472` bytes
(about 70.53 GiB), before filesystem allocation granularity and report/headroom.
The protocol uses that complete value as its environment-planning floor rather
than attempting this case by default. One sequential eager-validation sweep would
read exactly:

```text
64 * ((2*536,870,912 - 28) + 637,993,128)
= 109,551,035,136 bytes (about 102.03 GiB)
```

The authoritative value is the formula and generated report; the rounded planning
figures are not acceptance results. `open-64` uses 64 representative pairs, not
64 maximum pairs.

## Deterministic fixture matrix

All files live beneath caller-selected evidence roots (normally
`target/v2-evidence`) and use private evidence-only names.

| Case | Exact dimensions | Purpose |
| --- | --- | --- |
| `min` | 1 frame, 1 series, 0 observations | minimum nonempty source |
| `min-observed` | 1 / 1 / 1 | minimum recent index |
| `representative` | 256 / 32 / 16,384 (64 per frame) | mixed block and global recent order |
| `max-records` | 4,096 / 1 / 0 | frame-count bound |
| `max-series` | 4,096 / 4,096 / 0 | series-count bound |
| `max-observations` | 4,096 / 32 / 1,048,576 | `368,336,924`-byte source; observation-work bound |
| `max-bytes` | 4,096 / 32 / 65,536 | exact `536,870,912`-byte source and `543,361,192`-byte segment |
| `open-64` | 64 distinct representative pairs | sequential state-drop proof |

With seed 1, the currently exercised representative source is `5,916,188` bytes
and its exact segment is `7,503,556` bytes. These are deterministic fixture facts,
not latency or RSS acceptance.

Exact-byte tests independently cover the one-frame minimum oracle and two real
current-V1 product rotations. The observation-bearing product comparison is a
deliberately smaller 4-frame, 2-series, 8-observation fixture—not the 256-frame
measurement fixture—and exercises non-append series blocks, append-directory
order, and per-series recent-observation ordering before byte-comparing with
`ManifestStore::build_segment_candidate_v1`.

## CLI, authority fence, and cleanup

```console
cargo +1.98.0 run --release --locked -p och-v2-evidence -- \
  prepare-root --root target/v2-evidence
cargo +1.98.0 run --release --locked -p och-v2-evidence -- \
  generate --root target/v2-evidence --case representative --seed 1
cargo +1.98.0 run --release --locked -p och-v2-evidence -- \
  stream-build --root target/v2-evidence --case representative
cargo +1.98.0 run --release --locked -p och-v2-evidence -- \
  stream-validate --root target/v2-evidence --case representative
cargo +1.98.0 run --release --locked -p och-v2-evidence -- report-ledger
```

Evidence roots are rejected when they or any existing ancestor contain a current
V1 or proposed V2 recognized store name. The tool never starts a runtime, opens a
store authority, emits a format marker/manifest/catalog/intent, names a product
segment final, deletes raw evidence, falls back, repairs, migrates, or grants
registry/query/receipt authority. Failure removes the private partial output by
default; `--keep-on-failure` retains at most that one bounded evidence partial.
Errors contain no path or canonical content.

The measurement script builds the tool, invokes the exact `prepare-root` CLI
boundary, and only then creates `reports/` or performs another evidence-root
write. A deterministic test applies that CLI boundary to a real current-V1 store,
exact-compares its direct names and bytes before/after refusal, and reopens the
store; a separate case proves a missing safe root can be created without creating
children. The shell ordering itself is checked as source because recursively
invoking Cargo from the package test is deliberately avoided.

## Measurement and acceptance protocol

The portable script builds the tool with Rust 1.98.0, release profile, and locked
dependencies; generates fixtures outside timing; and starts a fresh child for
every build and validation sample:

```console
./scripts/measure-v2-evidence.sh \
  --cases min,representative,max-records,max-series,max-observations \
  --repetitions 3
```

It writes sanitized machine, raw-sample, tool-ledger, and min/median/p95/max
reports under `target/v2-evidence/reports`. Darwin uses `/usr/bin/time -l` and its
byte-valued `maximum resident set size`; Linux uses `/usr/bin/time -v` and
converts maximum RSS KiB to bytes. Optional cgroup evidence may supplement but
never replace GNU-time evidence. Process state is cold per child; warm-process is
not measured; filesystem-cold is `UNKNOWN` unless an operator genuinely controls
it. Logical and allocated fixture/final bytes and zero external workspace are
reported separately. The summary grants the candidate label only when both the OS
is Linux and `uname -m` is exactly `x86_64`; every other architecture is
exploratory. Revision cleanliness inspects tracked changes only.

Darwin samples remain **PROTOTYPE MEASUREMENT / EXPLORATORY_ONLY** even below the
target. M03-PR03d records the completed Linux x86_64 maximum-bound standalone run
and owner acceptance. Numeric writer/open SLOs stay `UNKNOWN`; the tool cannot set
them. A peak is below target only when it is strictly less than `167,772,160`;
equality is not below target.

## Product non-inference and remaining hard stop

Standalone success does not prove native sole-writer scheduling, exact pre-intent
integration, Manifest-last transaction order, crash convergence, storage-pressure
custody, receipts, cleanup fault injection, open inventory bounds, or final
production latency. It does not authorize V2 names or bytes in `och-store`.

A future product PR remains blocked on all of the following:

1. a separately reviewed native timing/transaction/fault/cleanup/pressure/receipt
   evidence plan;
2. measured native writer-delay and eager-open results presented without invented
   SLOs;
3. a fresh owner checkpoint accepting those results and the integration plan; and
4. the complete native transaction, crash, cleanup, pressure, and receipt evidence
   matrix retained by [M03-PR03b](implementation-brief-m03-pr03b.md).
