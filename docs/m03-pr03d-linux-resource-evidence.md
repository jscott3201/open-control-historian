# M03-PR03d accepted Linux x86_64 standalone resource evidence

## Decision and authority boundary

On 2026-08-31 the owner accepted this record as **standalone tooling resource
evidence only** for the unchanged Native Segment V1. It satisfies only the
M03-PR03c Linux x86_64 standalone resource-measurement condition. Store Format
V1 remains the only implemented format.

This record adds no code under `crates/`, product API, durable byte or name,
writer/rotation/open integration, or Store Format V2 authority. Standalone
elapsed values are not writer delay, rotation latency, eager-open latency, or
SLOs. Writer-delay and eager-open SLOs remain `UNKNOWN`. V2 product work remains
blocked on a separately reviewed native timing/transaction/fault/cleanup/
pressure/receipt evidence plan, measured native results, and a fresh owner
checkpoint.

## Measured source and functional gate

The run used a detached, clean tracked checkout of:

- source SHA: `4e679a313477505c1dd90d23d08ef666b92e47c7`;
- `Cargo.lock` SHA-256:
  `a869a064cfbfda4a76f080e6303cfc59ed70316618ea8619fcad48b69b191219`;
- `scripts/measure-v2-evidence.sh` SHA-256:
  `59c222763ede0fd670dd6a6cb69b13c0ca6e7f39365c0150f07c723ddb33a01d`.

Before acceptance, `./scripts/gate.sh pr` passed on that Linux checkout: 279/279
nextest tests, 3/3 doctests, repository policy/docs checks, and `cargo deny`.
This is not a release-gate claim.

## Platform and measurement mode

| Property | Recorded value |
| --- | --- |
| OS / kernel / architecture | Linux / `6.17.0-1022-gcp` / `x86_64` |
| CPU | 4 CPUs, Intel(R) Xeon(R) CPU @ 2.80GHz |
| Physical memory | `16,764,178,432` bytes |
| Report filesystem / page size | `ext2/ext3` / `4,096` bytes |
| Rust / Cargo | 1.98.0 / 1.98.0 |
| Profile | release |
| RSS source | GNU `/usr/bin/time -v`, KiB converted to bytes |
| Process cache mode | cold process per sample |
| Warm process | not measured |
| Filesystem-cold mode | `UNKNOWN-uncontrolled` |
| Strict RSS target | `< 167,772,160` bytes |

The two committed machine reports preserve the complete sanitized platform
record: [core](evidence/m03-pr03d-linux-x86_64/core-machine.kv) and
[max-bytes](evidence/m03-pr03d-linux-x86_64/max-bytes-machine.kv).

## Completed command matrix

The timed runs used the checked-in measurement script, which builds once, creates
fixtures outside timing, and starts one fresh release child for each operation
sample:

```console
./scripts/measure-v2-evidence.sh \
  --cases min,representative,max-records,max-series,max-observations \
  --repetitions 3
OCH_V2_EVIDENCE_ROOT=target/v2-evidence-max-bytes \
  ./scripts/measure-v2-evidence.sh --cases max-bytes --repetitions 3
```

The functional 64-pair generation and sequential validation used the same release
tool commands outside the timed matrix:

```console
cargo +1.98.0 run --release --locked -p och-v2-evidence -- \
  generate --root target/v2-evidence-open-64 --case open-64 --seed 1
cargo +1.98.0 run --release --locked -p och-v2-evidence -- \
  validate-set --root target/v2-evidence-open-64 --set open-64
```

The full timed matrix therefore contains six named cases, two operations per
case, and three fresh samples per operation: 36 timed children. The exact child
rows are in [core samples](evidence/m03-pr03d-linux-x86_64/core-samples.tsv) and
[max-bytes samples](evidence/m03-pr03d-linux-x86_64/max-bytes-samples.tsv).

## Complete observed statistics

Elapsed columns and RSS columns are each `min / median / observed-p95 / max`.
With only three samples, reported p95 is the highest observed sample; it is not a
population percentile.

| Case / operation | Elapsed seconds | Peak RSS bytes |
| --- | --- | --- |
| `min` build | 0.00 / 0.00 / 0.00 / 0.00 | 2,289,664 / 2,355,200 / 2,416,640 / 2,416,640 |
| `min` validate | 0.00 / 0.00 / 0.00 / 0.00 | 2,404,352 / 2,482,176 / 2,482,176 / 2,482,176 |
| `representative` build | 0.21 / 0.21 / 0.22 / 0.22 | 3,948,544 / 3,952,640 / 4,009,984 / 4,009,984 |
| `representative` validate | 0.23 / 0.23 / 0.23 / 0.23 | 3,956,736 / 3,993,600 / 4,038,656 / 4,038,656 |
| `max-records` build | 0.12 / 0.12 / 0.12 / 0.12 | 2,818,048 / 2,916,352 / 2,932,736 / 2,932,736 |
| `max-records` validate | 0.13 / 0.13 / 0.13 / 0.13 | 2,756,608 / 2,777,088 / 2,940,928 / 2,940,928 |
| `max-series` build | 0.12 / 0.12 / 0.12 / 0.12 | 2,813,952 / 2,813,952 / 2,850,816 / 2,850,816 |
| `max-series` validate | 0.13 / 0.14 / 0.14 / 0.14 | 2,777,088 / 2,859,008 / 2,940,928 / 2,940,928 |
| `max-observations` build | 15.69 / 15.69 / 15.70 / 15.70 | 103,784,448 / 103,825,408 / 103,825,408 / 103,825,408 |
| `max-observations` validate | 16.38 / 16.41 / 16.43 / 16.43 | 104,046,592 / 104,050,688 / 104,087,552 / 104,087,552 |
| `max-bytes` build | 18.17 / 18.17 / 18.18 / 18.18 | 9,363,456 / 9,375,744 / 9,379,840 / 9,379,840 |
| `max-bytes` validate | 19.20 / 19.24 / 19.24 / 19.24 | 9,887,744 / 9,891,840 / 10,067,968 / 10,067,968 |

The source summaries retain full precision and sample counts:
[core summary](evidence/m03-pr03d-linux-x86_64/core-summary.kv) and
[max-bytes summary](evidence/m03-pr03d-linux-x86_64/max-bytes-summary.kv). Both
classify the run as `LINUX_X86_64_CANDIDATE_ONLY`, and every observed RSS value
is strictly below the target. The largest peak was `104,087,552` bytes during
max-observations validation. The largest elapsed maximum was `19.24s` during
max-bytes validation.

## State, workspace, and open-64 checks

All 36 per-operation tool reports were independently checked before the bounded
copy: every report had `controlled_bytes_after=0` and
`external_sort_workspace_bytes=0`. Every committed sample row independently has
zero logical and allocated external-workspace columns. Raw `.time.txt`,
per-operation `.tool.kv`, fixtures, journals, segments, and binaries are not
committed.

The derived [orchestrator verification record](evidence/m03-pr03d-linux-x86_64/orchestrator-verification.kv)
is explicitly labeled as verification rather than tool output. The committed
[open-64 generation](evidence/m03-pr03d-linux-x86_64/open64-generation.kv) and
[validation](evidence/m03-pr03d-linux-x86_64/open64-validation.kv) report 64
pairs, sequential pair-state release, controlled state zero, and external
workspace zero.

## Evidence integrity

The evidence directory contains only eight exact sanitized tool outputs, the
derived orchestrator verification record, and the relative-path checksum
manifest. Verify it portably from the manifest directory:

```console
(cd docs/evidence/m03-pr03d-linux-x86_64 && shasum -a 256 -c SHA256SUMS)
```

The exact source-output hashes and the derived record hash are in
[`SHA256SUMS`](evidence/m03-pr03d-linux-x86_64/SHA256SUMS). No raw or unbounded
report set is part of this record.
