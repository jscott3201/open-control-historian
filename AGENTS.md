# Repository instructions for coding agents

These instructions apply to the entire repository unless the user gives a specific override.
They are not a substitute for the applicable brief, which may contain additional instructions, constraints, and requirements.
The brief is the authoritative source for the product's intended behavior, and the repository instructions are a secondary source for the product's implementation. The brief may also contain additional instructions for the repository, which take precedence over these instructions.

## Current authority boundary

This workspace proves the M00 foundation, dependency-free canonical Historian
model, independent M00-PR03 evidence, the M01-PR01 caller-executor lifecycle,
M01-PR02 bounded volatile ingress, and M01-PR03 volatile latest-observation
publication. The reviewed M00-PR02/M00-PR03 `och-core` contracts remain unchanged;
M00-PR04 is their explicit dependency-free successor for store identity, bounded
series declaration revisions, terminal retirement, historical resolution, and
registry-issued envelope binding. M00-PR05 is the reviewed dependency-free
successor for source projection, capture/batch provenance, and the final bounded
declaration-authorized canonical admission record. Further `och-core` changes
require another explicitly reviewed successor. `och-core` owns identity, exact
value/content, time, quality/status, producer ordering, declaration lifecycle,
source/capture provenance, collection, gap/no-change, envelope, canonical
admission, and retry-comparison semantics. M02-PR01a established the reviewed
store-scoped runtime command boundary, and M02-PR01b0 established the reviewed
Journal V1 semantic frame format. M02-PR01b1 established their active-journal
durable successor, M02-PR02a makes a bounded manifest the committed description
of its active range, mechanical cutoff, and complete canonical registry history,
M02-PR02b adds its bounded durable retry horizon, M02-PR02c adds bounded
active-journal rotation, immutable raw-Journal sealing, and Generation Catalog V1,
the durable-format reset makes each artifact family current-only V1 behind a
fixed Store Format V1 epoch marker, and M02-PR03a adds one manifest-rooted
current-V1 conservative recovery transaction. M02-PR03b1 adds deterministic
store logical preflight, typed observed storage pressure, and sticky volatile
reopen custody to direct active journals and composed manifest stores. M02-PR03b2
projects that already-typed pressure into sticky bounded runtime evidence,
fail-stop health, and reaper-joined pressure shutdown. M03-PR01a adds one
dependency-free, bounded, offline Native Segment V1 candidate with exact
complete-frame fidelity and non-authorizing indexes. M03-PR02a adds one bounded
current-only observation query over an already parsed in-memory candidate.
M03-PR02b composes those proofs into one synchronous read-only query of exactly
one catalog-committed sealed raw-Journal generation. M03-PR03a adds only a
private crate-test transient typed-value block codec proof: exact raw coverage for
all current value families plus bounded Boolean bit-pack/RLE selection. It is not
a product API, durable format, Native Segment V1 change, or compatibility promise.
M03-PR03b adds only a checked-in Store Format V2 design/authority review barrier
for future mandatory publication of the unchanged raw-frame Native Segment V1 on
every nonempty rotation. It adds no implementation or accepted durable byte;
current product authority remains Store Format V1-only.
M03-PR03c adds only a private tooling streaming/multipass prototype, checked
resource ledger, deterministic fixtures, and exploratory measurement protocol for
the unchanged Native Segment V1. Its 160 MiB target and zero external-sort plan do
not authorize V2 product code or a native workspace threshold. M03-PR03d records
owner-accepted standalone tooling
Linux x86_64 resource evidence for that unchanged prototype and satisfies only
the PR03c standalone measurement condition. M03-PR03e adds only the executable
native timing/transaction/fault/cleanup/pressure/receipt plan and maps every
PR03b evidence row to a later private harness/report obligation. It adds no
harness or result. M03-PR03f adds only the owner-authorized, disabled-by-default,
rustdoc-hidden current-V1 native instrumentation/fault/crash prerequisite for
that later harness. It adds no harness, V2 source binding, report, measurement,
or V2 behavior; explicit all-feature builds compile the seam while defaults stay
off. M03-PR03g1 adds only the private source-closed disposable V2 executor
foundation: a 173-descriptor/source-site bijection, real compact P0-P7/rollback/
eager-open wrapper execution, safe-Rust SHA-256 and primitive oracles, and narrow
current-V1 success/pressure seam smoke. It adds no complete matrix, timing/fault
report, collector, child-crash campaign, measurement, accepted V2 byte, or
collection authorization. M03-PR03g2 must separately complete those harness
obligations; every `PR03E-M01..M11` row remains `UNSATISFIED`. Writer-delay,
eager-open, RSS, total-runtime, and native external-workspace limits/budgets/SLOs
remain `UNKNOWN`; g2, measured Linux native results, and a fresh
owner checkpoint still block implementation.
`och-runtime` depends
inward on `och-store`, opens one explicitly
bounded filesystem-backed store, admits only complete M00-PR05
`CanonicalAdmission` evidence, reserves exact encoded bytes before allocation,
and sends FIFO work to one dedicated blocking writer thread. Handled and durable
receipt stages are distinct; the latter is released only after journal sync,
checkpoint sync, Retry State V1 publication, and Manifest V1 publication
cover the append. Public register,
revise, retire, and active bind requests first cross a fixed nonblocking
control-admission bound, then share the sole writer ordering authority with
append publication. A fixed reaper owns the eventual writer join after
nonblocking Drop.

`och-store` owns the Store Format V1 marker, Journal V1 bytes and header, stable
never-renamed store lock, generation-scoped retained journal locks, deterministic
active-artifact create/open, bounded scan/append/rotation, double-slot mechanical
checkpoints, two-slot 160-byte Manifest V1 authority, three-slot complete registry
snapshots, three-slot durable retry snapshots, three-slot Generation Catalog V1,
three-slot Recovery State V1 event evidence, at most 64 immutable raw-Journal
sealed generations, strict current genesis, narrow rotation and terminal-suffix
recovery convergence, and publication. It
classifies only standard-library `StorageFull` and `QuotaExceeded` kinds at
store-owned mutation boundaries as pressure; the first such failure makes that
live handle require validated reopen while inspection remains available.
restores registry snapshots only by public `SeriesRegistry` replay and
requires every decoded journal declaration to match retained historical
authority; decoded records never authorize registry state. Markerless, historical,
or mixed durable formats fail path-free before lock creation or durable mutation;
there is no migration or compatibility decoder. `och-runtime` retains
the fixed 16-command count window, exact bounded
byte reservations through durability, outstanding retry coalescing, a bounded
FIFO durable replay tier followed by an expired/conflict guard tier, a
separately fixed 16-series volatile latest registry, store-scoped immutable
snapshots, group barriers, graceful drain/final barrier/seal/join, and
nonblocking fail-stop Drop. The blocking writer owns the one non-cloneable live
`SeriesRegistry`; runtime code gains no declaration/source interpretation
semantics and exposes no mutable registry handle. Latest state restarts empty;
completed retry outcomes restore only within the configured two-tier horizon.
The sole writer automatically rotates a nonempty generation at safe
size/count/age boundaries only after ordinary durability completes; one
store-global append sequence continues while offsets/checkpoint generations reset
per generation. Recovery removes and synchronizes only a proven terminal
invalid/torn suffix beyond the selected manifest cutoff after all authority is
validated; valid post-root frames and ambiguity refuse unchanged. The latest
committed report is durable event history and never registry, retry, latest,
receipt, or declaration authority. Runtime pressure is fail-stop: first evidence
wins, unresolved receipt stages stop, future latest capture becomes unavailable,
and consuming shutdown waits for the fixed reaper before returning that evidence.
Durable segment publication/authority, retention/reclamation, unbounded or
time-based retry, multi-segment or runtime query, adapters, manifest-backed latest
projection, pressure retry/clear or
continued degraded ingress, stale-restore custody, and broad repair remain absent.
The reviewed future V2 publication contract does not make any of those behaviors
present and remains blocked on measured native results and the fresh owner
checkpoint described above.
Published observations never imply current or held values.

The ignored `_roadmap/` directory is local and unpublished.
Do not commit or push the `_roadmap/` directory to github. Preserve unrelated
work and never use reset, stash, or broad cleanup to make a task appear clean.

## Package roles and dependencies

- `native`: product code and the only allowed default-member role;
- `adapter`: future edge integration that may depend inward on native code;
- `tooling`: repository automation, excluded from the product closure.

Keep root role ownership and each package's `package.metadata.och` declaration in
sync. Native code cannot depend on adapters or tooling, directly or transitively.
Adapters and tools cannot become implicit defaults. Match forbidden dependencies
by resolved Cargo package identity, not a dependency alias. Product crates must
inherit the workspace lints, include `#![forbid(unsafe_code)]`, and deny missing
public documentation.

The ordinary `och-runtime -> och-store -> och-core` product path adds no
exception and no third-party package. Tokio remains forbidden except for the
exact direct `och-runtime -> tokio` edge
with default features disabled and only `rt` and `sync`; never route it through a
helper or admit it to `och-core`, `och-store`, or another native root. Policy must verify both
that exact normal, non-optional manifest declaration and Tokio's resolved unified
feature set. The private `och-v2-evidence` tooling package may independently
declare the same pinned Tokio version with default features disabled and only
`rt` and `sync`, solely to drive the hidden runtime facade; this does not enter
defaults or the native closure. Do not introduce Arrow, Parquet, DataFusion, Flight, tonic/prost,
SQLx, PostgreSQL, object/cloud providers, embedded databases, memory mapping,
Studio, Engine, or donor code into the native model. A dependency needed only by
a policy or build check belongs under `tools/`, not in a native crate.

## Required workflow

1. Read the applicable brief and repository docs before editing.
2. Use graph-aware discovery first when symbols exist, then exact source tools.
3. Make the smallest coherent change and add focused deterministic tests.
4. Run focused tests, `./scripts/gate.sh pr`, and the release gate only when the
   task requests its heavier evidence.
5. State exact commands and skips. Never claim unavailable platform evidence.

Cargo-nextest 0.9.143 is the primary test runner. It does not execute doctests,
so retain the separate `cargo test --workspace --doc --locked` command. CI and
local development must call the same gate scripts. PR CI stays lean and Linux;
fresh advisories, clean feature configurations, and baseline work belong to the
manual/release-cycle workflow.

Comments and docs should explain why a constraint exists and who owns future
behavior. Do not add speculative abstractions, empty crates, unreviewed semantic
types, or compatibility claims. Do not publish crates or mutate remote repository
state unless a separate owner instruction explicitly authorizes it.
