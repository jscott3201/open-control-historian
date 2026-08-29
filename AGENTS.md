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
durable successor, and M02-PR02a now makes a bounded manifest the committed
description of its active range, mechanical cutoff, and complete canonical
registry history. `och-runtime` depends inward on `och-store`, opens one explicitly
bounded filesystem-backed store, admits only complete M00-PR05
`CanonicalAdmission` evidence, reserves exact encoded bytes before allocation,
and sends FIFO work to one dedicated blocking writer thread. Handled and durable
receipt stages are distinct; the latter is released only after journal sync,
checkpoint sync, and manifest publication cover the append. Public register,
revise, retire, and active bind requests share the sole bounded writer ordering
authority with append publication. A fixed reaper owns the eventual writer join
after nonblocking Drop.

`och-store` owns Journal V1 bytes, the header-v2 old-writer fence, stable
never-renamed store lock, retained journal lock, fixed active-artifact
create/open, bounded scan and append, double-slot mechanical checkpoint,
two-slot manifest, three-slot complete registry snapshots, strict bootstrap, and
publication. It restores snapshots only by public `SeriesRegistry` replay and
requires every decoded journal declaration to match retained historical
authority; decoded records never authorize registry state. `och-runtime` retains
the fixed 16-command count window, exact bounded
byte reservations through durability, outstanding-only retry coalescing, a
separately fixed 16-series volatile latest registry, store-scoped immutable
snapshots, group barriers, graceful drain/final barrier/seal/join, and
nonblocking fail-stop Drop. The blocking writer owns the one non-cloneable live
`SeriesRegistry`; runtime code gains no declaration/source interpretation
semantics and exposes no mutable registry handle. Latest state restarts empty and
completed retries are not restored. Successor rotation, long-term retry, query,
adapters, manifest-backed latest projection, and broad recovery remain absent.
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
feature set. Do not introduce Arrow, Parquet, DataFusion, Flight, tonic/prost,
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
