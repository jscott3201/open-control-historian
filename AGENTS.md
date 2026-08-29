# Repository instructions for coding agents

These instructions apply to the entire repository.

## Current authority boundary

This workspace proves only the M00-PR01 foundation. It has no Historian semantic
model, runtime, storage, persistence, query, or adapter implementation. M00-PR02
owns canonical identity, value, time, quality, ordering, collection, and gap
semantics. Do not invent those APIs in foundation work.

The ignored `_roadmap/` directory is local and unpublished. Do not read, edit,
stage, copy, summarize, or publish it. Do not read PDF files. Preserve unrelated
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

Do not introduce Tokio, Arrow, Parquet, DataFusion, Flight, tonic/prost, SQLx,
PostgreSQL, object/cloud providers, embedded databases, memory mapping, Studio,
Engine, or donor code in this foundation. A dependency needed only by a policy
or build check belongs under `tools/`, not in a native crate.

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
behavior. Do not add speculative abstractions, empty crates, semantic types, or
compatibility claims. Do not publish crates or mutate remote repository state
unless a separate owner instruction explicitly authorizes it.
