# OpenControl Historian

OpenControl Historian is at its canonical-model and bounded-runtime stage.
This repository provides a small Rust workspace, an enforceable native dependency
boundary, the dependency-free canonical Historian data model, bounded series
declaration authority, source/capture provenance and admission boundary,
independent deterministic contract evidence, and one caller-executor-owned Tokio writer
lifecycle with strict bounded volatile ingress and latest-observation snapshots
per runtime instance. It does **not** provide storage, persistence, durable
history, current/held-value, or query behavior.

## Current status

`och-core` now defines canonical store/series identity, immutable declaration
revisions, terminal retirement, exact value/content, timestamp, quality/status,
producer-order, collection-mode, gap/no-change, atomic envelope, registry-issued
declaration binding, declaration-authorized source/capture admission, and
content-qualified retry contracts described in the
[model contract](docs/model-contract.md). M00 also has an independent raw-fixture
oracle for both the original model and series lifecycle, public-model adapter
comparison, and checked-in schema-v1 ASCII golden
ledger under [`crates/och-core/tests/`](crates/och-core/tests/).
`och-runtime` adds async startup-after-readiness, one explicit immutable store
scope, one private writer, a synchronous fixed 16-command ingress that accepts
only complete `CanonicalAdmission` evidence, outstanding-only retry
coalescing/conflict rejection, shared terminal receipts, and a separately fixed
16-series runtime-local volatile latest registry. Cloneable synchronous read
handles capture store-scoped immutable snapshots;
graceful shutdown drains, seals the final registry, and joins, while Drop remains
nonblocking and abort-only on the caller's active Tokio executor. `WriterHandled`
means the writer consumed the command and completed its publication decision;
ineligible and stale commands remain handled no-ops. Published exact observations
are not current/held values and prove no storage, persistence, durable history,
query result, restart recovery, wire format, or adapter behavior. The volatile
runtime never consumes or mutates the series registry and gains no declaration,
source-interpretation, persistence, or durability authority.

The current workspace contains:

| Package | Role | Purpose |
| --- | --- | --- |
| [`och-core`](crates/och-core/) | native | Dependency-free canonical model, bounded series declaration authority, and a measurement-only example |
| [`och-runtime`](crates/och-runtime/) | native | Store-scoped caller-executor writer, canonical-admission ingress, and volatile immutable latest snapshots |
| [`och-policy`](tools/och-policy/) | tooling | Private Cargo-metadata dependency-law checker |

`och-core` has no dependencies. `och-runtime` depends inward on `och-core`; the
only forbidden-dependency exception remains the direct `och-runtime -> tokio`
edge with Tokio default features disabled and only `rt` and `sync`. Because core
was already a native root, the union native closure remains two roots and four
packages. `och-policy` and its dependencies are tooling excluded from defaults.

## Toolchain and checks

The repository pins Rust 1.98.0 (edition 2024), cargo-nextest 0.9.143, and
cargo-deny 0.20.2. Install the Rust components with rustup and install the two
Cargo tools from their prebuilt releases. Then run:

```console
./scripts/gate.sh pr
```

The PR gate formats, builds and checks the default native members, runs strict
workspace clippy, executes tests with cargo nextest, runs doctests separately,
proves the native metadata graph, builds rustdoc, checks repository hygiene, and
enforces non-advisory cargo-deny policy. Nextest does not execute doctests, so
`cargo test --workspace --doc --locked` remains an explicit non-redundant gate.

The network-capable and clean-build release evidence is intentionally separate:

```console
./scripts/gate.sh release
```

That mode adds fresh advisory checking, clean default/no-default/all-present
feature checks, and a bounded native baseline measurement. Tests still run once
through nextest rather than being repeated with `cargo test`.

## Design boundaries

- [Architecture](docs/architecture.md) describes the current package roles,
  canonical model boundary, and intentionally absent components.
- [Canonical model contract](docs/model-contract.md) records the exact public
  semantics, bounds, validation, and non-goals.
- [Dependency policy](docs/dependency-policy.md) explains the executable native
  closure law, the exact Tokio exception, and how future adapters must point inward.
- [M01-PR01 lifecycle contract and implementation brief](docs/implementation-brief-m01-pr01.md)
  records startup, shutdown, cancellation, error, and non-goal semantics.
- [M01-PR02 bounded-ingress delivery record](docs/continuation-m01-pr02.md)
  records admission, retry, receipt, drain, failure, bound, and non-goal evidence.
- [M01-PR03 latest-publication delivery record](docs/continuation-m01-pr03.md)
  records eligibility, producer-position ordering, registry bounds, immutable
  snapshots, and normal-seal/abnormal-unavailable behavior.
- [M00-PR02 implementation brief](docs/implementation-brief-m00-pr02.md) records
  this model slice's scope and acceptance evidence.
- [Foundation implementation brief](docs/implementation-brief.md) remains the
  historical M00-PR01 record.
- [Baseline](docs/baseline.md) records the initial dependency and binary-size
  evidence without inventing runtime measurements.
- [M00-PR03 evidence record](docs/continuation-m00-pr03.md) inventories the
  delivered independent oracle, golden, fixture builders, and non-goals.
- [M00-PR04 alignment and declaration-authority record](docs/continuation-m00-pr04.md)
  records the accepted predecessor baseline, bounded lifecycle contract,
  then-required pre-M02 source/capture successor, and explicit deferred ledger.
- [M00-PR05 source/capture crosswalk record](docs/continuation-m00-pr05.md)
  records the pinned Studio field crosswalk, bounded canonical admission contract,
  original M02-PR01 input boundary, and remaining deferred ledger.
- [M02-PR01a canonical-admission runtime record](docs/continuation-m02-pr01a.md)
  records the store-scoped runtime authority transition, exact volatile proof,
  accepted durable-journal split, and remaining M02 hard boundary.
- [M00-PR02 continuation](docs/continuation-m00-pr02.md) remains the historical
  handoff into this model delivery.

Contributor workflow is in [CONTRIBUTING.md](CONTRIBUTING.md), and automation or
coding-agent constraints are in [AGENTS.md](AGENTS.md).

## License

This repository is licensed under either of
[Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option. The
workspace is private to this repository (`publish = false`); no public crate
publication is configured.
