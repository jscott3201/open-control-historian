# OpenControl Historian

OpenControl Historian is at its evidenced canonical-model stage. This repository
provides a small Rust workspace, an enforceable native dependency boundary, the
dependency-free canonical Historian data model, and independent deterministic
contract evidence. It does **not** yet provide a Historian runtime or storage
behavior.

## Current status

`och-core` now defines the canonical identity, exact value/content, timestamp,
quality/status, producer-order, collection-mode, gap/no-change, atomic envelope,
and content-qualified retry contracts described in the
[model contract](docs/model-contract.md). M00 also has an independent raw-fixture
oracle, public-model adapter comparison, and checked-in schema-v1 ASCII golden
ledger under [`crates/och-core/tests/`](crates/och-core/tests/). There is still no
runtime, storage engine, persistence or wire format, query layer, or adapter.
Those absent capabilities must not be inferred from the model, tests, ledger, or
baseline example.

The current workspace contains:

| Package | Role | Purpose |
| --- | --- | --- |
| [`och-core`](crates/och-core/) | native | Dependency-free canonical model and a measurement-only example |
| [`och-policy`](tools/och-policy/) | tooling | Private Cargo-metadata dependency-law checker |

`och-core` has no product dependencies. `och-policy` and its dependencies are
workspace tooling and are excluded from the default native closure.

## Toolchain and checks

The repository pins Rust 1.98.0 (edition 2024), cargo-nextest 0.9.143, and
cargo-deny 0.20.2. Install the Rust components with rustup and install the two
Cargo tools from their prebuilt releases. Then run:

```console
./scripts/gate.sh pr
```

The PR gate formats, builds and checks the default native member, runs strict
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
  closure law and how future adapters must point inward.
- [M00-PR02 implementation brief](docs/implementation-brief-m00-pr02.md) records
  this model slice's scope and acceptance evidence.
- [Foundation implementation brief](docs/implementation-brief.md) remains the
  historical M00-PR01 record.
- [Baseline](docs/baseline.md) records the initial dependency and binary-size
  evidence without inventing runtime measurements.
- [M00-PR03 evidence record](docs/continuation-m00-pr03.md) inventories the
  delivered independent oracle, golden, fixture builders, and non-goals.
- [M00-PR02 continuation](docs/continuation-m00-pr02.md) remains the historical
  handoff into this model delivery.

Contributor workflow is in [CONTRIBUTING.md](CONTRIBUTING.md), and automation or
coding-agent constraints are in [AGENTS.md](AGENTS.md).

## License

This repository is licensed under either of
[Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option. The
workspace is private to this repository (`publish = false`); no public crate
publication is configured.
