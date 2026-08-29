# OpenControl Historian

OpenControl Historian is at its foundation stage. This repository currently
proves a small Rust workspace and an enforceable native dependency boundary; it
does **not** yet provide Historian behavior.

## Current status

There is no canonical observation model, identity/value/time/quality contract,
runtime, storage engine, persistence format, query layer, or adapter. Those
capabilities must not be inferred from the project name or the baseline example.
M00-PR02 is reserved for the first semantic model work.

The current workspace contains:

| Package | Role | Purpose |
| --- | --- | --- |
| [`och-core`](crates/och-core/) | native | Empty product-boundary anchor and a measurement-only example |
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

- [Architecture](docs/architecture.md) describes the current package roles and
  intentionally absent components.
- [Dependency policy](docs/dependency-policy.md) explains the executable native
  closure law and how future adapters must point inward.
- [Foundation implementation brief](docs/implementation-brief.md) records this
  slice's scope and acceptance evidence.
- [Baseline](docs/baseline.md) records the initial dependency and binary-size
  evidence without inventing runtime measurements.
- [M00-PR02 continuation](docs/continuation-m00-pr02.md) preserves the semantic
  authority boundary for the next delivery.

Contributor workflow is in [CONTRIBUTING.md](CONTRIBUTING.md), and automation or
coding-agent constraints are in [AGENTS.md](AGENTS.md).

## License

This repository is licensed under either of
[Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option. The
workspace is private to this repository (`publish = false`); no public crate
publication is configured.
