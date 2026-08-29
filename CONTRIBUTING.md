# Contributing

The repository is intentionally smaller than the product its name anticipates.
Keep changes bounded and describe why a boundary or behavior belongs before
adding implementation.

## Prerequisites

- Rust 1.98.0 with `rustfmt` and `clippy` (pinned by
  [`rust-toolchain.toml`](rust-toolchain.toml));
- cargo-nextest 0.9.143;
- cargo-deny 0.20.2;
- Python 3 and a POSIX shell for repository gates.

Use prebuilt cargo-nextest and cargo-deny artifacts when available. The gate
fails closed when a pinned tool or version is missing.

## Development loop

Run focused tests before the broad gate. For dependency-policy work:

```console
cargo +1.98.0 test -p och-policy --locked
cargo +1.98.0 run --locked -p och-policy -- check --manifest-path Cargo.toml
```

Run the complete lean gate before handing work off:

```console
./scripts/gate.sh pr
```

The primary test runner is cargo nextest:

```console
cargo +1.98.0 nextest run --workspace --locked --profile ci --no-tests=fail
```

Nextest does not run doctests. Keep this separate command green rather than
running the full test suite twice:

```console
cargo +1.98.0 test --workspace --doc --locked
```

Release maintainers or manual CI run `./scripts/gate.sh release` for advisory,
clean feature-configuration, and baseline evidence. Do not report that gate or
an untested platform as green unless it actually ran.

## Dependency and package changes

Every workspace package needs an explicit `package.metadata.och.role` and a
matching owner list in root workspace metadata. Product packages (`native` and
future `adapter`) must inherit strict workspace lints, forbid unsafe code, deny
missing docs, and declare those policies in package metadata. Tool dependencies
belong only to tooling packages.

Before adding a dependency, read [the dependency policy](docs/dependency-policy.md).
Native packages cannot point to adapters or tools. Future adapters may depend on
native packages but must never be implicit default members. Do not add empty
adapter placeholders merely to reserve names.

## Documentation and tests

Public Rust APIs require useful rustdoc. Repository comments and prose should
explain decisions, ownership, lifecycle, or failure behavior rather than narrate
syntax. Keep the README truthful as capabilities change, and update architecture
and baseline evidence in the same bounded change when their claims change.

Policy changes require deterministic positive and negative tests. Include direct
and transitive paths, package aliases, role reversals, default selection,
malformed metadata, and traversal termination where relevant. Tests must be
parallel-safe and must not rely on mutable external state.

The repository hygiene check validates local documentation links, required
instructions, UTF-8 text, file-size bounds, trailing whitespace, and
high-confidence secret forms. It deliberately rejects opaque binary or PDF files
rather than silently skipping them.
