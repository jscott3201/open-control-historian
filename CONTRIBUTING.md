# Contributing

Thank you for considering a contribution. OpenControl Historian is an early-stage,
unpublished source workspace: package version `0.0.0`, `publish = false`, and no
supported product CLI, release, production-readiness claim, or support/response
SLA. Keep proposals and pull requests bounded, and explain why a behavior belongs
inside the current authority before adding implementation.

Participation must follow the [Code of Conduct](CODE_OF_CONDUCT.md). Never report
a vulnerability in an issue, pull request, or discussion; follow
[SECURITY.md](SECURITY.md). The conduct enforcement address is not a security
reporting fallback.

## Start here

Before proposing a change, read:

- the public [README](README.md) and curated [documentation index](docs/README.md);
- the current [architecture](docs/architecture.md),
  [canonical model contract](docs/model-contract.md), and
  [dependency policy](docs/dependency-policy.md);
- the relevant current format or historical delivery record; and
- [AGENTS.md](AGENTS.md), which records repository authority and implementation
  constraints that contributions must preserve.

Current durable authority is Store Format V1 only. Future V2 documents are
unimplemented review barriers, not permission to emit or accept V2 bytes, publish
Native Segment artifacts, add compatibility, or broaden runtime behavior.

## Issues and pull requests

Use the bug or feature issue form when it fits; blank issues remain available.
Search for existing reports first, remove sensitive information, and describe the
smallest reproducible problem or outcome. Opening an issue does not imply
acceptance, scheduling, support, or a response-time commitment.

Pull requests should address one coherent concern. State the motivation, bounded
scope and non-goals, API/durable-format/compatibility impact, exact validation
commands and platform, and every relevant skip or unavailable environment. Do not
mix dependency upgrades, formatting, renames, or cleanup into an unrelated change.
Never claim an unrun gate or untested platform as evidence.

Public capability, support, platform, security, compatibility, or contract claims
must be truthful and updated in the appropriate README, architecture, contract,
format, or delivery record in the same bounded change. Historical evidence must
remain labeled with its actual tool, platform, profile, and product/non-product
scope.

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
