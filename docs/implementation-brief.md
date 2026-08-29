# M00-PR01 foundation implementation brief

## Outcome

This foundation delivery creates a Rust 1.98.0, edition-2024 workspace with an
empty native anchor and an executable dependency law. It establishes local and
GitHub Actions gates, dual licensing, contributor/agent guidance, architecture
truth, and initial baseline evidence. It does not implement Historian semantics.

## Included

- resolver 3 with `och-core` as the sole default member;
- inherited unsafe, missing-doc, rustdoc, and strict clippy policy;
- release/profiling profiles and `publish = false` packages;
- explicit native/adapter/tooling role ownership;
- all-feature Cargo metadata traversal from native roots by package identity;
- deterministic fixture and actual-workspace policy tests;
- cargo-nextest 0.9.143 CI configuration without retry-to-green behavior;
- cargo-deny 0.20.2 license/source/ban checks and release advisories;
- one lean Linux PR workflow and one manual/version-tag release-cycle workflow;
- documentation/link/instruction, file-size, UTF-8, whitespace, and no-secret
  checks that do not enter ignored or build-output trees;
- clean release feature checks and bounded baseline generation.

## Excluded

Canonical identity/value/time/quality/order/collection/gap/retry semantics,
runtime behavior, persistence, storage, query, adapters, publication, Studio or
Engine dependencies, donor compatibility, and large platform/data dependencies
are all outside M00-PR01. Future adapter roles are documented and tested with
fixtures rather than represented by empty crates.

## Acceptance commands

Focused policy evidence:

```console
cargo +1.98.0 test -p och-policy --locked
cargo +1.98.0 run --locked -p och-policy -- check --manifest-path Cargo.toml
```

Canonical gates:

```console
./scripts/gate.sh pr
./scripts/gate.sh release
```

The PR command owns formatting, locked default build/check, workspace clippy and
nextest, separate doctests, graph policy, rustdoc, repository checks,
non-network-heavy deny checks, and `git diff --check`. Release mode adds fresh
advisories, clean default/no-default/all-present-feature evidence, and the native
baseline. Workflow files invoke these same commands rather than reimplementing
them.
