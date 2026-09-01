# M03-PR03g1 private capability-contained V2 executor foundation

## Objective and authority

M03-PR03g1 extends the existing private `och-v2-evidence` tooling package only
with a capability-contained disposable V2 executor foundation for the PR03e semantic
fault descriptors. It changes no `crates/` code, product API, default feature,
Store Format V1 byte or authority, and accepts no Store Format V2 product byte.

This slice provides no complete harness, matrix result, report bundle, collector,
measurement, or all-boundary child-crash/reopen campaign. It does not authorize
collection. Every `PR03E-M01..M11` row remains `UNSATISFIED`; all native timing,
RSS, workspace, threshold, budget, and SLO values remain `UNKNOWN`.

## Foundation

The private foundation retains 173 literal descriptors covering P0 through P7,
precommit rollback, and eager open. At this reviewed revision, the private
`root::v2_io` subtree exclusively receives the issued `V2StoreChild` capability
used by the disposable V2 executor. The capability is non-public, non-`Clone`,
non-`Copy`, owns private path state, and exposes no dereference, path conversion,
path-returning callback, or crate-visible path API. Rust privacy prevents code
outside that subtree from constructing or receiving the capability or invoking
its private V2 I/O API. `fault`, `inventory`, `oracle`, `schema`, and `transaction`
are descendants of that subtree; transaction execution receives the capability,
not `&Path`. `EvidenceRoot` offers callers only owner-specific path-free summary
operations. Current-V1 smoke has a separate private `root::v1_smoke` lifecycle.

The concrete compiled source-site inventory carries typed IDs and complete phase,
artifact, operation, mutation, partial-write, pressure, occurrence, commit-side,
root, successor, and terminal metadata. The compiled registry verifies the exact
literal `FaultId`/metadata/generated-wrapper function-pointer bijection, including
missing, duplicate, extra, wrong-metadata, and wrong-wrapper refusal. This is
bounded capability containment and finite source-review evidence at the reviewed
revision. It is not a parser, source-language policy, or proof that arbitrary
present or future Rust source cannot perform filesystem I/O. Evidence-parent,
current-V1, and test-fixture I/O remains with its explicit private owner and is
outside the disposable V2 capability boundary.

Compact disposable execution proves every site under success and applicable
pre-operation, nonzero short-partial-write, `StorageFull`, and `QuotaExceeded`
injection. `CHILD_CRASH_AFTER_SUCCESS` remains explicitly registered as a g2
execution obligation and is not claimed by g1. Legal traces execute P0-P7 with
present and absent optional cleanup branches, intent-last cleanup, real
precommit rollback, and eager validation of 64 small raw/segment pairs one pair
at a time. Every short-write case compares immediate post-fault bytes, logical
length, and fingerprint with its distinct pre-fault state before independently
restoring the exact baseline. Every successfully acquired named
foundation/runtime-smoke child runs one operation result followed by exactly one
cleanup attempt. The original operation error takes precedence if cleanup also
fails; successful operation plus cleanup failure returns the cleanup error, and
ordinary error cleanup permits same-name retry. Best-effort `Drop` is only an
unwind fallback: no cleanup is claimed after process kill, panic-abort, failed
create, or unrecoverable filesystem failure.

The private primitive oracles retain marker/intent/catalog/manifest relationship
checks, Catalog bounds 1/64/65, canonical inventory bounds 156/157, safe-Rust
SHA-256 published vectors, complete bounded-file fingerprints, and hostile V1/V2/
mixed/markerless/unknown/non-file refusal. Fixture bytes remain only beneath newly
absent disposable case children and are reclaimed.

`root::v1_smoke` retains only current-V1 success and typed-pressure smoke through
`och_runtime::__m03_pr03e_native_harness`. That smoke proves facade consumption,
not V2 behavior.

## Dependency containment

The existing private tooling package adds exact direct normal edges to
`och-runtime` with only `m03-pr03e-native-harness` and Tokio `1.53.1` with default
features disabled and only `rt` and `sync`. Root defaults remain three native
crates, the native closure remains five packages, native code has no tooling edge,
and no `sha2` package is added.

## Unsupported command

Existing PR03c commands remain unchanged. g1 adds only:

```console
cargo +1.98.0 run --locked -p och-v2-evidence -- \
  native-foundation-check --root target/private-v2-foundation
```

The bounded summary states `COLLECTION_AUTHORIZED=false`, `REPORT_BUNDLE=ABSENT`,
all M rows `UNSATISFIED`, and no V2 product authority. `native-run`,
`native-validate`, `native-collect`, and hidden child mode are absent.

## Successor

M03-PR03g2 must separately add the complete matrix, nonempty timing/fault report
bundle, every-boundary parent-owned crash/reopen orchestration, sanitization, and
collection entry point. Only g2 acceptance may authorize a later Linux x86_64
collection. Measured evidence and a fresh owner checkpoint still precede any
product proposal.
