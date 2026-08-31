# OpenControl Historian

OpenControl Historian is at its canonical-model, manifest-rooted active-journal,
and bounded offline Native Segment V1 candidate/query stage.
This repository provides a small Rust workspace, an enforceable native dependency
boundary, the dependency-free canonical Historian data model, bounded series
declaration authority, source/capture provenance and admission boundary,
independent deterministic contract evidence, and one filesystem-backed runtime
path with exact byte reservation, a dedicated blocking writer, Journal V1 append,
group barriers, crash-safe manifest-backed durable receipts, bounded canonical
registry persistence, a bounded durable replay/guard horizon, bounded
successor rotation and raw-Journal sealing, bounded reopen evidence, and volatile
latest-observation snapshots. Current-V1 reopen may also commit one bounded report
while removing only a proven terminal invalid/torn active suffix; valid post-root
frames and ambiguity refuse unchanged. Store-owned mutating boundaries also
report typed standard-library storage pressure and put that live store handle in
sticky reopen custody without adding durable state. The runtime now retains the
first bounded typed pressure evidence, closes ingress before waking waiters, and
joins the existing fixed reaper before pressure shutdown returns. `och-store` can
also build and hostile-parse one bounded non-authorizing Native Segment V1
candidate from one committed sealed raw generation without publishing it or
changing store authority. An already parsed in-memory candidate supports one
hard-bounded, recent-first observation query for an exact series and optional
canonical effective-time interval. It does **not** yet provide durable segment
publication, a store/runtime historical query path, retention/reclamation,
unbounded or time-based retry, or current/held-value behavior.

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
`och-runtime` now opens one explicit immutable store scope asynchronously after
the dedicated blocking writer has created or validated and locked the active
artifacts. Its synchronous fixed 16-command ingress accepts only complete
`CanonicalAdmission` evidence, reserves exact frame bytes before allocation,
preserves FIFO across protected/normal/bulk resource classes, and coalesces only
equivalent outstanding retries. Handled receipts identify an append after its
volatile publication decision; distinct durable receipts are released only after
the group barrier covers that append in the journal, checkpoint, Retry State V1,
and committed Manifest V1. At a safe nonempty size/count/age boundary,
the sole writer completes ordinary durability, seals the raw Journal V1 bytes,
commits an empty successor through Generation Catalog V1 and Manifest V1, and
then continues the same global append sequence. Completed equivalents replay their original exact
handled/durable proof while the FIFO outcome tier retains them; overflow becomes
a bounded expired/conflict guard, and only eviction from both tiers makes a key
fresh. Public lifecycle and bind requests cross a fixed 16-request
nonblocking admission bound before sharing the same control gate and sole writer
as append publication. New bindings require the current active declaration,
while append validates the admission's exact retained historical declaration.
Slots and byte reservations remain held through durability. Cloneable read handles
capture store-scoped immutable latest snapshots; latest restarts empty and never
becomes recovery or declaration authority. Graceful shutdown drains, forces a
final barrier, seals latest, and joins. Drop signals fail-stop without blocking;
a fixed reaper owns the eventual blocking-thread join.

`och-store` owns the fixed Store Format V1 reset marker, store-scoped Journal V1
framing and hostile bounded decode, generation-one names plus deterministic
successor active pairs, a never-renamed stable store lock, generation-scoped
journal locks, two reusable 160-byte Manifest V1 slots, three reusable complete
registry and retry slots, three Generation Catalog V1 slots, three Recovery State
V1 report slots, and at most 64 immutable sealed raw-Journal generations. It
restores registry state only through public core lifecycle replay, requires exact
historical declarations for recovered and new admission bytes, and commits a
manifest only after the mechanical cutoff. Reopen exposes decoded records only
as non-authorizing evidence, restores only the manifest-referenced retry
projection, and does not rebuild latest. Normal open reads bounded catalog/header
metadata rather than every sealed payload byte. Each Manifest V1 root binds the exact
active successor/floor to the last sealed catalog entry; consecutive catalogs
append one entry to an identical prefix, and extra recognized generation files
refuse. A verified strict-prefix catalog left by an ordinary postcommit cleanup
interruption is the sole narrow redundant-catalog exception. A durable receipt proves the
manifest names the exact active generation/cutoff and retains its original commit
across later rotation under the stated platform contract; it is not a final
queryable-segment or universal physical-power-loss claim. Recovery first proves
the selected current root and every registry, retry, catalog/seal,
active/checkpoint, declaration, and report relationship under both retained
locks. A successful recovery remains runtime `Healthy`; inspection exposes the
latest committed report as event history, not proof it occurred during that open.
Direct active-journal and manifest-store inspection also expose volatile write
custody. Logical bounds and all knowable exact candidate records are preflighted
before each transaction's first mutation; this is not a physical-space or
future-I/O guarantee. Runtime inspection projects composed write custody and
first-wins path-free pressure evidence. Pressure atomically establishes
`StoragePressure` health before fail-stop wakes receipt/latest waiters; consuming
shutdown then waits for the fixed reaper and returns that exact evidence. No
pressure retry, clear, continued degraded ingress, or new receipt/latest variant exists.
Its dependency-free Native Segment V1 candidate retains complete original frames
in one SeriesId-ordered block per series, plus global append and recent-observation
directories. Candidate bytes remain in memory/offline, are rejected as store
inventory, and grant no declaration, runtime, durable query, retention, or
reclamation authority. Query results remain bounded non-authorizing projections
of already hostile-validated candidate bytes.

The current workspace contains:

| Package | Role | Purpose |
| --- | --- | --- |
| [`och-core`](crates/och-core/) | native | Dependency-free canonical model, bounded series declaration authority, and a measurement-only example |
| [`och-runtime`](crates/och-runtime/) | native | Store-scoped byte admission, durable retry replay/guard classification, automatic safe-boundary rotation, recovery/pressure inspection, writer-serialized registry control, manifest-backed receipts, and volatile latest snapshots |
| [`och-store`](crates/och-store/) | native | Journal V1, bounded generation rotation/sealing, offline Native Segment V1 candidates and bounded in-memory observation query, conservative terminal-suffix recovery, typed pressure/reopen custody, stable locking, canonical registry/retry/catalog snapshots, manifests, and reopen inspection |
| [`och-policy`](tools/och-policy/) | tooling | Private Cargo-metadata dependency-law checker |

`och-core` has no dependencies. `och-store` depends inward on `och-core`, and
`och-runtime` depends inward on both. The
only forbidden-dependency exception remains the direct `och-runtime -> tokio`
edge with Tokio default features disabled and only `rt` and `sync`. Because core
is shared, the union native closure is three roots and five packages:
`och-core`, `och-runtime`, `och-store`, `tokio`, and `pin-project-lite`.
`och-policy` and its dependencies are tooling excluded from defaults.

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
- [M02-PR01b0 implementation brief](docs/implementation-brief-m02-pr01b0.md)
  specifies the dependency-light Journal V1 semantic framing slice.
- [M02-PR01b0 continuation](docs/continuation-m02-pr01b0.md) records exact
  framing/decode evidence and its historical hard boundary before PR01b1.
- [M02-PR01b1 implementation brief](docs/implementation-brief-m02-pr01b1.md)
  specifies the sole active-journal durable runtime vertical and its bounds.
- [M02-PR01b1 continuation](docs/continuation-m02-pr01b1.md) records its exact
  ownership, evidence, platform qualification, and remaining PR02 boundary.
- [M02-PR02a implementation brief](docs/implementation-brief-m02-pr02a.md)
  specifies the manifest-rooted canonical registry authority transition.
- [M02-PR02a continuation](docs/continuation-m02-pr02a.md) records the delivered
  historical header fence, bootstrap, commit ordering, evidence, and successor ledger;
  it is superseded for current durable-format behavior by the reset record below.
- [M02-PR02b implementation brief](docs/implementation-brief-m02-pr02b.md)
  specifies the bounded durable two-tier retry authority transition.
- [M02-PR02b continuation](docs/continuation-m02-pr02b.md) records exact retry
  historical publication, atomic runtime handoff, compatibility, evidence, and successors.
- [M02-PR02c implementation brief](docs/implementation-brief-m02-pr02c.md)
  specifies the bounded rotation/seal authority transition and exclusions.
- [M02-PR02c continuation](docs/continuation-m02-pr02c.md) records exact
  historical generation, convergence, compatibility, evidence, and successor boundaries.
- [Store Format V1](docs/store-format-v1.md) defines the fixed reset epoch marker
  and mutation-free refusal fence.
- [M02 durable-format reset brief](docs/implementation-brief-m02-v1-durable-format-reset.md)
  records the current-only authority transition and exclusions.
- [M02 durable-format reset continuation](docs/continuation-m02-v1-durable-format-reset.md)
  records implementation evidence, bounds, and the then-deferred recovery handoff.
- [Journal V1 format](docs/journal-v1-format.md) defines the exact version-one
  header, frame, payload, active-artifact, checkpoint, byte-order, bound,
  checksum, and refusal contracts.
- [Manifest V1 format](docs/manifest-v1-format.md) defines the fixed inventory,
  reset fence, manifest and registry bytes, retry reference, publication order,
  bounds, and strict-reopen contract.
- [Retry State V1 format](docs/retry-state-v1-format.md) defines exact durable
  replay/guard bytes, capacities, canonical ordering, and refusal law.
- [Generation Catalog V1](docs/generation-catalog-v1-format.md) defines the fixed
  bounded sealed-generation inventory and reference law.
- [Sealed raw Journal V1](docs/sealed-journal-v1-format.md) defines the immutable
  pre-segment artifact and streaming verification contract.
- [Recovery State V1](docs/recovery-state-v1-format.md) defines the exact durable
  report bytes, manifest reference, convergence law, and diagnostic semantics.
- [Native Segment V1](docs/native-segment-v1-format.md) defines the exact bounded
  offline candidate bytes, source proof, hostile parser law, indexes, and explicit
  non-authority boundary.
- [M02-PR03a implementation brief](docs/implementation-brief-m02-pr03a.md) and
  [continuation](docs/continuation-m02-pr03a.md) record conservative current-V1
  recovery evidence and the remaining deferrals.
- [M02-PR03b1 implementation brief](docs/implementation-brief-m02-pr03b1.md) and
  [continuation](docs/continuation-m02-pr03b1.md) record store-only logical
  preflight, typed pressure evidence, sticky reopen custody, and the completed PR03b2 handoff.
- [M02-PR03b2 implementation brief](docs/implementation-brief-m02-pr03b2.md) and
  [continuation](docs/continuation-m02-pr03b2.md) record runtime pressure evidence,
  fail-stop ordering, receipt/latest preservation, and reaper-joined shutdown.
- [M03-PR01a implementation brief](docs/implementation-brief-m03-pr01a.md) and
  [continuation](docs/continuation-m03-pr01a.md) record the exact one-generation
  Native Segment V1 candidate, independent oracle, read-only bridge, and deferred
  publication/query/reclamation boundary.
- [Native Segment V1 observation query](docs/native-segment-query-v1.md) defines
  the bounded one-series recent-first result, interval, truncation, complexity,
  and non-authority contract.
- [M03-PR02a implementation brief](docs/implementation-brief-m03-pr02a.md) and
  [continuation](docs/continuation-m03-pr02a.md) record the in-memory query proof,
  focused evidence, and durable publication/cursor/merge deferrals.
- [M00-PR02 continuation](docs/continuation-m00-pr02.md) remains the historical
  handoff into this model delivery.

Contributor workflow is in [CONTRIBUTING.md](CONTRIBUTING.md), and automation or
coding-agent constraints are in [AGENTS.md](AGENTS.md).

## License

This repository is licensed under either of
[Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option. The
workspace is private to this repository (`publish = false`); no public crate
publication is configured.
