# M02-PR02a implementation brief: manifest-rooted registry authority

> Historical delivery record. Its durable-format compatibility and opening
> claims are superseded by the current-only V1 durable-format reset.

## Exact baseline and authority

- Repository: `open-control-historian`
- Base: `c045e06aa9036d65edc62ebefa94070e48a91865`
- Delivery branch: `feat/m02-pr02a-manifest-registry`
- Prerequisites: accepted M02-PR01a, M02-PR01b0, and M02-PR01b1
- Owner decisions: explicit bounded registry snapshot for nonempty pre-manifest
  bootstrap; a later two-tier durable retry horizon; existing store, declaration
  producer, and per-record epoch evidence only; volatile latest remains empty on
  reopen.

## One objective

Make a bounded generation manifest the committed description of the one active
generation-one Journal V1 range, its mechanical durable cutoff, and the complete
canonical `SeriesRegistry` history and tombstones. A stable store lock and a
manifest-required 28-byte active-header version fence make this one writer-owned
authority; Journal V1 admission frame bytes remain unchanged.

## Included

- two bounded manifest slots and three bounded registry snapshot slots;
- checksummed, independently decoded manifest and registry formats;
- fixed staging names, file synchronization, atomic reusable-slot publication,
  and directory synchronization;
- one never-renamed store lock retained for the mutable open, plus the existing
  journal lock as the old-binary migration backstop;
- exact empty genesis and explicit bounded nonempty pre-manifest bootstrap;
- registry restore solely by replay through public `SeriesRegistry` operations
  followed by exact snapshot comparison;
- writer-serialized register, revise, retire, and active bind operations;
- historical declaration validation before append mutation;
- manifest publication after the mechanical checkpoint and before durable
  receipt release;
- deterministic byte, parser, lifecycle, lock, bootstrap, restart, ordering,
  and fault evidence, including a primitive-only independent format oracle;
- tracked architecture, model, format, and continuation records.

## Excluded and successor ledger

- Durable retry replay/expiry is M02-PR02b. It will use an outcome replay tier,
  then an expired/conflict guard tier, and treat a key as fresh only after both
  bounded tiers expire.
- Rotation, sealing, successor journals, and generic verified immutable test
  artifacts are M02-PR02c.
- Broad manifest fallback, convergence, corruption events, and recovery repair
  are M02-PR03a.
- Logical disk preflight, real write/sync pressure, and degraded operation are
  M02-PR03b; this slice makes no portable physical-free-space claim.
- Manifest-backed latest projection is a named M03 successor. M02 reopen keeps
  volatile latest empty.
- Queries, rollups, retention, reclamation, adapters, Studio/Engine changes,
  dependencies, unsafe code, and further `och-core` changes are excluded.

## Invariants

- `och-core` remains the sole declaration lifecycle authority.
- Decoded journal records never authorize registry state or new evidence.
- New bindings use `SeriesRegistry::bind` and therefore require the current
  active declaration.
- Append validation is historical: the admission declaration must exactly equal
  `SeriesRegistry::resolve(series, revision)`. Already-issued evidence remains
  admissible after correction or retirement.
- Registry and append control share one bounded writer ordering authority;
  lifecycle/bind admission is nonblocking and fixed at 16 retained requests.
- No returned lifecycle commit or durable receipt precedes manifest publication.
- Every public refusal is path-free and every required read/allocation is bounded.

## Acceptance commands

```console
cargo +1.98.0 test -p och-store --locked
cargo +1.98.0 test -p och-runtime --locked
cargo +1.98.0 test -p och-core --locked
cargo +1.98.0 test --workspace --doc --locked
git diff --check
./scripts/gate.sh pr
```

The release gate is not part of this slice.

## Risks and replan triggers

Stop rather than narrow silently if exact restore cannot be proven through public
core APIs; bounded single-writer ordering cannot be retained; the header fence
would alter Journal V1 frame bytes; publication requires an unbounded inventory
or unsafe fallback; a new dependency, unsafe code, runtime migration, retry,
rotation, latest rebuild, or broad recovery becomes necessary; or independent
format evidence cannot remain implementation-independent.
