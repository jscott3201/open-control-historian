# M00-PR03 independent evidence record

M00-PR02 stopped after the reviewed dependency-free canonical model and its
focused constructor/invariant tests. M00-PR03 now supplies independent oracle,
golden, and fixture-builder evidence for that frozen public contract without
changing production model source, manifests, dependencies, gates, or workflows.

## Delivered design

One integration-test target, `crates/och-core/tests/m00_independent_evidence.rs`,
is the only evidence layer allowed to import and call `och_core`. It adapts raw
specifications into the actual public API and compares outcomes with two sibling
modules:

- `m00_independent_evidence/fixtures.rs` contains primitive valid builders,
  one-failure negative builders, ordering inputs, and retry-matrix inputs;
- `m00_independent_evidence/oracle.rs` independently checks written bounds,
  manually validates UUID shape, uses Euclidean timestamp arithmetic, compares
  primitive tuples, scans bounded collection evidence, classifies raw retries,
  and renders the ledger.

Neither sibling module imports `och_core`, model constants, constructors,
ordering helpers, classification methods, or error logic. The top-level target
also inventories the existing nominal-family compile-fail doctest. Golden
freshness and actual-versus-oracle comparisons are separate tests.

The checked-in
`crates/och-core/tests/fixtures/m00-pr03-evidence-v1.txt` file is a small ASCII/LF
line ledger with an explicit schema-1 header, 22 stable ordered case rows,
canonical decimal and lowercase hex facts, and no bless/update path. Its header
marks it test-only and explicitly denies wire, persistence, and API-compatibility
authority.

## Evidence inventory

The independent evidence covers the public semantics in
[the model contract](model-contract.md):

- canonical lowercase RFC 9562 `UUIDv7` text/bytes and nominal family separation;
- exact `RealBits` ordering/equality, integer extremes, text/token bounds, state,
  unavailable content, and external artifact/content identity;
- independent Euclidean negative, zero, positive, and extreme timestamps, exact
  reverse milliseconds, checked rejection, and deliberately unconstrained
  observation chronology;
- all quality levels and flags, absent/repeated/bounded native status, and
  full-range canonical producer numbers ordered epoch then sequence;
- the raw tuple `(effective, receive, observation ID)` separately from producer
  position order, with source and producer position proven excluded;
- all five immutable collection modes, interval metadata rules,
  observation-only/gap-only/mixed evidence, and half-open no-change/gap endpoints;
- exact 256-observation and 64-gap maxima plus 12 one-failure atomic rejection
  builders that establish one oracle violation before actual error comparison;
- one minimal deterministic constructor case for every one of the 24 sanitized
  `ModelError` variants current when M00-PR03 was accepted; successor inventories
  expand that historical count;
- the exact Equivalent/Conflict/Distinct retry matrix, including each content and
  scope/key distinction, plus secret-sentinel absence from both Debug forms and
  the ledger.

Retained-capacity behavior remains inventoried as implementation-owned evidence
in the existing unit tests. It is intentionally not promoted to oracle, wire, or
persistence authority.

## Next boundary

M00-PR03 does not own runtime lifecycle, durable retry state, persistence or wire
formats, serialization dependencies, hashing, storage/query behavior, adapters,
UUID generation, Studio/Engine integration, donor code, or platform services.
Those capabilities remain absent and require separately reviewed future slices;
the M00 ledger and tests must not be treated as their compatibility contract.
