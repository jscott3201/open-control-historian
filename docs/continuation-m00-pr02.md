# Continuation note: M00-PR02

M00-PR01 stops at a dependency boundary. M00-PR02 retains exclusive semantic
authority for the first canonical Historian model.

Before adding runtime, persistence, query, or adapter behavior, M00-PR02 should
define and test the reviewed contracts for:

- observation identity and value representation;
- timestamp/time-domain and quality meaning;
- ordering and collection behavior;
- duplicate, missing-data/gap, and retry implications where in scope.

The next slice should not infer these contracts from the baseline example, which
prints only a build marker, or from the empty `och-core` crate. Keep APIs narrow,
document invariants and failure behavior, and preserve the native dependency law.
If semantics require a dependency currently forbidden by foundation policy, that
is an architecture decision requiring replanning rather than a policy bypass.

Runtime selection, storage/journal/segment formats, persistence, query engines,
Arrow/Parquet/DataFusion/Flight, network stacks, databases, cloud/object
providers, memory mapping, Studio/Engine integration, and adapters remain later
work unless a separately authorized brief moves a specific concern inward.
