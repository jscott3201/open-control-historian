# M03-PR03b Store Format V2 contract continuation

## Delivered review boundary

M03-PR03b checks in a documentation-only Store Format V2 design and authority
contract. It freezes the names, exact primitive layouts, authority relationships,
single-writer publication order, crash convergence law, fail-closed open law, and
future evidence prerequisites for mandatory Published Native Segment V1 on every
nonempty V2 rotation.

This is not implementation evidence. Product, test, tooling, Cargo, and current
V1 format bytes remain unchanged. Current code remains Store Format V1-only and
continues to reject every proposed V2 marker, manifest, catalog, intent, segment
name, and mixed inventory as unsupported or unknown.

## Frozen future identities

- Store Format V2 uses marker magic `OCHFMT02`, version `2`, and an exact 32-byte
  marker under `store-format-v2.och`.
- Manifest V2 uses `OCHMAN02`, version `2`, the unchanged 160-byte field positions,
  and roots only Generation Catalog V2 at bytes `132..156`.
- Generation Catalog V2 uses `OCHCAT02`, an exact 64-byte header, exact 80-byte
  entries, at most 64 entries, and an exact 5,188-byte maximum.
- Rotation Intent V2 uses `OCHROT02`, version `2`, and an exact 128-byte record
  binding raw and segment identities before either can become authority.
- Published Native Segment V1 uses the exact existing `OCHSEG01` bytes and the
  canonical generation-derived final name. It is not Native Segment V2.

Registry, retry, and recovery artifacts remain their current V1 families. Active
journals, checkpoints, frames, and retained raw seals remain Journal V1. The
stable lock remains `store-v1.lock`.

The exact V2 inventory cap is `91 + 64 + 1 = 156`: the current equivalent
recognized inventory maximum, plus 64 segment finals, plus one segment staging
name. Replacing the V1 marker/manifest/catalog/intent names with V2 names is
count-neutral.

## Authority and failure boundary

Manifest V2 publishes last and is the sole commit point. Before it commits,
reopen may return only to the exact prior root or refuse unchanged. After it
commits, reopen must completely validate the manifest, Catalog V2, retained raw
seal, Published Native Segment V1, and successor relation before adoption.

Committed cleanup retains `journal-rotation-v2.intent` as proof while it
idempotently removes the exact predecessor active Journal V1, synchronizes the
directory, removes the predecessor checkpoint, synchronizes the directory,
removes exact-matching raw, segment, catalog, and manifest staging in fixed
publication order with a directory sync after each present removal, and proves
the exact clean committed inventory. Only then may it remove the intent last and
synchronize the directory again. The retained raw-seal and Published Native
Segment V1 finals are never cleanup targets. A cleanup crash revalidates the
committed relation and resumes only that exact proof-backed prefix; it never falls
back to the prior root. Missing or ambiguous proof refuses unchanged, and an
absent intent is accepted only with an already-clean committed inventory.

Full raw and segment payload validation is eager on open. A committed segment
that is missing, corrupt, foreign, malformed, excessive, or catalog-mismatched
refuses unchanged. Retained raw Journal V1 bytes never provide implicit open,
query, rebuild, repair, or degraded fallback. Segment publication grants no raw
deletion, registry, declaration, retry, receipt, query, retention, or reclamation
authority.

## Implementation hard stop and successor handoff

The existing in-memory segment builder can exceed 700 MB and is not authorized on
the sole-writer path. Implementation requires a separately reviewed bounded
streaming/multipass plan with fixed buffer/index/workspace limits, an exact peak
resident-memory bound, writer-delay and eager-open latency evidence, complete
fault injection, and owner approval. Numeric budgets and SLOs remain `UNKNOWN`.

The required future implementation matrix is recorded in the
[M03-PR03b implementation brief](implementation-brief-m03-pr03b.md). This docs PR
claims none of its byte-oracle, namespace, epoch-refusal, convergence, pressure,
fail-closed, predecessor/staging/intent-last cleanup-fault, bound, memory, or
latency evidence.

No migration, codec-backed bytes, query/runtime integration, multi-generation
merge, cursor, compaction, pin, retention/reclamation, raw deletion, fallback,
repair/rebuild, adapter, dependency, memory mapping, or benchmark belongs to this
slice. M03-PR03a/TVBP remains private transient test evidence with no persistence
or compatibility status.
