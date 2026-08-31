# Published Native Segment V1 design and authority contract

> Review barrier only. Native Segment V1 exists today only as a transient
> non-authorizing candidate. This document defines its future Store Format V2
> publication identity without changing one segment byte.

Published Native Segment V1 is the exact existing `OCHSEG01` Native Segment V1
grammar persisted as a mandatory companion to each retained raw Journal V1 entry
in Generation Catalog V2. It is deliberately not called Native Segment V2.

Any future codec-backed bytes require a separately reviewed Native Segment V2 and
a later Store Format successor. This contract reserves no codec tag, name,
alternate payload, or compatibility path.

## Exact identity and names

The final name is
`native-segment-v1-g{generation:020}.och`; generation one is therefore
`native-segment-v1-g00000000000000000001.och`. The only staging name is
`native-segment-v1.staging`.

The final bytes are exactly the current
[Native Segment V1 format](native-segment-v1-format.md), including:

- magic `OCHSEG01`, version `1`, and the exact existing header and section layout;
- each complete original Journal V1 frame exactly once, byte-for-byte;
- source raw-Journal length, checksum, sequence range, generation, StoreId, and
  registry-generation linkage;
- deterministic series, global append, and recent-observation indexes; and
- the existing four-byte trailing CRC-32C over every preceding segment byte.

Generation Catalog V2 stores the exact complete segment length, including the
trailer, and the exact trailer checksum value. No wrapper, publication header,
sidecar, compression, dictionary, replacement value payload, or codec registry is
added.

The M03-PR03a private TVBP proof bytes are forbidden. They are not eligible for
the segment, catalog, intent, staging artifact, or any future compatibility
decoder.

## One-to-one publication law

Every retained Catalog V2 raw entry has exactly one Published Native Segment V1
with the same generation. The segment must reconstruct the exact raw source
identity carried by that entry. An orphan segment, missing segment, generation
gap, alternate final name, duplicate, or raw/segment mismatch is invalid
inventory or invalid committed authority.

The sole writer publishes the segment in the same rotation transaction as the
raw seal. It exclusively creates the fixed staging file, writes the exact bytes
through a future bounded streaming or multipass implementation, synchronizes it,
performs a complete hostile parse and source-link verification, renames it to the
canonical final, and synchronizes the directory before Catalog V2 and Manifest V2
publication. A complete final is still non-authoritative until Manifest V2 commits
the catalog that names its exact identity.

## Validation and fail-closed law

Publication performs complete raw and segment validation. Before authority
adoption on every future V2 open, all committed raw and segment payloads are
eagerly and completely checksum-validated and hostile-parsed with bounded
streaming/multipass resources. Validation includes every current Journal V1 and
Native Segment V1 framing, canonical-layout, frame, index, checksum, source
reconstruction, StoreId, generation, range, registry, raw length, and raw
checksum rule, plus exact Catalog V2 identity.

Detection of a missing, corrupt, foreign, malformed, excessive, or
catalog-mismatched committed segment refuses the store unchanged. The retained
raw Journal V1 does not provide implicit open, query, degraded-operation, rebuild,
or repair fallback. Detection is never silently weakened to metadata-only or
lazy-on-query validation.

The current in-memory candidate builder can exceed 700 MB and is forbidden on the
sole-writer publication path. The required eager-open validation also has no
approved resource or latency budget. A future implementation requires separately
reviewed fixed buffer, index, and workspace limits, an exact peak resident-memory
bound, writer-delay and open-latency evidence, phase-by-phase fault injection, and
owner approval. Those numeric budgets and SLOs are `UNKNOWN`; this contract does
not invent them.

## Non-authority boundary

Publication grants no registry, declaration, canonical admission, retry, receipt,
query, retention, reclamation, deletion, recovery, or repair authority. The raw
Journal V1 remains retained semantic and recovery evidence. A published segment
does not authorize raw deletion and does not imply a runtime or durable query API.

There is no PR03a/TVBP persistence, compression, dictionary, replacement payload,
codec registry, compatibility decoder, multi-generation merge, cursor,
compaction, pin, retention/reclamation, memory mapping, adapter, or query/runtime
integration in this contract.
