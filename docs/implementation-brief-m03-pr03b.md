# M03-PR03b Store Format V2 contract implementation brief

## Objective and present-state fence

Check in one documentation-only review barrier for a future Store Format V2 in
which every nonempty rotation must publish a retained raw Journal V1 seal and an
exact Published Native Segment V1 in one sole-writer transaction. This PR changes
no code or current format. Store Format V1 remains the only implemented format,
and all proposed V2 names and bytes remain unsupported or unknown to current code.

The owner approved the design contract, not implementation. No implementation may
begin until the bounded streaming resource plan and evidence prerequisites below
receive separate review and owner approval.

## Approved authority decisions

1. Store Format V2 is new-store/current-only; there is no V1 migration.
2. Every nonempty V2 rotation publishes one retained sealed Journal V1 and one
   exact `OCHSEG01` Published Native Segment V1.
3. Both artifacts are prepared and published by the existing sole writer in one
   manifest-rooted transaction.
4. Manifest V2 remains the sole commit point and publishes last.
5. A committed missing, corrupt, foreign, malformed, or catalog-mismatched
   segment fails closed. Retained raw bytes provide no implicit fallback.
6. Full committed raw/segment payload validation is eager on open and must use an
   approved bounded streaming/multipass design.

Exact bytes and names are split across the [Store Format V2](store-format-v2-contract.md),
[Manifest V2](manifest-v2-contract.md),
[Generation Catalog V2](generation-catalog-v2-contract.md), and
[Published Native Segment V1](published-native-segment-v1-contract.md) contracts.

## Mandatory sole-writer rotation order

Under the retained stable `store-v1.lock` and active-generation lock, a future V2
rotation follows this exact order:

1. **Read-only preflight.** Validate the selected Manifest V2 root, active range,
   mechanical checkpoint, registry coverage, catalog capacity, exact namespace,
   and every knowable bound. Prepare the exact 128-byte Rotation Intent V2, raw
   identity, segment identity, Catalog V2 candidate, empty successor bytes, and
   Manifest V2 candidate before the first mutation. The approved streaming or
   multipass plan must derive those exact identities without materializing the
   current over-700-MB in-memory builder path.
2. **Intent.** Exclusively publish, synchronize, bounded-read back, and exact-decode
   `journal-rotation-v2.intent`.
3. **Retained raw seal.** Exclusively stage and stream the exact durable source,
   synchronize it, perform complete framing/declaration/range/length/checksum
   verification, rename it to the canonical sealed Journal V1 final, and
   synchronize the directory.
4. **Published segment.** Exclusively stage and stream/multipass-build exact
   Native Segment V1 bytes, synchronize them, perform a complete hostile parse
   and exact source-link verification, rename to the canonical segment final,
   and synchronize the directory.
5. **Successor.** Create and synchronize the exact empty successor active Journal
   V1 and checkpoint at the prior global sequence cutoff.
6. **Catalog.** Publish, synchronize, bounded-read back, and exact-decode the next
   Generation Catalog V2 candidate containing the raw and segment identities.
7. **Commit.** Publish and directory-synchronize Manifest V2 last. Only then adopt
   the successor and narrowly remove exact redundant staging and intent evidence.

No step may expose a successful rotation or durable authority before step 7.
Storage-pressure custody and all other failure paths must preserve the existing
no-false-commit law.

## Crash and reopen law

Before Manifest V2 commit, the prior exact manifest root remains sole authority.
Reopen may roll back and remove only intent-proven uncommitted transaction
artifacts to that prior exact root, or it refuses unchanged. It never adopts an
uncommitted raw seal, segment, catalog, or successor and never guesses from
directory names or bytes without the complete relation.

After Manifest V2 commit, reopen validates the exact manifest/catalog/raw/segment/
successor relation before authority adoption. It may remove only proven redundant
staging and intent evidence. Any missing, corrupt, foreign, malformed, partial,
future, forked, unrelated, excessive, or mismatched committed evidence refuses
unchanged.

There is no broad fallback, raw deletion, segment fallback, rebuild, repair,
migration, or second writer. Each injected interruption must converge to exactly
the prior root or exactly the committed V2 root; ambiguity refuses unchanged.

## Resource and implementation hard stop

The existing in-memory Native Segment V1 builder can require more than 700 MB at
current maximum bounds. It is not authorized on the sole-writer path. The design
also requires complete eager validation of all committed raw/segment payloads on
open, whose latency and workspace have not been approved.

Before implementation, a separate bounded streaming/multipass proposal must
define and prove:

- fixed buffer, index, and temporary-workspace limits;
- an exact peak resident-memory bound for writer rotation and open;
- writer-delay and open-latency measurements at relevant bounds;
- deterministic cleanup and pressure custody at every publication phase;
- complete hostile parsing and source-link validation without semantic weakening;
- fault injection at every write, sync, readback, rename, directory-sync,
  publication, adoption, and cleanup phase; and
- explicit owner approval of the measured resource and latency budget.

The numeric memory budget and all writer-delay/open-latency SLOs are `UNKNOWN`.
This contract makes no publication benchmark claim.

## Future acceptance evidence matrix

This documentation PR claims none of the implementation evidence below. A future
implementation is incomplete until each row has independent, deterministic proof.

| Future requirement | Required evidence | Docs-PR status |
| --- | --- | --- |
| Marker, intent, catalog, and manifest primitives | Independent primitive-only byte oracles for every field, reserved range, endian rule, checksum, exact length, and hostile variant | Not implemented or claimed |
| Published segment bytes | Independent full-segment oracle proving exact unchanged `OCHSEG01` bytes, source reconstruction, indexes, and trailer | Not implemented or claimed |
| Namespace and inventory | Exact recognized-name oracle, 156-entry maximum, unknown-name refusal, and no orphan/gap/alternate segment | Not implemented or claimed |
| Epoch fence | V1, V2, markerless, historical, and mixed-format refusal before lock creation/acquisition or mutation, with before/after equality | Not implemented or claimed |
| Transaction convergence | Fault injection at every phase proving only exact prior-root rollback or exact committed-root adoption | Not implemented or claimed |
| Pressure and receipts | Storage-pressure custody, no false manifest/catalog/segment commit, and no false durable receipt | Not implemented or claimed |
| Committed fail-closed behavior | Missing, corrupt, foreign, malformed, truncated, excessive, and catalog-mismatched segment refusal with exact inventory equality | Not implemented or claimed |
| Raw/segment linkage | Full checksum/hostile parse proving exact StoreId, generation, range, registry, raw length/CRC, frame coverage, and catalog identity | Not implemented or claimed |
| Bounds | Catalog entries 1 and 64 succeed, entry 65 refuses; inventory 156 is accepted only in a valid state and 157 refuses | Not implemented or claimed |
| Streaming resources | Exact peak resident-memory bound plus writer-delay/open-latency evidence for minimum, representative, and maximum legal inputs | `UNKNOWN`; prerequisite |

## Explicit deferrals

This slice contains no implementation, public API, current V1 code or format
change, V1 migration, codec-backed segment, query/runtime integration,
multi-generation merge, cursor, compaction, pin, retention/reclamation, raw
deletion, degraded fallback, repair/rebuild, adapter, dependency, memory mapping,
or publication benchmark claim.

M03-PR03a/TVBP bytes remain private transient test evidence and are ineligible for
persistence. Any codec-backed Native Segment V2 requires separate format review
and a later Store Format successor; no tags or names are reserved here.

## Docs-PR acceptance commands

```console
git diff --check
./scripts/gate.sh pr
```

The release gate is outside this documentation-only slice.
