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
7. **Commit.** Publish and directory-synchronize Manifest V2 last. Revalidate the
   complete manifest/catalog/raw/segment/successor relation. Commit fixes the new
   root; no later cleanup fault permits fallback to the prior root.
8. **Adopt and clean.** Adopt the validated successor. Keep
   `journal-rotation-v2.intent` as cleanup proof while performing this exact,
   idempotent sequence: remove the intent/root-proven predecessor active Journal
   V1 and synchronize the directory; remove its exact predecessor checkpoint and
   synchronize the directory; then process any redundant transaction staging in
   fixed publication order: `sealed-journal-v1.staging`,
   `native-segment-v1.staging`, `generation-catalog-v2.staging`, and
   `manifest-v2.staging`. Each present staging artifact must exact-match its
   intent/root derivative before removal, and each removal is followed by a
   directory synchronization. Read back and prove the exact committed inventory,
   remove the intent last, and synchronize the directory once more. A repeated
   removal may accept absence only when the committed root, intent, and exact
   cleanup prefix prove that the earlier attempt completed it. The retained
   raw-seal final and Published Native Segment V1 final are never cleanup targets.

No step may expose durable authority before step 7 or transaction-complete success
before step 8. Storage-pressure custody and all other failure paths must preserve
the existing no-false-commit law. A postcommit cleanup failure reports the
committed-root truth and never manufactures prior-root fallback.

## Crash and reopen law

Before Manifest V2 commit, the prior exact manifest root remains sole authority.
Reopen may roll back and remove only intent-proven uncommitted transaction
artifacts to that prior exact root, or it refuses unchanged. It never adopts an
uncommitted raw seal, segment, catalog, or successor and never guesses from
directory names or bytes without the complete relation. Rollback retains the
intent through exact uncommitted-artifact and staging cleanup, synchronizes those
removals, removes the intent last, and synchronizes the directory.

After Manifest V2 commit, reopen validates the exact manifest/catalog/raw/segment/
successor relation before authority adoption and cannot return to the prior root.
While the exact intent remains, reopen uses it with the committed root to identify
the predecessor generation and resumes only the cleanup prefix in step 8:
predecessor active Journal V1, predecessor checkpoint, redundant staging, then
intent-last removal, with the specified directory synchronizations. A crash
before, between, or after any removal converges idempotently to the exact committed
root and exact canonical inventory. The retained raw seal and Published Native
Segment V1 remain present and exact throughout.

If intent/root proof is missing or ambiguous, a cleanup state is not an exact
prefix, or a retained final or committed relation is missing, corrupt, foreign,
malformed, partial, future, forked, unrelated, excessive, or mismatched, reopen
refuses unchanged. If the intent is absent, reopen accepts only an already-clean
committed inventory with no predecessor or transaction staging; it never guesses
that leftover evidence is redundant.

There is no broad fallback, raw deletion, segment fallback, rebuild, repair,
migration, or second writer. Each injected interruption must converge to exactly
the prior root or exactly the committed V2 root; ambiguity refuses unchanged.

## Resource and implementation hard stop

The existing in-memory Native Segment V1 builder can require more than 700 MB at
current maximum bounds. It is not authorized on the sole-writer path. The design
also requires complete eager validation of all committed raw/segment payloads on
open, whose latency and workspace have not been approved.

M03-PR03c now supplies a separately reviewed tooling-only streaming/multipass
prototype, a 160 MiB absolute target, zero external-sort plan, deterministic
fixtures, and a reproducible measurement protocol. M03-PR03d records the
owner-accepted standalone Linux x86_64 measurements and satisfies only that
tooling measurement condition. It does not satisfy native integration. Before
implementation, M03-PR03e now defines the executable successor protocol, but its
complete [native execution-evidence matrix](m03-pr03e-native-execution-evidence-plan.md)
must still be implemented and proved:

- fixed buffer, index, and temporary-workspace limits;
- an exact peak resident-memory bound for writer rotation and open;
- writer-delay and open-latency measurements at relevant bounds;
- deterministic cleanup and pressure custody at every publication phase;
- complete hostile parsing and source-link validation without semantic weakening;
- fault injection at every write, sync, readback, rename, publication, adoption,
  predecessor active/checkpoint removal, staging cleanup, intent-last removal,
  and directory-sync boundary; and
- explicit owner approval of the measured resource and latency budget.

The prototype memory target is 160 MiB. M03-PR03d's accepted standalone values are
not writer delay, rotation latency, eager-open latency, or SLOs. Writer-delay,
open-latency, native RSS, and total-runtime budgets/SLOs remain `UNKNOWN`.
M03-PR03e authorizes only later private-harness review; that harness, measured
Linux native results, and a fresh owner checkpoint remain mandatory. This
contract makes no publication benchmark claim. See the
[M03-PR03c resource plan](m03-pr03c-segment-resource-plan.md),
[M03-PR03d evidence record](m03-pr03d-linux-resource-evidence.md), and
[M03-PR03e execution-evidence plan](m03-pr03e-native-execution-evidence-plan.md).

## Future acceptance evidence matrix

This documentation PR claims none of the implementation evidence below. A future
implementation is incomplete until each row has independent, deterministic proof.

| Future requirement | Required evidence | Docs-PR status |
| --- | --- | --- |
| Marker, intent, catalog, and manifest primitives | Independent primitive-only byte oracles for every field, reserved range, endian rule, checksum, exact length, and hostile variant | `UNSATISFIED` |
| Published segment bytes | Independent full-segment oracle proving exact unchanged `OCHSEG01` bytes, source reconstruction, indexes, and trailer | `UNSATISFIED` |
| Namespace and inventory | Exact recognized-name oracle, 156-entry maximum, unknown-name refusal, and no orphan/gap/alternate segment | `UNSATISFIED` |
| Epoch fence | V1, V2, markerless, historical, and mixed-format refusal before lock creation/acquisition or mutation, with before/after equality | `UNSATISFIED` |
| Transaction convergence | Fault injection at every publication phase proving exact prior-root rollback only before commit and exact committed-root adoption only after commit | `UNSATISFIED` |
| Committed cleanup convergence | Inject failure before, between, and after predecessor active deletion, predecessor checkpoint deletion, each fixed staging-name deletion, intent-last removal, and each directory sync; prove no retained raw/segment deletion, no postcommit prior-root fallback, no extra inventory, and eventual exact committed-root reopen | `UNSATISFIED` |
| Pressure and receipts | Storage-pressure custody, no false manifest/catalog/segment commit, and no false durable receipt | `UNSATISFIED` |
| Committed fail-closed behavior | Missing, corrupt, foreign, malformed, truncated, excessive, and catalog-mismatched segment refusal with exact inventory equality | `UNSATISFIED` |
| Raw/segment linkage | Full checksum/hostile parse proving exact StoreId, generation, range, registry, raw length/CRC, frame coverage, and catalog identity | `UNSATISFIED` |
| Bounds | Catalog entries 1 and 64 succeed, entry 65 refuses; inventory 156 is accepted only in a valid state and 157 refuses | `UNSATISFIED` |
| Streaming resources | Exact peak resident-memory bound plus writer-delay/open-latency evidence for minimum, representative, and maximum legal inputs | `UNSATISFIED`; PR03d is standalone comparison data only |

M03-PR03e maps these 11 rows one-for-one to literal future harness and report
obligations. Plan acceptance does not satisfy a row.

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
