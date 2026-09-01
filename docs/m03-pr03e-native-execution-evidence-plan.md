# M03-PR03e Native V2 execution-evidence plan

## Status, authority, and evidence vocabulary

This document is the normative, executable evidence plan for the private Native
V2 harness and a later evidence report bundle. M03-PR03e itself was a
**review barrier only** and added no harness, product-reachable code, accepted V2 artifact, measured
result, numeric SLO or budget, opener, decoder, publication path, or Store Format
V2 authority. Store Format V1 remains the only implemented and accepted format.

Plan acceptance first authorized a separate bounded prerequisite review, now
delivered by M03-PR03f as disabled-by-default, rustdoc-hidden current-V1 native
instrumentation. It is not the private harness and binds no V2 source site.
M03-PR03g1 supplies only the separately reviewed capability-contained disposable
V2 executor foundation consuming that seam. Its opaque child capability and
compiled literal registry are finite current-revision Rust privacy/source-review
evidence, not a universal or future-source filesystem-I/O proof. It does not
authorize collection.
M03-PR03g2 must separately complete collection/report/supervisor infrastructure
before any collection authorization. Every row in the
[M03-PR03b future evidence matrix](#pr03b-evidence-crosswalk)
remains `UNSATISFIED` until the complete acceptance-candidate measurements and
fault results exist and the evidence returns through the authority progression
defined below. A structural harness `PASS` never satisfies a row. M03-PR03d is comparison evidence for a standalone tooling
prototype only; it establishes no native budget or result.

This plan uses these terms strictly:

- **plan obligation** is a requirement on the later harness or its report;
- **observation** is a complete reported sample, never an SLO or budget;
- **acceptance-candidate** is Linux x86_64 evidence eligible for later owner
  review, not evidence accepted by this plan;
- **UNKNOWN** is mandatory when a fact was not genuinely controlled or measured;
- **root classification** is exactly `PRIOR_ROOT`, `COMMITTED_ROOT`, or
  `UNCHANGED_REFUSAL`; and
- **UNSATISFIED** means no implementation or result is claimed by this docs PR.

The reviewed byte, name, and authority contracts remain exclusively in
[Store Format V2](store-format-v2-contract.md),
[Manifest V2](manifest-v2-contract.md),
[Generation Catalog V2](generation-catalog-v2-contract.md), and
[Published Native Segment V1](published-native-segment-v1-contract.md). This plan
must not be used to reinterpret or weaken them.

## Current seams and ordering that a future harness must observe

The later harness must instrument the future implementation at the current
ownership seams rather than introducing another writer or receipt authority:

- `crates/och-runtime/src/store_worker.rs` owns `run_store_worker`,
  `flush_pending`, and `rotation_required` on the dedicated blocking writer;
- `crates/och-runtime/src/ingress.rs` owns
  `IngressShared::complete_durable_batch` and the distinct handled and durable
  receipt stages;
- `crates/och-store/src/manifest.rs` owns
  `ManifestStore::{open, open_committed, sync_pending, requires_rotation, rotate,
  prepare_rotation, complete_rotation}` plus current intent, publication, and
  cleanup helpers;
- `crates/och-store/src/active.rs` owns
  `ActiveJournal::{open, append, sync_pending, pending_durable_cutoff}`; and
- `crates/och-store/src/pressure.rs` owns the standard-library pressure
  classification and sticky `StoreWriteState::ReopenRequired` custody.

Current ordinary durability is not part of V2 rotation and must remain first:

```text
journal sync
  -> checkpoint write and sync
  -> Retry State V1 snapshot publication
  -> Manifest commit covering that retry snapshot and journal cutoff
  -> runtime inspection update
  -> durable receipt resolution
  -> only then may automatic V2 rotation begin
```

`ActiveJournal::sync_pending` owns the journal-then-checkpoint mechanical cutoff.
`ManifestStore::sync_pending` prepares the complete retry/manifest relation before
that transaction, publishes Retry State V1 after the mechanical cutoff, and then
publishes the ordinary manifest. `flush_pending` updates inspection before
`IngressShared::complete_durable_batch` resolves the covered durable stages.

M03-PR03f now exposes only closed current-V1 observations/faults at those actual
seams under `m03-pr03e-native-harness`. Its registry deliberately contains no
`V2TX-*`, `V2TIME-P*`, or `V2IO-*` source-bound identity because no native V2
transaction exists. The later harness and any still-future product implementation
must add their separately reviewed complete V2 registry rather than relabeling
current-V1 events.

A receipt truthfully durable under that prior ordinary manifest remains durable
if the later rotation fails, requires reopen, or crashes. Rotation may neither
delay that already-earned truth nor cite an intent, staging artifact, uncommitted
raw seal, uncommitted segment, uncommitted catalog, or uncommitted Manifest V2 as
receipt evidence. No timing instrumentation may move or combine these transitions.

## Private harness boundary

The later harness must be private/test-only and unreachable from any product API.
It owns one out-of-band private evidence parent, with case descriptors, disposable
markers, process-control files, and reports outside every store-under-test child.
For each case, the store-under-test child path must be newly absent and then
exclusively created by the harness. From that creation onward, the child contains
only reviewed V2 inventory appropriate to the exact tested state; no private
marker, descriptor, report, or control artifact may enter that inventory.

Before creating a child, root/ancestor safety must refuse when the selected
evidence parent, child location, or any existing ancestor has real V1, real V2,
or mixed store authority inventory at that directory level. An out-of-band
disposable descriptor authorizes only the private parent/case relationship and
cannot override that refusal or become a store artifact.

The harness may reuse exact primitive encoders, decoders, parsers, streaming laws,
and independent oracles. Duplicated harness transaction logic is test machinery;
it cannot count as production implementation proof. The harness must expose no
public opener, decoder, migration, compatibility promise, durable publication
authority, runtime path, or supported command. V2 fixture bytes are not current
store bytes and must never be placed in or beside a real V1 store.

Only completed M03-PR03g2 harness review may authorize measured evidence
collection. The g1 executor foundation does not. A later product
implementation must run this complete matrix against the actual production call
path; a harness-only pass cannot be carried forward as product proof.

## Future V2 transaction state machine

The harness schema and future implementation instrumentation must use these
literal phase IDs and transitions. A phase succeeds only after every listed
boundary and relation has completed. `V2TX-PRIOR` is the selected prior Manifest
V2 root; `V2TX-COMMITTED` is the new selected root; `V2TX-REFUSED` is a sanitized
unchanged refusal.

| Step | Phase ID | Required transition and successful postcondition |
| ---: | --- | --- |
| 1 | `V2TX-P0-PREFLIGHT` | From `V2TX-PRIOR`, perform read-only format, inventory, root, active-range, checkpoint, registry-coverage, catalog-capacity, namespace, arithmetic, resource, and free-space/headroom preflight. Prepare every knowable exact intent, raw/segment identity, successor, Catalog V2, and Manifest V2 relation before mutation. Success remains `V2TX-PRIOR`; no byte or inventory mutation is allowed. |
| 2 | `V2TX-P1-INTENT` | Exclusively create-new, write, synchronize, completely bounded-read back, exact-decode, and directory-synchronize the single fixed non-authorizing `journal-rotation-v2.intent`. There is no intent staging or rename boundary. Success is `V2TX-INTENT`; the prior root remains sole authority. |
| 3 | `V2TX-P2-RAW` | Open and stream the exact fully durable active Journal V1 into the exclusive raw staging file; synchronize, completely read back, and validate framing, declarations, range, length, and checksum; rename to the canonical sealed-Journal final; synchronize the directory; then completely read back the final. Success is `V2TX-RAW`; neither final nor staging is authority. |
| 4 | `V2TX-P3-SEGMENT` | Stream/multipass-publish the exact unchanged `OCHSEG01` bytes to exclusive segment staging; synchronize; completely hostile-parse and validate the exact source link and source boundary sequence; rename to the canonical segment final; synchronize the directory; then completely read back and revalidate the final. Success is `V2TX-SEGMENT`; the segment remains non-authorizing. |
| 5 | `V2TX-P4-SUCCESSOR` | Create the exact empty successor active Journal V1 and checkpoint at the prior global sequence cutoff; write, synchronize, completely read back, and validate both and their relation. Success is `V2TX-SUCCESSOR`; the successor is not yet adopted. |
| 6 | `V2TX-P5-CATALOG` | Exclusively stage, write, synchronize, completely bounded-read back, exact-decode, and rename the next Catalog V2 candidate; synchronize the directory; read back the final and validate its complete raw/segment/successor relation. Success is `V2TX-CATALOG`; the candidate is not authority. |
| 7 | `V2TX-P6-MANIFEST` | Exclusively stage, write, synchronize, completely bounded-read back, exact-decode, and rename Manifest V2 over only the unreferenced alternate slot; synchronize the directory and validate the complete Manifest/Catalog/raw/segment/successor relation. The Manifest V2 final publication is the sole authority-changing commit boundary; its required directory sync and relation validation must finish before phase success is reported. Success is `V2TX-COMMITTED`. |
| 8 | `V2TX-P7-ADOPT-CLEAN` | Only from the exact validated `V2TX-COMMITTED` root, adopt the successor and perform the committed cleanup prefix below. Success is the adopted, exact clean `V2TX-COMMITTED` inventory. |

The exact committed cleanup prefix for `V2TX-P7-ADOPT-CLEAN` is:

1. adopt the fully validated successor in memory;
2. remove the intent/root-proven predecessor active Journal V1, then synchronize
   the directory;
3. remove its exact predecessor checkpoint, then synchronize the directory;
4. for each present exact-matching staging artifact, remove and synchronize in
   this fixed order: raw, segment, catalog, manifest;
5. read and prove the exact clean committed inventory while the intent remains;
6. remove the intent last; and
7. perform the final directory synchronization.

Every removal accepts absence only when the committed root, still-present intent,
and exact completed cleanup prefix prove an earlier successful removal. A retained
committed raw final or segment final is never a cleanup or rollback target. Before
commit, an intent-proven would-be final remains non-authoritative transaction
evidence and may be removed only by exact rollback to restore the prior inventory;
it is not a retained final. Current V1 `cleanup_committed_rotation` ordering,
which groups removals before one directory sync and removes the intent before
staging, is explicitly ineligible for V2 reuse where it conflicts with this
contract.

### Commit-side convergence law

Before the Manifest V2 final publication boundary, only exact intent-proven
rollback to `V2TX-PRIOR` or `V2TX-REFUSED` with byte-for-byte unchanged inventory
is legal. Rollback retains the intent while it validates and removes only exact
intent derivatives, synchronizes every required removal, proves the exact prior
inventory, removes the intent last, and performs a final directory sync. It never
adopts a staged or final raw, segment, catalog, or successor candidate.

At and after the Manifest V2 final publication boundary, prior-root fallback is
forbidden. Reopen must either validate `V2TX-COMMITTED`, adopt it, and resume only
the exact committed cleanup prefix, or return `V2TX-REFUSED` unchanged. The
in-process phase-complete marker occurs only after directory sync and complete
relation validation, but a fault or crash after successful Manifest rename is
classified on the committed side and may never assume prior-root rollback.

Intent-absent predecessor or staging leftovers refuse unchanged. Missing,
foreign, corrupt, malformed, partial, excessive, forked, unrelated, ambiguous, or
mismatched evidence refuses unchanged. No path may guess from names, use raw as a
segment fallback, rebuild, repair, migrate, or delete committed raw/segment
finals.

## Machine-readable timing events

All event records use one process-local monotonic clock based on
`std::time::Instant`. `start_ns` and `stop_ns` are unsigned nanoseconds relative
to one process-local monotonic origin; `elapsed_ns` must exactly equal their
checked difference. Wall-clock time, filesystem timestamps, and timestamps from
different processes are never subtracted. Each event row records `event_id`,
`parent_event_id`, `phase_id`, `case_id`, `sample_id`, `process_mode`,
`store_mode`, `rotation_trigger_path`, `durability_batch_id`, `sample_class`,
`sample_outcome`, joined `fault_id`, joined `fault_mode`, joined `pressure_kind`,
`start_ns`, `stop_ns`, `elapsed_ns`, event `outcome`, `trace_status`,
`distribution_eligible`, and optional bounded `pair_ordinal`.

### Closed sample classification and eligibility

`sample_class` is exactly one of `SUCCESS`, `FAULT`, `PRESSURE`, or
`NON_SUCCESS`. `sample_outcome` is exactly one of `SUCCESS`, `REFUSED`,
`TERMINAL`, `REOPEN_REQUIRED`, `ERROR`, `CRASHED`, or `OTHER_NON_SUCCESS`.
Every emitted event `outcome` uses that same closed value set; empty, unknown,
free-text, or any other event outcome is invalid. An event not reached is omitted
under the exact incomplete trace rather than emitted with an invented outcome.
`pressure_kind` is `NONE`, `StorageFull`, or `QuotaExceeded`; `fault_id` and
`fault_mode` are either `NONE` or exact closed fault-registry values. A present
fault ID/mode pair must join the same `sample_id` to its exact fault-result row;
pressure evidence may also name its injected boundary, but pressure precedence
still applies. Missing, partial, conflicting, or unmatched joined evidence fails
schema validation.

Classification uses this exact first-match precedence so a sample cannot fit two
classes:

1. `PRESSURE` when `pressure_kind != NONE`;
2. otherwise `FAULT` when `fault_id != NONE` or `fault_mode != NONE`;
3. otherwise `NON_SUCCESS` when `sample_outcome != SUCCESS`, when
   any emitted event `outcome != SUCCESS`, when the intended operation or process
   result is non-success, when `trace_status != COMPLETE_SUCCESS`, or when the
   exact required successful event tree/count is incomplete; and
4. only the remaining complete successful sample is `SUCCESS`.

`sample_outcome=SUCCESS` is valid only when the intended operation and process
completed successfully and every required emitted event in the complete tree has
`outcome=SUCCESS`. Otherwise `sample_outcome` is deterministic by this first-match
result precedence: `CRASHED` for process crash, `REOPEN_REQUIRED` for the sticky
typed reopen result, `TERMINAL` for another terminal result, `REFUSED` for a typed
refusal, `ERROR` for an emitted event/process error, and `OTHER_NON_SUCCESS` for
the remaining closed operation non-success. Existing `PRESSURE` then `FAULT` then
`NON_SUCCESS` class precedence still applies independently.

`trace_status` is exactly `COMPLETE_SUCCESS`, `COMPLETE_NON_SUCCESS`,
`INCOMPLETE_FAULT_WITNESS`, or `INCOMPLETE_NON_SUCCESS_WITNESS`.
`COMPLETE_SUCCESS` is valid only for `sample_class=SUCCESS`; a complete-shaped
fault, pressure, refusal, terminal/reopen result, or other non-success uses
`COMPLETE_NON_SUCCESS` and remains ineligible.

The eligibility equivalence is closed:

```text
distribution_eligible=true
iff sample_class=SUCCESS
and sample_outcome=SUCCESS
and fault_id=NONE
and fault_mode=NONE
and pressure_kind=NONE
and trace_status=COMPLETE_SUCCESS
and intended operation/process result=SUCCESS
and every required emitted event outcome=SUCCESS
and the exact required event tree and event counts are complete.
```

Every event row for one `sample_id` repeats these sample fields exactly or joins
deterministically to one bounded sample record carrying them; disagreement among
rows or joined reports fails. Every fault injection, abrupt crash witness,
pressure overlay, typed refusal, terminal/reopen result, incomplete trace, and
other non-success has `distribution_eligible=false`, even when the final
registered boundary succeeded and the event tree appears complete. The validator
rejects `sample_outcome=SUCCESS`, `sample_class=SUCCESS`, or
`distribution_eligible=true` whenever any emitted event, process result, or
intended operation result is non-success.

`rotation_trigger_path` is a closed field with exactly these values:

- `PRE_APPEND`: age or fit demand is discovered before the preserved incoming
  append;
- `POST_PUBLICATION`: rotation demand is discovered after append publication; or
- `NOT_APPLICABLE`: the sample is not an automatic writer-rotation sample.

Each automatic-rotation case declares exactly one closed
`expected_rotation_trigger_path`, either `PRE_APPEND` or `POST_PUBLICATION`.
Every sample/event row for that case must carry the same `rotation_trigger_path`;
an automatic-rotation case may never declare or report `NOT_APPLICABLE`. Required
sample and event counts are enforced independently for each declared case/path,
not pooled across paths or demand classes.

The complete matrix has this minimum closed automatic-rotation case set, matching
the demand classes reachable at each writer-owned source path:

| Required case ID | Demand class | `expected_rotation_trigger_path` |
| --- | --- | --- |
| `ROTATE-PRE-APPEND-FIT` | incoming append does not fit the current active generation | `PRE_APPEND` |
| `ROTATE-PRE-APPEND-AGE` | age demand discovered before the preserved incoming append | `PRE_APPEND` |
| `ROTATE-POST-PUBLICATION-SIZE` | size boundary reached after publication | `POST_PUBLICATION` |
| `ROTATE-POST-PUBLICATION-COUNT` | record-count boundary reached after publication | `POST_PUBLICATION` |
| `ROTATE-POST-PUBLICATION-AGE` | age boundary reached after publication | `POST_PUBLICATION` |

The schema/source validator must prove this case list covers every reachable
automatic-rotation demand class at the path where it actually occurs. It must not
manufacture a post-publication counterpart for a fit-only pre-append case.
`NOT_APPLICABLE` remains valid only for nonrotation ordinary-durability and
eager-open cases and cannot hide a rotation case. The complete matrix must still
contain both writer paths; missing either path fails the existing missing-path
fixtures.

### Writer-owned trigger paths

| Trigger path | Required pre-rotation barrier | Rotation-delay start | Rotation-delay stop |
| --- | --- | --- | --- |
| `PRE_APPEND` | Age or fit demand is discovered while the incoming append remains preserved. Any prior pending ordinary flush, inspection publication, and all covered durable receipt resolution complete first. If there is no pending ordinary work, `V2TIME-ORD-NOOP-BARRIER` records that explicit no-op; no receipt is invented. | Immediately after the flush/receipt sequence or explicit no-op barrier, and immediately before `V2TX-P0-PREFLIGHT`. | Immediately before the preserved incoming append resumes, or when its exact terminal/reopen-required result is fixed. |
| `POST_PUBLICATION` | Demand is discovered after publication. The forced ordinary flush, inspection publication, and every covered durable receipt resolution complete first, leaving pending empty. | Immediately after the last covered receipt resolves and immediately before `V2TX-P0-PREFLIGHT`. | Immediately before the writer accepts or receives the next ordered command, or when the exact terminal/reopen-required result is fixed. |

Queue/admission time before either start is excluded. A rotation event that starts
before its applicable ordinary receipt sequence or no-op barrier is invalid.

The top-level event IDs and exact boundaries are:

| Event ID | Start | Stop |
| --- | --- | --- |
| `V2TIME-RECEIPT-HANDLED-DURABLE` | The handled stage becomes visible for an append. | Its durable stage becomes visible under the ordinary manifest. A terminal resolution is an incomplete latency sample recorded in fault results, not a durable stop. This is ordinary durability and is never a rotation duration. |
| `V2TIME-WRITER-ROTATION-DELAY` | The trigger-specific barrier above has completed and the sole writer commits to rotate, immediately before `V2TX-P0-PREFLIGHT`. | The exact trigger-specific resume/receive boundary above, or the exact terminal/reopen-required result. |
| `V2TIME-ROTATION-MUTATION-CRITICAL` | Immediately before the first `V2TX-P1-INTENT` mutation. | The final directory sync after intent-last cleanup, or immediately before an injected operation error returns with its precommit/postcommit classification. |
| `V2TIME-EAGER-OPEN` | Future V2 open entry, before format and inventory preflight. | A fully validated writable handle is returned, or the sanitized refusal is fixed before return. |

### Literal ordinary-durability subevents

| Event ID | Literal start | Literal stop |
| --- | --- | --- |
| `V2TIME-ORD-JOURNAL-SYNC` | Immediately before the ordinary active Journal V1 `sync_all`. | Immediately after success, or immediately before its exact error classification returns. |
| `V2TIME-ORD-CHECKPOINT-WRITE` | Immediately before writing the prepared alternate checkpoint slot. | Immediately after the complete write, or immediately before its exact error classification returns. |
| `V2TIME-ORD-CHECKPOINT-SYNC` | Immediately before checkpoint `sync_all`. | Immediately after success, or immediately before its exact error classification returns. |
| `V2TIME-ORD-CHECKPOINT-ADOPT` | Immediately before the synchronized checkpoint becomes the in-memory mechanical durable cutoff. | Immediately after the exact new cutoff is installed and inspectable. |
| `V2TIME-ORD-RETRY-PUBLISH` | Immediately before the first Retry State V1 staging mutation. | After its required artifact sync, bounded readback/validation, final publication, following directory sync, and final relation validation, or at the exact classified fault return. |
| `V2TIME-ORD-MANIFEST-PUBLICATION-PREPARE` | Immediately before the first ordinary manifest staging mutation. | After staging write/sync and bounded readback/decode, immediately before final rename; or at the exact precommit fault return. |
| `V2TIME-ORD-MANIFEST-RENAME-COMMIT` | Immediately before the ordinary manifest final rename. | Immediately after rename success or its error. Successful rename is the ordinary manifest authority-changing commit boundary. |
| `V2TIME-ORD-MANIFEST-POSTCOMMIT-VALIDATE` | Immediately after successful ordinary manifest rename. | After the following directory sync, final readback, and complete ordinary manifest/retry/checkpoint relation validation, or at the exact postcommit fault return. |
| `V2TIME-ORD-MANIFEST-ADOPT` | Immediately before installing the validated committed ordinary manifest in memory. | Immediately after manifest slots, current root, and retry projection name that exact commit. |
| `V2TIME-ORD-INSPECTION-UPDATE` | Immediately before publishing runtime inspection for the ordinary commit. | Immediately after the committed manifest is visible through inspection. |
| `V2TIME-ORD-RECEIPT-RESOLVE` | Immediately before entering the atomic covered durable-batch transition. | Immediately after every covered durable stage is visible and waiters may wake, or after the exact terminal resolution is fixed. |
| `V2TIME-ORD-NOOP-BARRIER` | On a `PRE_APPEND` demand with no pending ordinary work, immediately before checking that no flush/receipt work is required. | Immediately after pending-empty and the existing ordinary committed root are proven unchanged, before V2 preflight. |

An ordinary flush uses the exact peer order shown above from journal sync through
receipt resolution. The no-op barrier replaces that complete sequence only for a
no-pending `PRE_APPEND` path and cannot produce a synthetic handled or durable
receipt.

### Literal V2 transaction and eager-open subevents

The transaction phase IDs remain exactly `V2TX-P0-PREFLIGHT` through
`V2TX-P7-ADOPT-CLEAN`; event IDs refine measurement only.

| Event ID | Phase | Literal start | Literal stop |
| --- | --- | --- | --- |
| `V2TIME-P0-PREFLIGHT` | `V2TX-P0-PREFLIGHT` | Immediately before the first read-only V2 format/inventory/root preflight operation. | After all preflight relationships and exact transaction candidates are prepared, immediately before the first P1 intent mutation, or at the sanitized refusal. |
| `V2TIME-P1-INTENT` | `V2TX-P1-INTENT` | Immediately before exclusive create-new of `journal-rotation-v2.intent`. | After intent file sync, complete bounded readback/decode, and following directory sync, or at the exact classified fault return. |
| `V2TIME-P2-RAW` | `V2TX-P2-RAW` | Immediately before opening the durable active Journal V1 source. | After raw-final directory sync and complete final readback/validation, or at the exact classified fault return. |
| `V2TIME-P3-SEGMENT` | `V2TX-P3-SEGMENT` | Immediately before the first raw-source read or segment-staging create used by exact `OCHSEG01` streaming publication, whichever occurs first. | After segment-final directory sync and complete hostile final/source-link validation, or at the exact classified fault return. |
| `V2TIME-P4-SUCCESSOR` | `V2TX-P4-SUCCESSOR` | Immediately before the first successor active Journal V1 or checkpoint create. | After both successor artifacts are synchronized, completely read back, and relation-validated, or at the exact classified fault return. |
| `V2TIME-P5-CATALOG` | `V2TX-P5-CATALOG` | Immediately before Catalog V2 staging create. | After final rename, following directory sync, complete final readback/decode, and raw/segment/successor relation validation, or at the exact classified fault return. |
| `V2TIME-P6-MANIFEST` | `V2TX-P6-MANIFEST` | Immediately before Manifest V2 staging create. | After `V2TIME-P6-MANIFEST-POSTCOMMIT-VALIDATE` completes, or at the exact classified precommit/postcommit fault return. This aggregate contains only the next three declared child spans. |
| `V2TIME-P6-MANIFEST-PUBLICATION-PREPARE` | `V2TX-P6-MANIFEST` | Immediately before Manifest V2 staging create. | After staging write/sync and complete bounded readback/decode, immediately before final rename; or at the exact precommit fault return. |
| `V2TIME-P6-MANIFEST-RENAME-COMMIT` | `V2TX-P6-MANIFEST` | Immediately before the Manifest V2 final rename. | Immediately after rename success or its error. Successful rename is the sole V2 authority-changing commit boundary and switches root classification to `COMMITTED_ROOT` immediately. |
| `V2TIME-P6-MANIFEST-POSTCOMMIT-VALIDATE` | `V2TX-P6-MANIFEST` | Immediately after successful Manifest V2 final rename. | After the following directory sync, complete final readback, and complete Manifest/Catalog/raw/segment/successor relation validation, or at the exact postcommit fault return. |
| `V2TIME-P7-ADOPTION` | `V2TX-P7-ADOPT-CLEAN` | Immediately before in-memory adoption of the fully postcommit-validated successor and committed root. | Immediately after current root, catalog, active successor, and inspection state name the exact committed relation. |
| `V2TIME-P7-CLEANUP` | `V2TX-P7-ADOPT-CLEAN` | Immediately before the first committed predecessor-active cleanup boundary. | Immediately after intent-last removal and its final directory sync, with the exact clean committed inventory proven; or at the exact postcommit fault return. |
| `V2TIME-OPEN-PAIR-VALIDATION` | eager open | Immediately before opening the raw final for `pair_ordinal`. | After complete raw and segment validation and source-link comparison, pair-state release, and the pair resource-ledger check, before the next pair opens or the refusal is fixed. |

`V2TIME-OPEN-PAIR-VALIDATION` has one row per pair with
`pair_ordinal=1..=64`; aggregate `V2TIME-EAGER-OPEN` never replaces those rows.

### Closed event-order and containment validator

`parent_event_id=NONE` is the one canonical root sentinel. Empty, null, omitted,
or unknown parent values are invalid. The event-node key is
`(sample_id, event_id, pair_ordinal)`; non-pair events use `pair_ordinal=NONE`.
Every non-root node has exactly one direct parent node in the same sample.

The only legal direct-parent edges are:

| Direct parent | Exact direct children |
| --- | --- |
| `NONE` | root aggregates `V2TIME-WRITER-ROTATION-DELAY`, `V2TIME-RECEIPT-HANDLED-DURABLE`, and `V2TIME-EAGER-OPEN`; separate root event `V2TIME-ORD-NOOP-BARRIER` |
| `V2TIME-WRITER-ROTATION-DELAY` | `V2TIME-P0-PREFLIGHT`, `V2TIME-ROTATION-MUTATION-CRITICAL` |
| `V2TIME-ROTATION-MUTATION-CRITICAL` | `V2TIME-P1-INTENT`, `V2TIME-P2-RAW`, `V2TIME-P3-SEGMENT`, `V2TIME-P4-SUCCESSOR`, `V2TIME-P5-CATALOG`, aggregate `V2TIME-P6-MANIFEST`, `V2TIME-P7-ADOPTION`, `V2TIME-P7-CLEANUP` |
| `V2TIME-P6-MANIFEST` | `V2TIME-P6-MANIFEST-PUBLICATION-PREPARE`, `V2TIME-P6-MANIFEST-RENAME-COMMIT`, `V2TIME-P6-MANIFEST-POSTCOMMIT-VALIDATE` |
| `V2TIME-RECEIPT-HANDLED-DURABLE` | `V2TIME-ORD-JOURNAL-SYNC`, `V2TIME-ORD-CHECKPOINT-WRITE`, `V2TIME-ORD-CHECKPOINT-SYNC`, `V2TIME-ORD-CHECKPOINT-ADOPT`, `V2TIME-ORD-RETRY-PUBLISH`, `V2TIME-ORD-MANIFEST-PUBLICATION-PREPARE`, `V2TIME-ORD-MANIFEST-RENAME-COMMIT`, `V2TIME-ORD-MANIFEST-POSTCOMMIT-VALIDATE`, `V2TIME-ORD-MANIFEST-ADOPT`, `V2TIME-ORD-INSPECTION-UPDATE`, and `V2TIME-ORD-RECEIPT-RESOLVE`; never `V2TIME-ORD-NOOP-BARRIER` |
| `V2TIME-EAGER-OPEN` | every `V2TIME-OPEN-PAIR-VALIDATION` identified by its exact `pair_ordinal` |

Root sets are also closed. A receipt projection has only the receipt root; an
eager-open sample has only the eager-open root; a rotation sample with a pending
ordinary barrier has only the writer root and joins receipt projections by batch
ID; and a pending-empty `PRE_APPEND` rotation sample has the no-op root followed
by the writer root. Those two roots may touch at the boundary but may not overlap
or cross. No other multi-root sample is valid.

No other parent type or edge is legal. A descendant is transitively contained by
each ancestor but names only its one direct parent. Every child interval lies
fully within its direct parent interval, and therefore every descendant lies
within every ancestor. Sibling intervals may touch when one `stop_ns` exactly
equals the next `start_ns`; siblings may not overlap, cross, or contain one
another. Parent/child intervals may nest but may not cross.

The later plan/schema validator must enforce this closed forest of direct-parent
trees plus the existing trace grammar:

1. Reject cycles, duplicate parentage, duplicate event-node keys, unknown parents,
   wrong parent types, roots whose parent is not `NONE`, children whose parent is
   `NONE`, missing direct parents, and crossing intervals.
2. Peer subevents may not overlap, merge, reorder, or silently disappear.
   Complete traces follow the exact ordinary order and the exact
   P0→P1→P2→P3→P4→P5→P6 preparation→P6 rename→P6 postcommit→P7 adoption→P7
   cleanup order. The declared direct/transitive hierarchy is the only permitted
   containment.
3. Successful Manifest V2 final rename changes root classification immediately.
   Every later event or fault result is `COMMITTED_ROOT` or an unchanged refusal
   of that committed relation; `PRIOR_ROOT` after rename success is invalid even
   though directory sync and postcommit validation remain.
4. Rotation timing cannot begin before the applicable complete ordinary receipt
   sequence or separate no-op root. The trigger-specific stop must precede the
   preserved append resume (`PRE_APPEND`) or next command receive/accept
   (`POST_PUBLICATION`).
5. Every complete `sample_id` has exactly the required event set, declared case
   path, direct parents, transitive containment, order, and pair coverage.
   Required eligible-success sample/event counts are enforced per `case_id` and
   `expected_rotation_trigger_path`. A fault/non-success may retain an exact
   completed tree or an exact prefix under its closed `trace_status`, but is always
   `distribution_eligible=false`.
6. Each automatic-rotation case declares exactly one expected path and every row
   must match it. Matrix validation requires all five closed demand/path cases and
   therefore both writer paths overall. It refuses `NOT_APPLICABLE` on rotation,
   an unrecognized value, a case/path mismatch, pooled counts, or implicit trigger
   coverage. Nonrotation ordinary/eager-open cases may use `NOT_APPLICABLE`.

`durability_batch_id` is either `NONE` or one nonzero bounded unsigned 64-bit
identifier. A receipt-latency sample carries the physical ordinary batch ID; a
rotation sample with a pending ordinary barrier joins that same ID, while a
pending-empty no-op path uses `NONE`. If one physical ordinary batch covers
multiple receipts, each covered receipt has its own `sample_id` projection and
its own receipt root/direct-child tree. The same observed ordinary timestamps may
repeat across those projections and their shared `durability_batch_id`, but no
event node is shared across samples or given multiple parents. The rotation tree
joins the receipt projections by batch ID; it never parents their ordinary event
nodes. Its preflight/rotation start must be at or after every joined covered
receipt root has stopped.

The future plan/schema validator must include these literal fixtures:

| Fixture ID | Required result |
| --- | --- |
| `TRACE-ACCEPT-PRE-APPEND` | Accept one exact complete required `PRE_APPEND` rotation tree with either joined covered receipt projection(s) by batch ID or the explicit no-op root before P0. |
| `TRACE-ACCEPT-POST-PUBLICATION` | Accept one exact complete required `POST_PUBLICATION` rotation tree joined to its forced ordinary-durability receipt projection(s), all stopped before P0. |
| `TRACE-REJECT-MERGED-SUBEVENT` | Reject two required peer spans represented as one event. |
| `TRACE-REJECT-OVERLAPPING-PEERS` | Reject peer overlap not explained by declared parent containment. |
| `TRACE-REJECT-REORDERED-SUBEVENT` | Reject any ordinary or P0→P7 reorder. |
| `TRACE-REJECT-MISSING-SUBEVENT` | Reject a complete sample missing any required literal span. |
| `TRACE-REJECT-PRE-RECEIPT-ROTATION` | Reject `POST_PUBLICATION` P0/rotation start before all covered durable receipt resolution. |
| `TRACE-REJECT-PRE-BARRIER-ROTATION` | Reject no-pending `PRE_APPEND` P0/rotation start before the explicit no-op barrier. |
| `TRACE-REJECT-MISSING-PRE-APPEND` | Reject an applicable matrix with no complete `PRE_APPEND` sample. |
| `TRACE-REJECT-MISSING-POST-PUBLICATION` | Reject an applicable matrix with no complete `POST_PUBLICATION` sample. |
| `TRACE-REJECT-CASE-TRIGGER-MISMATCH` | Reject a sample whose trigger differs from its case's one declared `expected_rotation_trigger_path`, including `NOT_APPLICABLE` on rotation. |
| `TRACE-REJECT-POST-RENAME-PRIOR-ROOT` | Reject `PRIOR_ROOT` classification after successful `V2TIME-P6-MANIFEST-RENAME-COMMIT`. |
| `ELIGIBILITY-ACCEPT-COMPLETE-SUCCESS` | Accept exact complete tree/count, success outcome, absent fault/pressure evidence, `sample_class=SUCCESS`, `trace_status=COMPLETE_SUCCESS`, and `distribution_eligible=true`. |
| `ELIGIBILITY-REJECT-FINAL-FAULT-ELIGIBLE` | Reject a complete-shaped sample marked eligible when a final-boundary injected fault ID/mode exists, even if that boundary event reports success. |
| `ELIGIBILITY-REJECT-PRESSURE-ELIGIBLE` | Reject any `StorageFull` or `QuotaExceeded` sample marked eligible. |
| `ELIGIBILITY-REJECT-NON-SUCCESS-ELIGIBLE` | Reject a refusal, terminal/reopen result, crash, error, incomplete trace, or other non-success marked eligible. |
| `ELIGIBILITY-REJECT-NON-SUCCESS-EVENT-SUCCESS-SAMPLE` | Reject a complete event tree containing one non-success event when the sample is falsely labeled `sample_outcome=SUCCESS`, `sample_class=SUCCESS`, and `distribution_eligible=true`. |
| `ELIGIBILITY-REJECT-SUMMARY-INELIGIBLE` | Reject any timing summary that includes an ineligible sample or counts it toward a required success count. |
| `TREE-ACCEPT-DIRECT-NESTED` | Accept the exact direct-parent tree, including P6 grandchildren transitively contained by mutation-critical and writer ancestors. |
| `TREE-ACCEPT-SIBLING-BOUNDARY-TOUCH` | Accept ordered siblings where one stop equals the next start without overlap. |
| `TREE-REJECT-WRONG-PARENT` | Reject any event attached to a parent not listed in the direct-parent table. |
| `TREE-REJECT-CROSSING-PARENT-CHILD` | Reject a child interval extending outside or crossing its direct parent. |
| `TREE-REJECT-CROSSING-SIBLINGS` | Reject overlapping, crossing, or sibling-containing intervals. |
| `TREE-REJECT-CYCLE` | Reject every direct or transitive parent cycle. |
| `TREE-REJECT-UNKNOWN-PARENT` | Reject a non-`NONE` parent node absent from the same sample. |
| `TREE-REJECT-DUPLICATE-PARENTAGE` | Reject duplicate event-node keys or one node assigned more than one direct parent. |
| `TREE-REJECT-ROOT-SENTINEL` | Reject a root event whose `parent_event_id` is not `NONE`, including empty or unknown values. |
| `TREE-REJECT-CHILD-SENTINEL` | Reject a child event whose `parent_event_id=NONE`. |

A killed child cannot emit an in-process stop event. Crash-after-success cases
therefore record the child's last successful boundary event and the parent's
separate process-exit/reopen observations. They remain raw fault witnesses with
`sample_class=FAULT` and `distribution_eligible=false`, even when a complete
same-process start/stop pair or complete-shaped event tree exists. Parent and
child monotonic values are never combined.

Instrumentation overhead must be measured in a paired harness-only calibration
and reported, but samples are not adjusted or discarded. No duration in this plan
is an SLO.

## Sample, cache, and platform policy

### Timing tiers

| Tier | Mandatory samples | Statistics and claim boundary |
| --- | --- | --- |
| Tractable minimum and representative receipt/rotation/open cases | At least 30 independent cold/fresh-process samples per case/event and at least 100 warm same-process samples per case/event. | Preserve every sample. Report min, median, observed p90/p95/p99, max, IQR, and MAD. These are observations only. |
| Independent massive maxima, 64-pair eager-open, and 65th-entry refusal | Three independent witness runs per case. | Preserve and report every sample plus min/median/max. Explicitly report `percentile_claim=NONE`; three witnesses do not support a percentile claim. |
| Every fault-ID/mode combination, including each pressure-kind overlay | At least three deterministic repetitions. | All three must produce exactly the same expected convergence and receipt classification; any divergence fails the row. No timing percentile is claimed. |

For tractable tiers, observed p90/p95/p99 and quartiles use the nearest-rank rule
on the complete sorted sample multiset: rank `ceil(p*n)`, one-based. Median is the
same observed p50 rule, IQR is observed p75 minus observed p25, and MAD is the
observed p50 of all absolute deviations from that median. No warm-up, slow,
fast, scheduler-affected, or other outlier may be removed. Failed samples remain
in the fault/result report and cannot silently reduce the required successful
sample count. Every required timing count is the count of distinct
`distribution_eligible=true` successful `sample_id` values only; fault/pressure
repetitions and complete-shaped non-success witnesses never satisfy it.

Each sample independently labels:

- `process_cache=COLD` for a fresh process or `process_cache=WARM` for a later
  operation in the same retained process;
- `filesystem_cache=WARM`, `COLD`, or `UNKNOWN`;
- `store_mode=NEW` or `REUSED`; and
- exact case, fixture seed, operation, and sample ordinal.

Warm same-process timing keeps the process warm, not an implicitly mutated store.
Every warm repetition begins from an independently prepared equivalent
precondition and store fixture outside the timed interval, with its precondition
ID and inventory fingerprint reported. The preparation either exclusively creates
a new store-under-test child or follows a case's explicit `store_mode=REUSED`
contract and exact reusable-state fingerprint. A post-rotation, post-adoption, or
post-cleanup store cannot be silently recycled as the next equivalent sample.

Filesystem-cold is `UNKNOWN` unless it is genuinely controlled and documented by
an owner-approved environment mechanism. This plan authorizes no cache drop,
host mutation, reboot, privileged command, or cloud mutation. Process-cold does
not imply filesystem-cold.

The future acceptance-candidate target is the existing owner-approved dedicated
GCP Linux x86_64 AgentBox using a documented local filesystem. Hosted PR CI is
functional/structural only and must not publish acceptance timing. Darwin arm64
is compile, functional, report-sanitization, and exploratory timing only. Linux
arm64, Windows, network filesystems, cloud/object filesystems, and FUSE are
excluded unless separately approved. Physical power-loss durability is excluded;
real child-process crash and reopen after each successful registered boundary is
required.

## Closed semantic fault and pressure registry

The later harness must contain one closed, literal semantic fault registry. Every
actual future V2 filesystem or adoption boundary must call through an instrumented
boundary carrying exactly one registered `fault_id`. A schema/source validator
must fail on an unregistered call site, duplicate ID, registered-but-unreachable
ID, unknown report ID, missing expected successor boundary, or operation/phase
mismatch. Repeated streaming calls use one call-site ID plus a bounded
`occurrence_ordinal`; dynamically generated IDs and wildcard registry rows are
forbidden. For every observed transition, the validator must prove the actual
next ID is one of the exact state-specific allowed successors and must reject an
implicit or unregistered terminal transition.

The submitted registry has one literal row per boundary with these fields:

```text
fault_id phase_id artifact operation mutation short_write_allowed
pressure_allowed allowed_next_fault_ids allowed_terminal_states commit_side
expected_root_class
```

`allowed_next_fault_ids` is a closed bounded set, not a wildcard. It explicitly
enumerates branches for present versus absent optional cleanup artifacts and any
bounded streaming-loop self/exit transitions. Each loop also fixes its maximum
`occurrence_ordinal`. Every state-conditioned outcome must resolve to exactly one
listed successor or one explicit `allowed_terminal_states` value; a row may list
both kinds for distinct outcomes. A row with no successor must list an explicit
terminal, while a nonterminal outcome cannot use an empty successor set. Dynamic
IDs, inferred fallthrough, implicit terminals, and open-ended loops are invalid.

The following table is the closed minimum semantic coverage. The harness PR must
expand every listed family/operation cell into literal IDs and add any additional
I/O call site used by its implementation; omission fails structural acceptance.

| ID prefix / phase | Artifact families | Required literal operation rows |
| --- | --- | --- |
| `V2IO-P0-*` / `V2TX-P0-PREFLIGHT` | root inventory, selected manifest, active Journal V1, checkpoint, registry, retry, recovery, prior Catalog V2, each retained raw/segment pair | directory open/read, file open, metadata read, bounded read, complete validation, relation validation |
| `V2IO-P1-INTENT-*` / `V2TX-P1-INTENT` | single fixed exclusive `journal-rotation-v2.intent` | exclusive create/create-new, write and applicable partial write, file `sync_all`, bounded readback open/read and exact decode, following directory `sync_all`; the reviewed contract has no intent staging or rename boundary |
| `V2IO-P2-RAW-*` / `V2TX-P2-RAW` | active source, raw staging, raw final | source open/read, staging create/write/`sync_all`, readback open/read/full validation, rename, following directory `sync_all`, final open/read/full validation |
| `V2IO-P3-SEGMENT-*` / `V2TX-P3-SEGMENT` | raw source, segment staging, segment final | source open/read, staging create/write/`sync_all`, readback open/read/hostile full validation/source-link validation, rename, following directory `sync_all`, final open/read/full validation |
| `V2IO-P4-SUCCESSOR-*` / `V2TX-P4-SUCCESSOR` | successor active Journal V1 and checkpoint | each create, header/slot write, partial write, each `sync_all`, directory `sync_all`, readback open/read/full validation, relation validation |
| `V2IO-P5-CATALOG-*` / `V2TX-P5-CATALOG` | Catalog V2 staging and final | create, write, `sync_all`, bounded readback open/read/exact validation, rename, following directory `sync_all`, final open/read/full relation validation |
| `V2IO-P6-MANIFEST-*` / `V2TX-P6-MANIFEST` | Manifest V2 staging and alternate final | create, write, `sync_all`, bounded readback open/read/exact validation, rename/publication, following directory `sync_all`, final open/read/complete committed relation validation |
| `V2IO-P7-ADOPT-*` / `V2TX-P7-ADOPT-CLEAN` | successor and in-memory store authority | adoption, inspection publication, following state validation |
| `V2IO-P7-CLEAN-*` / `V2TX-P7-ADOPT-CLEAN` | predecessor active, predecessor checkpoint, raw staging, segment staging, catalog staging, manifest staging, intent | exact-match open/read/validation when present, each ordered remove, the directory `sync_all` immediately after each present removal, clean-inventory read/validation, intent-last remove, final directory `sync_all` |
| `V2IO-RB-*` / precommit rollback | every intent-proven uncommitted final/staging candidate and successor pair | exact-match open/read/validation, each ordered remove, every following directory `sync_all`, prior-inventory read/validation, intent-last remove, final directory `sync_all` |
| `V2IO-OPEN-*` / eager open | root, marker, stable lock, manifest pair, registry/retry/recovery, active/checkpoint, Catalog V2, intent/staging when present, all 64 raw/segment pairs | pre-lock directory open/read, each file open/metadata/read, bounded readback, complete hostile validation, lock create/open/acquire only after format fence, convergence removes and following directory syncs, final relation and writable-handle adoption |

`open`, `create`, `read`, bounded readback, complete validation, `write`, partial
write, `sync_all`, rename/publication, remove, adoption, and the directory sync
following every mutation are distinct boundaries. A high-level helper ID cannot
hide lower I/O calls. The registry validator must compare the closed registry with
actual instrumented source so a future implementation cannot add an uncovered
boundary.

Every applicable registered boundary must exercise these literal modes:

- `PRE_OPERATION_ERROR`: deterministic error before the operation changes state;
- `SHORT_PARTIAL_WRITE`: deterministic nonzero short write followed by error,
  only where a write can partially succeed; and
- `CHILD_CRASH_AFTER_SUCCESS`: after the operation succeeds, instrumentation
  records that boundary, publishes readiness, and blocks inside boundary finish
  before it can return. A parent-owned controller uses OS kill/abort or an
  equivalent abrupt immediate termination. The child performs no unwinding,
  destructors, `Drop` cleanup, exit handler, report flush, or other cleanup.

Crash control, observations, and reports are parent-owned and remain in the
out-of-band evidence parent, never in the store-under-test. Physical power loss
remains excluded; abrupt child-process death and subsequent parent-driven reopen
are mandatory.

Each store-owned mutation boundary additionally overlays both
`PRESSURE_STORAGE_FULL` (`std::io::ErrorKind::StorageFull`) and
`PRESSURE_QUOTA_EXCEEDED` (`std::io::ErrorKind::QuotaExceeded`). Pressure is
injected as a deterministic pre-operation error and, for writes, after a
deterministic short/partial write. Raw OS error numbers are diagnostics only and
never select pressure semantics.

### Required result for every fault case

Every mode/ID repetition records the selected prior root, exact pre-inventory
fingerprint, immediate post-process inventory fingerprint, process result and
last successful boundary, handled and durable receipt stages, reopen result,
post-reopen inventory fingerprint, final inventory fingerprint, first typed
pressure when applicable, and one exact root classification.

The first typed pressure wins. The live store becomes sticky
`ReopenRequired`, runtime health becomes fail-stop storage pressure, future work
stops, and unresolved receipt stages resolve without false durability. There is
no pressure retry or clear. A previously completed ordinary durable receipt
remains durable; no pending receipt may cite a false manifest, catalog, raw, or
segment commit. Reopen is the sole convergence path.

The hostile matrix must include missing, foreign, corrupt, malformed, truncated,
partial, excessive, forked, unrelated, ambiguous, unknown-name, mixed-format,
intent-absent-leftover, and raw/segment/catalog/Manifest relation-mismatch cases.
Every refusal exact-compares before/after inventory fingerprints and reports
`UNCHANGED_REFUSAL`. A fingerprint is SHA-256 over the canonical sorted sequence
of relative artifact name, file kind, logical length, and complete artifact
SHA-256. Reports contain only the aggregate and per-artifact hashes, never bytes.

## Bounds and resource ledger

The later structural and execution matrices must prove these exact boundaries:

- Catalog V2 entry 1 and entry 64 succeed; entry 65 refuses before the first
  mutation and leaves exact before/after equality.
- A canonical 156-entry inventory succeeds. Entry 157, an unknown name, a
  non-file, and any V1/V2 mixture refuse before lock creation/acquisition or
  mutation with exact equality.
- Eager open validates all 64 committed raw/segment pairs completely, one pair
  state at a time. Pair `n` state is released and ledger-checked before pair
  `n+1` opens.
- Every case measures and ledgers requested and actual logical and allocated
  external workspace, whether the observation is zero or nonzero.
- Incomplete, unbounded, arithmetic-overflowing, or unledgered workspace behavior
  fails the harness evidence. Every candidate implementation must expose finite,
  checked workspace bounds and complete evidence, but this plan does not choose
  their accepted numeric value or require zero.

Every operation reports a complete resource ledger with actual and requested
capacities for frame metadata, observation/index state, input frame, canonical
re-encode buffer, decoder state, re-encoder state, I/O scratch, transaction
records, receipt records, fault state, and pair state. It also records stack-size
assumptions, thread count, RSS source and units, logical and allocated artifact
storage, logical and allocated external workspace, maximum concurrent transaction
inventory, available storage and planned headroom, page size, and process/filesystem
cache labels. Requested capacity without actual allocator capacity, or aggregate
RSS without the representation ledger, is incomplete.

Planning 64 independently maximum-sized raw/segment pairs plus one maximal active
Journal V1 requires at least `75,728,169,472` logical bytes before
filesystem allocation granularity, reports, and headroom. One complete 64-pair
eager-validation sweep reads about `109,551,035,136` bytes under the reviewed
formula. Those massive cases belong on the dedicated AgentBox/release evidence
path, never hosted PR CI.

The PR03c 160 MiB prototype target, its zero-external-workspace design, and every
PR03d standalone value are tooling comparison data only. A nonzero native
workspace observation is not by itself a plan failure or `REPLAN`. Native peak
RSS, writer rotation delay, eager-open latency, total runtime, headroom, workspace
limit/acceptance threshold, and all SLOs/budgets remain `UNKNOWN` until measured
Linux evidence returns and the fresh owner checkpoint accepts them. This plan sets
no pass threshold for those observations.

## Bounded report bundle and schema

Reports are schema-versioned UTF-8 KV/TSV, never free-form logs. Schema version
`m03-pr03e-v1` requires this bounded bundle:

| Relative file | Required contents |
| --- | --- |
| `run.kv` | Plan acceptance SHA; clean harness SHA and measured source SHA; tracked and untracked tree status; `Cargo.lock`, harness-source, and instrumentation-source SHA-256; exact commands/profile; Rust/Cargo versions; platform, CPU, memory, filesystem, mount/locality, load, storage/headroom, page/cache/store facts; report classification and exclusions. |
| `timing-samples.tsv` | Every raw complete or incomplete event row with direct parent or root `NONE`, phase, case/sample and optional durability-batch IDs, closed trigger path, sample class/outcome, joined fault ID/mode and pressure kind, exact process/store/cache labels, monotonic start/stop/elapsed nanoseconds, event outcome, trace status, distribution eligibility, and pair ordinal. |
| `timing-summary.tsv` | Per-case/path required eligible-success count, actual distinct eligible-success count, declared/observed trigger path, min/median/observed p90/p95/p99/max/IQR/MAD or witness-only min/median/max plus `percentile_claim=NONE`; only `distribution_eligible=true` samples are aggregated, counts are never pooled, and every ineligible sample is excluded. |
| `resource-ledger.tsv` | Every actual/requested capacity and all process, stack, RSS, storage, requested/actual logical/allocated external workspace, concurrent-inventory, headroom, page, and cache fields listed above for every case. |
| `fault-registry.tsv` | The complete closed registry row for every instrumented boundary. |
| `fault-results.tsv` | Sample ID/class/outcome/eligibility, fault ID/mode, repetition, expected/actual result, pressure kind/raw diagnostic, receipt stages, durability-batch and rotation-trigger IDs, exact completed event prefix/tree, process result, last boundary, pre/immediate/reopen/final fingerprints, and root classification. |
| `matrix.tsv` | Every PR03b crosswalk ID; every required rotation `case_id`, demand class, and one `expected_rotation_trigger_path`; both writer paths overall; every eligibility, direct-parent/containment, and existing event-order fixture; and every timing, bound, hostile, fault/mode, pressure, platform, and report obligation with expected rows, observed rows, and `PASS`/`FAIL`; no skipped or implied row. |
| `SHA256SUMS` | Relative SHA-256 for every other bounded report file and no unlisted report file. |

All KV keys and TSV columns are closed and schema-validated. The bundle must cap
itself at 64 MiB, each data file at 16 MiB, each physical line at 4,096 bytes,
each scalar at 1,024 bytes, and row counts at the exact precomputed matrix count.
If the complete required matrix cannot fit, the harness returns `REPLAN`; it does
not truncate. Raw reports retain every complete and incomplete observation but no
canonical payload; summaries retain only eligible successful statistics.

Validation rejects a missing/unknown/duplicate ID, missing required field,
class/outcome/evidence/eligibility mismatch, illegal direct parent or interval,
incomplete matrix row, unsafe value, unknown schema version, dirty or mismatched
measured source, mismatched plan/harness/instrumentation hash, checksum mismatch,
unlisted file, absolute path, username, hostname, cloud project/instance ID,
credential, environment dump, canonical admission payload, raw journal, segment,
core dump, or unbounded log. Sanitized paths are report-root-relative only.

## PR03b evidence crosswalk

This table maps every row of the PR03b future evidence matrix to an executable
harness and report obligation. Status is deliberately uniform: this plan contains
neither the harness nor results.

| Crosswalk ID / PR03b requirement | Future harness obligation | Required report proof | Status |
| --- | --- | --- | --- |
| `PR03E-M01` Marker, intent, catalog, and manifest primitives | Implement independent primitive-only byte oracles for every reviewed field, zero/reserved range, endian rule, checksum, exact length, and hostile variant; byte-compare against future implementation only in disposable roots. | Oracle case IDs, source/oracle hashes, every positive/hostile result, and complete matrix count. | `UNSATISFIED` |
| `PR03E-M02` Published segment bytes | Independently emit and full-compare exact unchanged `OCHSEG01` bytes, complete source reconstruction, indexes, and trailer without using the production emitter as oracle. | Fixture dimensions, raw/segment identities, complete comparison and hostile parse rows, and oracle hashes. | `UNSATISFIED` |
| `PR03E-M03` Namespace and inventory | Enumerate the exact V2 recognized-name oracle; prove canonical 156, unknown-name refusal, and no orphan, gap, duplicate, alternate segment, non-file, or leftover. | Canonical sorted inventory fingerprints and before/after equality for every refusal. | `UNSATISFIED` |
| `PR03E-M04` Epoch fence | Exercise V1, V2, markerless, historical, and every mixed-format inventory before lock create/acquire or mutation. | Boundary trace proving pre-lock refusal plus exact pre/immediate/final inventory equality. | `UNSATISFIED` |
| `PR03E-M05` Transaction convergence | Exercise every registered phase boundary in all applicable modes with three identical repetitions; allow only exact prior-root rollback before commit and committed-root adoption after commit; validate that successful Manifest V2 rename switches classification immediately; require the closed direct-parent/transitive tree and ineligible classification for every fault witness. | Closed fault registry, direct-parent/containment and eligibility fixture rows, complete raw fault/event rows, receipt stages, four inventory fingerprints, reopen result, and exact root classification. | `UNSATISFIED` |
| `PR03E-M06` Committed cleanup convergence | Fault/crash before, between, and after successor adoption, predecessor active removal/sync, checkpoint removal/sync, each raw/segment/catalog/manifest staging removal/sync, clean-inventory proof, intent-last removal, and final sync. | Exact cleanup-prefix trace proving no committed raw/segment deletion, no postcommit fallback, no extra inventory, and eventual exact committed reopen. | `UNSATISFIED` |
| `PR03E-M07` Pressure and receipts | Overlay `StorageFull` and `QuotaExceeded` at every store-owned mutation boundary, including partial-write cases; verify first-wins sticky custody, runtime fail-stop, ordinary receipt preservation, no false new receipt/commit, no rotation start before covered receipts or the explicit no-op barrier, and ineligibility of every pressure/refusal/terminal result. | Pressure kind, raw diagnostic, sample class/outcome and false eligibility, custody/health transition, handled/durable stages, durability-batch/trigger join, ordered event prefix, process result, reopen-only convergence, and root/inventory fingerprints. | `UNSATISFIED` |
| `PR03E-M08` Committed fail-closed behavior | Generate missing, corrupt, foreign, malformed, truncated, partial, excessive, forked, unrelated, ambiguous, and catalog-mismatched committed segment/pair cases. | Sanitized refusal, complete eager-validation boundary, and exact before/after fingerprints with `UNCHANGED_REFUSAL`. | `UNSATISFIED` |
| `PR03E-M09` Raw/segment linkage | Completely checksum and hostile-parse every pair; verify exact StoreId, generation, range, registry, raw length/CRC, frame coverage, unchanged frame bytes, indexes, trailer, and Catalog V2 identity. | One complete validation row and bounded resource ledger per pair, including pair-state release before the next pair. | `UNSATISFIED` |
| `PR03E-M10` Bounds | Prove entries 1 and 64, pre-mutation entry-65 refusal, canonical inventory 156, and 157/unknown/mixed refusal unchanged. | Exact boundary traces, mutation count zero for refusals, and canonical before/after fingerprints. | `UNSATISFIED` |
| `PR03E-M11` Streaming resources | Execute minimum, representative, independent maximum, 64-pair, and refusal tiers; measure requested/actual logical/allocated external workspace for zero and nonzero observations; collect the complete demand/path matrix, closed direct-parent tree, literal ordered events, eligibility fixtures, native writer-delay, eager-open, RSS, storage, and representation ledgers without inventing thresholds. | Raw observations plus timing statistics/counts computed only from distinct eligible successes grouped by declared case/path; direct/transitive tree and fixture completeness; batch joins, witness labels, pair events, finite checked workspace/resource ledgers, platform/cache facts, and explicit `UNKNOWN` native limits/budgets/SLOs. PR03c/PR03d values remain tooling comparison data. | `UNSATISFIED` |

No row may become satisfied in a harness implementation PR merely because its
schema or test case exists. Structural harness checks and measured results are
distinct records. No row may cite PR03d as native proof.

## Acceptance and authority progression

1. **M03-PR03e docs acceptance:** authorized review of a bounded prerequisite
   before the complete private harness. It accepted no result, V2 artifact, or
   product budget.
2. **M03-PR03f instrumentation prerequisite:** supplies only disabled-by-default,
   rustdoc-hidden current-V1 source observation/fault/crash plumbing. It accepts
   no harness, report, measurement, V2 source binding, or product authority.
3. **M03-PR03g1 capability-contained executor foundation:** proves the compiled
   private literal descriptor/source-site/generated-wrapper bijection and compact
   disposable P0-P7, rollback, and eager-open execution behind one opaque V2 child
   capability subtree. This is finite reviewed-revision evidence, not a universal
   Rust I/O-source proof. It adds no report, collector, all-boundary crash campaign,
   or collection authorization; all crosswalk rows remain `UNSATISFIED`.
4. **M03-PR03g2 complete harness infrastructure:** must add the closed matrix,
   nonempty timing/fault reports, every-boundary parent-owned crash/reopen campaign,
   sanitization, and collection entry point. Only g2 acceptance may authorize
   collection.
5. **Measured native evidence:** the complete Linux x86_64 acceptance-candidate
   bundle is collected from a clean reviewed harness/measured SHA on the approved
   AgentBox. Darwin evidence remains exploratory. No SLO or budget is inferred.
6. **Fresh owner checkpoint:** the owner reviews the complete native results and
   decides whether any measured budgets and an implementation plan are accepted.
7. **Separate product implementation:** only after that explicit checkpoint may
   a Store Format V2 product PR be planned. Its actual code must rerun the entire
   matrix; harness-only evidence cannot substitute.

## Explicit exclusions

This plan authorizes no harness in this PR, `crates/`, `tools/`, `scripts/`, CI,
Cargo, dependency, product API, V2 opener/decoder/publication, format-byte or name
change, V1 migration, fallback, rebuild, repair, query, retention, compaction,
raw deletion, cloud execution, cache mutation, measured report, numeric native
budget, accepted native workspace threshold, or SLO. Physical power-loss and
excluded platforms/filesystems remain outside authority. Zero and nonzero native
workspace observations are evidence inputs, not authority granted by this plan.
