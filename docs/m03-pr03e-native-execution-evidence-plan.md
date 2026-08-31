# M03-PR03e Native V2 execution-evidence plan

## Status, authority, and evidence vocabulary

This document is the normative, executable evidence plan for a possible future
private Native V2 harness and report bundle. It is a **review barrier only**.
M03-PR03e adds no harness, product-reachable code, accepted V2 artifact, measured
result, numeric SLO or budget, opener, decoder, publication path, or Store Format
V2 authority. Store Format V1 remains the only implemented and accepted format.

Plan acceptance authorizes only a separate, bounded review of a private/test-only
harness. Every row in the [M03-PR03b future evidence matrix](#pr03b-evidence-crosswalk)
remains `UNSATISFIED` until the required harness exists, the measurements and
fault results exist, and the evidence returns through the authority progression
defined below. M03-PR03d is comparison evidence for a standalone tooling
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

Harness review authorizes measured evidence collection only. A later product
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
different processes are never subtracted. Each sample records `event_id`,
`phase_id`, `case_id`, `sample_id`, `process_mode`, `store_mode`, `start_ns`,
`stop_ns`, `elapsed_ns`, `outcome`, and optional bounded `pair_ordinal`.

The top-level event IDs and exact boundaries are:

| Event ID | Start | Stop |
| --- | --- | --- |
| `V2TIME-RECEIPT-HANDLED-DURABLE` | The handled stage becomes visible for an append. | Its durable stage becomes visible under the ordinary manifest. A terminal resolution is an incomplete latency sample recorded in fault results, not a durable stop. This is ordinary durability and is never a rotation duration. |
| `V2TIME-WRITER-ROTATION-DELAY` | After ordinary flush, inspection update, and covered durable receipt completion, when the sole writer commits to rotate, immediately before `V2TX-P0-PREFLIGHT`. | The writer can accept the next ordered append, or returns the exact terminal/reopen-required result. Queue and admission delay before the start are excluded. |
| `V2TIME-ROTATION-MUTATION-CRITICAL` | Immediately before the first `V2TX-P1-INTENT` mutation. | The final directory sync after intent-last cleanup, or immediately before an injected operation error returns with its precommit/postcommit classification. |
| `V2TIME-EAGER-OPEN` | Future V2 open entry, before format and inventory preflight. | A fully validated writable handle is returned, or the sanitized refusal is fixed before return. |

Required subevents are `V2TIME-ORD-JOURNAL-SYNC`,
`V2TIME-ORD-CHECKPOINT-SYNC`, `V2TIME-ORD-RETRY-PUBLISH`,
`V2TIME-ORD-MANIFEST-COMMIT`, `V2TIME-ORD-INSPECTION-UPDATE`,
`V2TIME-ORD-RECEIPT-RESOLVE`, `V2TIME-P0-PREFLIGHT`,
`V2TIME-P1-INTENT`, `V2TIME-P2-RAW`, `V2TIME-P3-SEGMENT`,
`V2TIME-P4-SUCCESSOR`, `V2TIME-P5-CATALOG`,
`V2TIME-P6-MANIFEST-COMMIT`, `V2TIME-P7-ADOPTION`,
`V2TIME-P7-CLEANUP`, and `V2TIME-OPEN-PAIR-VALIDATION`. The last event has one
row per pair with `pair_ordinal=1..=64`; aggregate eager-open duration does not
replace those rows.

A killed child cannot emit an in-process stop event. Crash-after-success cases
therefore record the child's last successful boundary event and the parent's
separate process-exit/reopen observations; they are fault witnesses and are not
included in timing distributions unless a complete same-process start/stop pair
exists. Parent and child monotonic values are never combined.

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
sample count.

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
- `CHILD_CRASH_AFTER_SUCCESS`: after the boundary returns success, a parent-owned
  controller uses OS kill/abort or an equivalent abrupt immediate termination
  before the next registered boundary. The child performs no unwinding,
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
- The zero-external-sort/workspace design remains mandatory. If the later harness
  needs nonzero external workspace, it must return `REPLAN` for owner review
  rather than measuring or normalizing it.

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

The PR03c 160 MiB prototype target and every PR03d standalone value are comparison
data only. Native peak RSS, writer rotation delay, eager-open latency, total
runtime, headroom, and all SLOs/budgets remain `UNKNOWN` until native measurements
return and the owner accepts them. This plan sets no pass threshold for those
observations.

## Bounded report bundle and schema

Reports are schema-versioned UTF-8 KV/TSV, never free-form logs. Schema version
`m03-pr03e-v1` requires this bounded bundle:

| Relative file | Required contents |
| --- | --- |
| `run.kv` | Plan acceptance SHA; clean harness SHA and measured source SHA; tracked and untracked tree status; `Cargo.lock`, harness-source, and instrumentation-source SHA-256; exact commands/profile; Rust/Cargo versions; platform, CPU, memory, filesystem, mount/locality, load, storage/headroom, page/cache/store facts; report classification and exclusions. |
| `timing-samples.tsv` | Every complete sample and event ID with exact process/store/cache labels, monotonic start/stop/elapsed nanoseconds, result, and pair ordinal. |
| `timing-summary.tsv` | Required count, actual count, min/median/observed p90/p95/p99/max/IQR/MAD or witness-only min/median/max plus `percentile_claim=NONE`. |
| `resource-ledger.tsv` | Every actual/requested capacity and all process, stack, RSS, storage, workspace, concurrent-inventory, headroom, page, and cache fields listed above. |
| `fault-registry.tsv` | The complete closed registry row for every instrumented boundary. |
| `fault-results.tsv` | Fault ID, mode, repetition, expected/actual result, pressure kind/raw diagnostic, receipt stages, process result, last boundary, pre/immediate/reopen/final fingerprints, and root classification. |
| `matrix.tsv` | Every PR03b crosswalk ID and every timing, bound, hostile, fault/mode, pressure, platform, and report obligation with expected rows, observed rows, and `PASS`/`FAIL`; no skipped or implied row. |
| `SHA256SUMS` | Relative SHA-256 for every other bounded report file and no unlisted report file. |

All KV keys and TSV columns are closed and schema-validated. The bundle must cap
itself at 64 MiB, each data file at 16 MiB, each physical line at 4,096 bytes,
each scalar at 1,024 bytes, and row counts at the exact precomputed matrix count.
If the complete required matrix cannot fit, the harness returns `REPLAN`; it does
not truncate. Reports include complete samples but no canonical payload.

Validation rejects a missing/unknown/duplicate ID, missing required field,
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
| `PR03E-M05` Transaction convergence | Exercise every registered phase boundary in all applicable modes with three identical repetitions; allow only exact prior-root rollback before commit and committed-root adoption after commit. | Closed fault registry, complete fault rows, receipt stages, four inventory fingerprints, reopen result, and exact root classification. | `UNSATISFIED` |
| `PR03E-M06` Committed cleanup convergence | Fault/crash before, between, and after successor adoption, predecessor active removal/sync, checkpoint removal/sync, each raw/segment/catalog/manifest staging removal/sync, clean-inventory proof, intent-last removal, and final sync. | Exact cleanup-prefix trace proving no committed raw/segment deletion, no postcommit fallback, no extra inventory, and eventual exact committed reopen. | `UNSATISFIED` |
| `PR03E-M07` Pressure and receipts | Overlay `StorageFull` and `QuotaExceeded` at every store-owned mutation boundary, including partial-write cases; verify first-wins sticky custody, runtime fail-stop, ordinary receipt preservation, and no false new receipt/commit. | Pressure kind, raw diagnostic, custody/health transition, handled/durable stages, process result, reopen-only convergence, and root/inventory fingerprints. | `UNSATISFIED` |
| `PR03E-M08` Committed fail-closed behavior | Generate missing, corrupt, foreign, malformed, truncated, partial, excessive, forked, unrelated, ambiguous, and catalog-mismatched committed segment/pair cases. | Sanitized refusal, complete eager-validation boundary, and exact before/after fingerprints with `UNCHANGED_REFUSAL`. | `UNSATISFIED` |
| `PR03E-M09` Raw/segment linkage | Completely checksum and hostile-parse every pair; verify exact StoreId, generation, range, registry, raw length/CRC, frame coverage, unchanged frame bytes, indexes, trailer, and Catalog V2 identity. | One complete validation row and bounded resource ledger per pair, including pair-state release before the next pair. | `UNSATISFIED` |
| `PR03E-M10` Bounds | Prove entries 1 and 64, pre-mutation entry-65 refusal, canonical inventory 156, and 157/unknown/mixed refusal unchanged. | Exact boundary traces, mutation count zero for refusals, and canonical before/after fingerprints. | `UNSATISFIED` |
| `PR03E-M11` Streaming resources | Execute minimum, representative, independent maximum, 64-pair, and refusal tiers with zero external workspace; collect complete native writer-delay, eager-open, RSS, storage, and representation ledgers without thresholds. | All timing samples/statistics, witness labels, pair events, complete resource ledger, platform/cache facts, and explicit `UNKNOWN` budgets/SLOs. PR03d values appear only as labeled comparison data. | `UNSATISFIED` |

No row may become satisfied in a harness implementation PR merely because its
schema or test case exists. Structural harness checks and measured results are
distinct records. No row may cite PR03d as native proof.

## Acceptance and authority progression

1. **M03-PR03e docs acceptance:** authorizes only review of one later bounded
   private harness PR. It accepts no code, result, V2 artifact, or product budget.
2. **Later private harness review:** must pass plan-schema, closed-registry,
   matrix-completeness, disposable-root, sanitization, and functional structural
   checks. Acceptance authorizes evidence collection only.
3. **Measured native evidence:** the complete Linux x86_64 acceptance-candidate
   bundle is collected from a clean reviewed harness/measured SHA on the approved
   AgentBox. Darwin evidence remains exploratory. No SLO or budget is inferred.
4. **Fresh owner checkpoint:** the owner reviews the complete native results and
   decides whether any measured budgets and an implementation plan are accepted.
5. **Separate product implementation:** only after that explicit checkpoint may
   a Store Format V2 product PR be planned. Its actual code must rerun the entire
   matrix; harness-only evidence cannot substitute.

## Explicit exclusions

This plan authorizes no harness in this PR, `crates/`, `tools/`, `scripts/`, CI,
Cargo, dependency, product API, V2 opener/decoder/publication, format-byte or name
change, V1 migration, fallback, rebuild, repair, query, retention, compaction,
raw deletion, cloud execution, cache mutation, measured report, numeric native
budget, or SLO. Physical power-loss, excluded platforms/filesystems, and nonzero
external workspace remain outside authority.
