# M03-PR03e Native V2 execution-evidence plan continuation

## Delivered review barrier

M03-PR03e records one documentation-only executable plan for the native
timing/transaction/fault/cleanup/pressure/receipt evidence that still blocks any
Store Format V2 product work. Current product code, Store Format V1, Cargo,
dependencies, and every reviewed V2 byte and name remain unchanged.

The normative
[execution-evidence plan](m03-pr03e-native-execution-evidence-plan.md) binds a
future private harness to the current sole-writer, ordinary durability, inspection,
durable-receipt, manifest-store, active-journal, and sticky-pressure seams. It
requires ordinary journal/checkpoint/retry/Manifest durability, inspection update,
and covered receipt resolution before automatic rotation begins. A prior ordinary
durable receipt remains truthful if the later rotation fails.

## Frozen future evidence protocol

The plan defines literal V2 transaction phase and timing IDs, one expected path
per automatic-rotation case, a closed demand matrix covering pre-append fit/age
and post-publication size/count/age across both `PRE_APPEND` and
`POST_PUBLICATION`, per-case/path counts, closed event ordering and trace fixtures,
Manifest-rename commit-side classification, exact precommit rollback/postcommit
adoption law, intent-last committed cleanup, complete pair validation, and the
prohibition on reusing incompatible current V1 cleanup ordering. It requires:

- 30 independent fresh-process and 100 warm same-process samples for tractable
  minimum and representative timing cases, with each warm sample starting from
  an independently prepared equivalent precondition unless explicitly `REUSED`;
- three witness runs and no percentile claim for independent massive maxima,
  64-pair eager open, and 65th-entry refusal;
- three identical deterministic repetitions for every closed fault-ID/mode and
  pressure-kind combination;
- pre-operation error, applicable partial write, and abrupt OS-kill/abort
  crash-after-success evidence without unwinding or `Drop` at every registered
  boundary;
- both `StorageFull` and `QuotaExceeded` at every store-owned mutation boundary,
  with first-wins sticky reopen custody and no false commit or receipt;
- exact prior/immediate/reopen/final inventory fingerprints and one of only
  `PRIOR_ROOT`, `COMMITTED_ROOT`, or `UNCHANGED_REFUSAL`;
- canonical 156-entry success, 157/unknown/mixed unchanged refusal, Catalog V2
  entries 1 and 64 success, entry 65 pre-mutation refusal, and sequential
  one-pair-at-a-time validation of all 64 committed pairs; and
- a bounded sanitized KV/TSV report bundle with complete samples, resource
  ledger, source hashes, matrix coverage, and relative `SHA256SUMS`.

PR03c's 160 MiB and zero-external-workspace results remain standalone tooling
comparison data only. Every native case must ledger requested and actual logical
and allocated external workspace, whether zero or nonzero. Nonzero is not by
itself a plan failure or `REPLAN`; incomplete, unbounded, overflowing, or
unledgered workspace fails. The accepted native workspace limit, threshold, and
SLO remain `UNKNOWN` pending measured Linux evidence and fresh owner review.

The dedicated owner-approved GCP Linux x86_64 AgentBox is the later
acceptance-candidate platform. Hosted PR CI is functional only and Darwin arm64
is functional/report-sanitization plus exploratory timing. Physical power loss,
cache-drop mutation, and excluded platforms/filesystems are outside the plan.

## Authority and successor handoff

Every one of the 11 M03-PR03b future evidence rows is mapped to a future harness
and report obligation and remains `UNSATISFIED`. M03-PR03d remains accepted
standalone tooling comparison evidence only. Native RSS, writer rotation delay,
eager-open latency, total-runtime budgets, workspace acceptance threshold, and
SLOs remain `UNKNOWN`.

Acceptance of this docs slice authorizes only a later bounded private harness PR.
That harness may emit reviewed V2 names/bytes only in newly absent, exclusively
created store-under-test children beneath an out-of-band private evidence parent.
Descriptors, disposable markers, reports, and process controls stay outside each
exact V2 inventory. The harness grants no product authority. Harness acceptance
authorizes measured collection only. Complete Linux native results must return
for a fresh owner checkpoint; only after owner acceptance may a separate V2
product implementation be planned, and its actual code must rerun the complete
matrix.

There is still no current V2 opener, decoder, publication path, compatibility
promise, migration, fallback, rebuild, repair, query integration, retention,
compaction, raw deletion, numeric native budget, or SLO.
