# M03-PR03e Native V2 execution-evidence plan implementation brief

## Objective and authority fence

Check in one bounded documentation-only review barrier that turns the remaining
M03-PR03b native timing/transaction/fault/cleanup/pressure/receipt prerequisite
into an executable future private-harness and report protocol.

This slice adds no harness, product code, Cargo/dependency change, V2 opener or
publication path, accepted V2 artifact, measured result, numeric native budget,
or SLO. Store Format V1 remains the only implemented format. Acceptance authorizes
only a later bounded private/test-only harness review.

## Delivered documentation contract

The normative
[Native V2 execution-evidence plan](m03-pr03e-native-execution-evidence-plan.md)
defines:

- the current runtime/store/receipt ownership seams and unchanged ordinary
  durability-before-rotation ordering;
- literal transaction phase IDs from read-only preflight through Manifest V2
  commit, adoption, exact committed cleanup, and intent-last synchronization;
- prior-root versus committed-root crash and refusal classification;
- exact monotonic timing event boundaries for handled-to-durable receipt latency,
  writer rotation delay, mutation-critical time, eager open, every transaction
  phase, and every pair validation;
- cold/warm process, filesystem-cache, store-reuse, sample-count, witness-only,
  observed-statistic, platform, and no-outlier policy;
- a closed semantic fault registry, complete I/O-boundary validator,
  deterministic pre-operation/partial-write/crash-after-success modes, and both
  standard-library pressure overlays;
- exact receipt, pressure, reopen, inventory-fingerprint, hostile, bound,
  64-pair, resource-ledger, and zero-workspace obligations;
- one bounded, schema-versioned, sanitized KV/TSV report bundle with complete
  source hashes, commands, samples, ledgers, fault results, matrix rows, and
  `SHA256SUMS`; and
- an explicit crosswalk from every PR03b future-evidence row to its harness and
  report obligation, with every row still `UNSATISFIED`.

The plan preserves the reviewed V2 names, bytes, Manifest-last authority, eager
full-validation requirement, raw-retention law, and exact committed cleanup
order. It explicitly forbids reuse of incompatible current V1 cleanup ordering.

## Bounds and platform boundary

Tractable timing requires at least 30 independent fresh-process and 100 warm
same-process samples. Independent massive maxima, 64-pair eager open, and
65th-entry refusal require three witness runs with no percentile claim. Every
fault-ID/mode combination requires at least three identical deterministic
repetitions.

The existing owner-approved dedicated GCP Linux x86_64 AgentBox is the later
acceptance-candidate target. Hosted PR CI remains functional only; Darwin arm64
is functional/report-sanitization and exploratory timing only. No machine or
cloud execution belongs to this docs slice.

The exact 64-pair-plus-active logical planning floor is `75,728,169,472` bytes,
and one maximum 64-pair sweep reads about `109,551,035,136` bytes. These are
planning arithmetic, not measured native budgets. PR03c's 160 MiB target and
PR03d's accepted standalone values remain comparison data only. Native RSS,
writer delay, eager-open latency, total-runtime budgets, and SLOs remain
`UNKNOWN`.

## Acceptance and successor handoff

Focused documentation checks must prove that all 11 PR03b rows are mapped exactly
once and remain `UNSATISFIED`, and that phase IDs, timing events, fault modes,
sample tiers, platform/cache policy, report files/fields, authority progression,
and exclusions are present.

Required PR gates are:

```console
python3 scripts/check_repository.py
git diff --check
./scripts/gate.sh pr
```

The release gate is not requested. The next permissible slice is a separately
reviewed private harness PR. Harness acceptance would authorize evidence
collection only; measured Linux native results must then return for a fresh owner
checkpoint before any product implementation may be planned.

## Explicit exclusions

No change under `crates/`, `tools/`, `scripts/`, `.github`, Cargo files, or
dependencies belongs to this PR. There is no V2 implementation, opener, decoder,
publication, migration, fallback, rebuild, repair, query, retention, compaction,
raw deletion, GCP run, measured report, product authority, numeric SLO, or budget
claim.
