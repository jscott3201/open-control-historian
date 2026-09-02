# M03-PR03g2 complete-harness continuation

M03-PR03g2 completes the private PR03e structural harness infrastructure inside
the existing `och-v2-evidence` tooling package. The closed result contains 173
literal fault descriptors/source sites, 487 registry-derived applicability and
fault-result rows, a 639-row matrix, 173 timing-event rows, six timing summaries,
six resource ledgers, and an exact seven-data-file plus `SHA256SUMS` bundle.

The parent executes all 173 `CHILD_CRASH_AFTER_SUCCESS` targets using an
out-of-band request/ready protocol, abrupt kill, wait/reap, immediate fingerprint,
descriptor-selected reopen/convergence, path-free witness validation, and exactly
one parent cleanup attempt. The spawned process has a non-cleaning worker view;
`V2StoreChild` custody never leaves the parent. Control and report state never
enters a store child, and successful runs leave only the intended validated
structural bundle.

Bounded lifecycle handling now carries every observe/kill error through wait/reap;
unproven reap returns `REPLAN` without parent cleanup under the possibly live
child. Hidden-child startup independently rejects non-direct, non-directory, or
symlinked `cases`, `control`, selected-child, request, and ready layouts before
worker mutation. The structural report replacement keeps a valid prior while it
syncs and validates staging, synchronizes every authority rename/removal, rolls
handled failures back, and reconciles only unambiguous interrupted states.

Reopen consumes the actual post-kill synthetic transaction inventory. It proves
unchanged preflight, exact precommit rollback, or descriptor-successor committed
adoption/cleanup without clear-and-rebuild; committed raw/segment/catalog/manifest
final hashes must survive, and postcommit convergence cannot equal the prior root.

The structural command is repeatable and reports only
`STRUCTURAL_SYNTHETIC`. It explicitly states no collection, measured-native, or
product authority. The reserved `native-collect` parser always returns `REPLAN`
before root creation. There is no acceptance context or measured bundle builder;
caller SHA/tree assertions cannot mint evidence. A later accepted collector must
independently prove git facts and implement all timing tiers and three-repetition
fault/mode/pressure coverage before Linux collection can occur.

Every `PR03E-M01..M11` row remains `UNSATISFIED`. Native writer-delay,
eager-open, RSS, total-runtime, external-workspace limits, thresholds, budgets,
and SLOs remain `UNKNOWN`; `V2_PRODUCT_AUTHORITY=false`. The next permissible
step is a separately reviewed complete Linux x86_64 collector implementation,
then collection, evidence review, and a fresh owner checkpoint. Product planning
remains blocked.
