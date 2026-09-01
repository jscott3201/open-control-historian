# M03-PR03f native evidence-instrumentation continuation

## Delivered prerequisite

M03-PR03f adds the owner-authorized, disabled-by-default
`m03-pr03e-native-harness` feature to `och-runtime` and `och-store`. Runtime owns
the rustdoc-hidden temporary facade; store exposes only the hidden native seam
needed across the existing inward edge. Both defaults are explicitly empty and
runtime forwards only the same-named store feature. There is no dependency,
lockfile, root workspace, default-member, or tooling-package change.

The closed native registry binds only actual current-V1 append, ordinary
durability, Manifest, pressure, handled, inspection, durable receipt, and
rotation seams. One fixed-capacity process-local session records explicit
begin/finish events from one monotonic origin, supports one exact legal fault
target, reports structural incompleteness without overwriting, and never changes
product truth when observation alone fails.

## Preserved current behavior

Feature-off builds contain no instrumentation state or activation path. A
feature-on session with no fault has byte-for-byte identical Store Format V1
inventory and the same inspection and receipt outcomes as a sessionless fixture.
Ordinary ordering remains:

```text
journal sync -> checkpoint write/sync/adopt -> Retry State V1 publication
-> Manifest V1 prepare/rename/postcommit/adopt -> inspection update
-> durable-batch receipt resolution -> only then rotation
```

Real `StorageFull` and `QuotaExceeded` pre-operation/partial-write injections use
existing typed classification, sticky `ReopenRequired` custody, runtime fail-stop
health, no-false-durability behavior, reaper-joined shutdown, and validated reopen.
Crash readiness is in memory; the executing native path blocks inside the exact
successful boundary finish before returning, while the parent owns pipe
readiness, kill, wait/reap, and
reopen. Native instrumentation writes no control/report file and never aborts.

## Remaining authority boundary

This is instrumentation first, not a partial harness. There is no
`tools/och-v2-native-harness`, report bundle, matrix collector, fixture oracle,
measurement command/result, cloud execution, cache mutation, V2 ID/phase/source
binding, V2 product code, accepted threshold, budget, or SLO. Every
`PR03E-M01..M11` row remains `UNSATISFIED`; native RSS, writer rotation delay,
eager-open latency, total runtime, headroom, and external-workspace acceptance
remain `UNKNOWN`.

The next permissible slice after acceptance is one separately reviewed complete
private harness consuming this seam. Harness acceptance would authorize evidence
collection only. Complete Linux x86_64 native results must return for fresh owner
review before any V2 product proposal, and later product code must rerun the full
matrix.
