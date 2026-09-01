# M03-PR03d Linux x86_64 resource-evidence continuation

## Delivered result

M03-PR03d records the owner-accepted M03-PR03c standalone tooling resource
measurements from a detached clean Linux x86_64 checkout of
`4e679a313477505c1dd90d23d08ef666b92e47c7`. The checked-in evidence contains 36
timed children across six cases plus one 64-pair functional generation and
sequential validation.

Every observed RSS value was strictly below the `167,772,160`-byte target. The
largest peak was `104,087,552` bytes during max-observations validation, and the
largest elapsed maximum was `19.24s` during max-bytes validation. All 36 raw
operation reports independently recorded controlled state zero and external-sort
workspace zero. Open-64 reported 64 pairs, sequential pair state, controlled
state zero, and external workspace zero.

The [accepted evidence record](m03-pr03d-linux-resource-evidence.md) links the
exact sanitized reports, complete min/median/observed-p95/max data and samples,
source anchors, command matrix, platform record, derived verification, and
relative-path checksum manifest. With three samples, observed p95 is only the
highest observed sample, not a population percentile.

## Accepted boundary

This is **standalone tooling resource evidence only** for unchanged Native
Segment V1. It satisfies only M03-PR03c's standalone Linux x86_64 resource
measurement condition. It adds no `crates/` change, product API, durable byte or
name, writer/rotation/open integration, or V2 authority. Store Format V1 remains
the only implemented format.

Standalone elapsed values are not writer delay, rotation latency, eager-open
latency, or SLOs. Writer-delay and eager-open SLOs remain `UNKNOWN`. No release
gate or production benchmark is claimed.

## Successor handoff

Store Format V2 implementation remains blocked. A separately reviewed native
timing/transaction/fault/cleanup/pressure/receipt
[execution-evidence plan](m03-pr03e-native-execution-evidence-plan.md) now defines
the later private-harness integration proof without implementing it. That harness
and measured Linux native results must return for a fresh owner checkpoint. The
complete M03-PR03b transaction, crash, cleanup, pressure, receipt, fail-closed,
and authority matrix remains mandatory and `UNSATISFIED`.

There is still no current V2 decoder/opener/publication path, native streaming
module, durable segment authority, migration, fallback, deletion, repair, runtime
query integration, memory mapping, external database/cloud dependency, or product
latency claim.
