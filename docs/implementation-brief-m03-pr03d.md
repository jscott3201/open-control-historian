# M03-PR03d Linux x86_64 resource-evidence implementation brief

## Objective

Check in one bounded documentation/evidence-only record of the owner-accepted
M03-PR03c standalone tooling measurements on Linux x86_64. This successor closes
only the standalone Linux resource-measurement prerequisite for unchanged Native
Segment V1.

Current Store Format V1 remains the only implemented format. This slice adds no
`crates/` change, product API, durable byte or name, writer/open integration, or
Store Format V2 authority.

## Delivered evidence

- Eight exact sanitized tool outputs record the clean measured revision, platform,
  six-case/36-child timed matrix, complete samples and summaries, and open-64
  generation/validation.
- A relative-path `SHA256SUMS` manifest fixes all copied outputs and the clearly
  labeled orchestrator-derived verification record.
- Every RSS sample is strictly below `167,772,160` bytes. The high-water result is
  `104,087,552` bytes during max-observations validation.
- All 36 raw operation reports were independently verified to return controlled
  state and external-sort workspace to zero; every committed sample workspace
  column is zero.
- Open-64 generated and sequentially validated 64 pairs with controlled state and
  external workspace zero.

The durable [accepted evidence record](m03-pr03d-linux-resource-evidence.md)
contains source hashes, exact commands, platform facts, complete observed
statistics, evidence links, checksum procedure, and owner decision.

## Acceptance and remaining hard stop

The owner accepts these numbers as **standalone tooling resource evidence only**.
Standalone elapsed values are not writer delay, rotation latency, eager-open
latency, or SLOs. Writer-delay and eager-open SLOs remain `UNKNOWN`.

V2 product work remains blocked. M03-PR03e now defines the separately reviewed
[native execution-evidence plan](m03-pr03e-native-execution-evidence-plan.md) and
authorizes only a later private harness review. That harness, measured Linux
native results, and a fresh owner checkpoint remain mandatory. M03-PR03b's
transaction and failure matrix remains `UNSATISFIED`.

## PR acceptance commands

```console
(cd docs/evidence/m03-pr03d-linux-x86_64 && shasum -a 256 -c SHA256SUMS)
python3 scripts/check_repository.py
git diff --check
./scripts/gate.sh pr
```

A focused parser must also prove 36 rows, six cases, three build and three
validation samples per case, strict RSS bounds, zero external workspace,
Linux/x86_64 candidate labels, clean measured revision, raw-operation state-zero
verification, and open-64 state zero. The release gate is not requested.
