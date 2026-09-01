# Documentation index

This index separates current implemented authority from historical delivery
records and future review material. OpenControl Historian is an early-stage,
source-only workspace. Store Format V1 is the only implemented durable format;
Native Segment V1 is currently an offline, non-authorizing candidate/query, not a
published segment or durable query authority.

## Start here

- [Public project overview](../README.md)
- [Architecture and current ownership](architecture.md), including the
  [accessible architecture diagram](assets/architecture.svg)
- [Canonical model contract](model-contract.md)
- [Native dependency policy](dependency-policy.md)
- [Contributor workflow](../CONTRIBUTING.md)
- [Repository automation constraints](../AGENTS.md)

The independent M00 model fixture/oracle evidence remains under
[`crates/och-core/tests/`](../crates/och-core/tests/). It proves reviewed model
contracts; it is not runtime or durable-format evidence.

## Current implemented contracts: Store Format V1 only

These documents describe current accepted/emitted product behavior:

- [Store Format V1](store-format-v1.md) — reset-epoch marker and mutation-free
  refusal fence
- [Journal V1](journal-v1-format.md) — header, canonical admission frames,
  active artifacts, checkpoints, bounds, checksum, and decode authority
- [Manifest V1 and registry snapshot](manifest-v1-format.md) — committed active
  cutoff and registry/retry/catalog/recovery references
- [Retry State V1](retry-state-v1-format.md) — bounded durable replay and guard
  horizon
- [Generation Catalog V1](generation-catalog-v1-format.md) — bounded committed
  sealed-generation inventory
- [Sealed raw Journal V1](sealed-journal-v1-format.md) — immutable pre-segment
  artifact and streaming verification
- [Recovery State V1](recovery-state-v1-format.md) — non-authorizing event
  evidence for conservative terminal-suffix recovery

Durability is scoped to these documented V1 filesystem and synchronization laws;
it is not a universal physical-power-loss guarantee.

## Current Native Segment V1 candidate/query

- [Native Segment V1 candidate format](native-segment-v1-format.md) defines one
  dependency-free, bounded, offline candidate derived from one committed sealed
  raw-Journal generation.
- [Native Segment V1 observation query](native-segment-query-v1.md) defines one
  bounded, recent-first, read-only observation query over an already parsed
  candidate and the exact synchronous one-generation store composition.

Candidate bytes are not Store Format V1 inventory, are not named by Manifest V1
or Generation Catalog V1, and authorize no registry, durable query, publication,
retention, or reclamation state.

## Evidence classifications

- [Native foundation baseline](baseline.md) is an `och-core` build/dependency and
  binary-size sanity marker. It is not a Historian CLI or evidence of runtime
  throughput, latency, RSS, durability, or production readiness.
- [Transient typed-value block codec proof](typed-value-block-codec-proof.md) is
  private `och-store` crate-test evidence only. Its bytes are not a product API,
  durable format, Native Segment V1 byte, or compatibility promise.
- [M03-PR03c resource plan](m03-pr03c-segment-resource-plan.md) and
  [M03-PR03d accepted Linux resource evidence](m03-pr03d-linux-resource-evidence.md)
  cover standalone private tooling only. The tracked bounded evidence bundle is
  discoverable under
  [`docs/evidence/m03-pr03d-linux-x86_64/`](evidence/m03-pr03d-linux-x86_64/).
  None of it proves native writer, open, runtime, or production behavior.
- [M03-PR03e native execution-evidence plan](m03-pr03e-native-execution-evidence-plan.md)
  remains the review plan for a later private harness and report.
- [M03-PR03f native evidence instrumentation](implementation-brief-m03-pr03f.md)
  is the disabled-by-default current-V1 native prerequisite.
- [M03-PR03g1 private executor foundation](implementation-brief-m03-pr03g1.md)
  consumes that seam from existing private tooling only for capability-contained
  disposable V2 execution and current-V1 success/pressure smoke. The containment
  claim is finite to the reviewed private Rust API and source revision, not all
  arbitrary or future filesystem I/O. It emits no
  report and does not authorize collection; all M01-M11 rows remain
  `UNSATISFIED` and all native limits remain `UNKNOWN`.

## Historical delivery records

These records preserve the scope, acceptance evidence, and successor boundary of
each delivered slice. Later current contracts above govern present behavior where
a historical record says otherwise.

### M00 — foundation and canonical authority

- **M00-PR01:** [foundation implementation brief](implementation-brief.md)
- **M00-PR02:** [canonical-model implementation brief](implementation-brief-m00-pr02.md)
  and [continuation](continuation-m00-pr02.md)
- **M00-PR03:** [independent evidence record](continuation-m00-pr03.md)
- **M00-PR04:** [series lifecycle and declaration-authority record](continuation-m00-pr04.md)
- **M00-PR05:** [source/capture and canonical-admission record](continuation-m00-pr05.md)

### M01 — runtime lifecycle, ingress, and latest publication

- **M01-PR01:** [lifecycle contract and implementation brief](implementation-brief-m01-pr01.md)
- **M01-PR02:** [bounded-ingress delivery record](continuation-m01-pr02.md)
- **M01-PR03:** [latest-publication delivery record](continuation-m01-pr03.md)

### M02 — Store Format V1 durability, rotation, and recovery

- **M02-PR01a:** [canonical-admission runtime record](continuation-m02-pr01a.md)
- **M02-PR01b0:** [Journal V1 implementation brief](implementation-brief-m02-pr01b0.md)
  and [continuation](continuation-m02-pr01b0.md)
- **M02-PR01b1:** [active-journal implementation brief](implementation-brief-m02-pr01b1.md)
  and [continuation](continuation-m02-pr01b1.md)
- **M02-PR02a:** [manifest/registry implementation brief](implementation-brief-m02-pr02a.md)
  and [continuation](continuation-m02-pr02a.md)
- **M02-PR02b:** [durable retry implementation brief](implementation-brief-m02-pr02b.md)
  and [continuation](continuation-m02-pr02b.md)
- **M02-PR02c:** [rotation/seal implementation brief](implementation-brief-m02-pr02c.md)
  and [continuation](continuation-m02-pr02c.md)
- **M02 durable-format reset:** [implementation brief](implementation-brief-m02-v1-durable-format-reset.md)
  and [continuation](continuation-m02-v1-durable-format-reset.md)
- **M02-PR03a:** [conservative recovery implementation brief](implementation-brief-m02-pr03a.md)
  and [continuation](continuation-m02-pr03a.md)
- **M02-PR03b1:** [store pressure/preflight implementation brief](implementation-brief-m02-pr03b1.md)
  and [continuation](continuation-m02-pr03b1.md)
- **M02-PR03b2:** [runtime pressure implementation brief](implementation-brief-m02-pr03b2.md)
  and [continuation](continuation-m02-pr03b2.md)

### M03 — offline segment candidate/query and future-review barriers

- **M03-PR01a:** [Native Segment candidate implementation brief](implementation-brief-m03-pr01a.md)
  and [continuation](continuation-m03-pr01a.md)
- **M03-PR02a:** [in-memory query implementation brief](implementation-brief-m03-pr02a.md)
  and [continuation](continuation-m03-pr02a.md)
- **M03-PR02b:** [one-generation store bridge implementation brief](implementation-brief-m03-pr02b.md)
  and [continuation](continuation-m03-pr02b.md)
- **M03-PR03a:** [typed-value proof implementation brief](implementation-brief-m03-pr03a.md),
  [proof contract](typed-value-block-codec-proof.md), and
  [continuation](continuation-m03-pr03a.md)
- **M03-PR03b:** [future V2 review-barrier implementation brief](implementation-brief-m03-pr03b.md)
  and [continuation](continuation-m03-pr03b.md)
- **M03-PR03c:** [tooling resource plan](m03-pr03c-segment-resource-plan.md),
  [implementation brief](implementation-brief-m03-pr03c.md), and
  [continuation](continuation-m03-pr03c.md)
- **M03-PR03d:** [accepted standalone tooling evidence](m03-pr03d-linux-resource-evidence.md),
  [implementation brief](implementation-brief-m03-pr03d.md), and
  [continuation](continuation-m03-pr03d.md)
- **M03-PR03e:** [future native evidence plan](m03-pr03e-native-execution-evidence-plan.md),
  [implementation brief](implementation-brief-m03-pr03e.md), and
  [continuation](continuation-m03-pr03e.md)
- **M03-PR03f:** [native evidence-instrumentation brief](implementation-brief-m03-pr03f.md)
  and [continuation](continuation-m03-pr03f.md)
- **M03-PR03g1:** [private executor-foundation brief](implementation-brief-m03-pr03g1.md)
  and [continuation](continuation-m03-pr03g1.md)

## Future Store Format V2 review material

> **Unimplemented and non-authorizing.** These documents are review contracts for
> possible future work. They are not accepted or emitted product bytes, do not
> implement runtime segment publication, and create no migration, decoder, or
> compatibility promise. Current V1 code must continue to refuse V2 names/bytes.

- [Store Format V2 design contract](store-format-v2-contract.md)
- [Manifest V2 design contract](manifest-v2-contract.md)
- [Generation Catalog V2 design contract](generation-catalog-v2-contract.md)
- [Published Native Segment V1 design contract](published-native-segment-v1-contract.md)
- [M03-PR03b implementation barrier](implementation-brief-m03-pr03b.md)
- [M03-PR03e private-harness/evidence plan](m03-pr03e-native-execution-evidence-plan.md)
- [M03-PR03f native evidence-instrumentation prerequisite](implementation-brief-m03-pr03f.md)
- [M03-PR03g1 private executor-foundation boundary](implementation-brief-m03-pr03g1.md)

The PR03c tooling target and PR03d standalone Linux x86_64 measurements do not set
a native workspace threshold. PR03f supplies current-V1 instrumentation and
PR03g1 supplies only its private executor foundation. M03-PR03g2, later measured
native results, and a fresh owner checkpoint remain prerequisites for any separately
reviewed V2 product proposal.
