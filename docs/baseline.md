# Native foundation baseline

This measurement describes only the measurement example in `och-core`. It is a
build/dependency regression anchor, not evidence of Historian throughput,
latency, durability, or resident service cost.

## Initial local measurement

Recorded for M00-PR01 with `./scripts/measure-baseline.sh`:

| Field | Value |
| --- | --- |
| Machine | Darwin 25.6.0, arm64 |
| Compiler | rustc 1.98.0 (88d9e12ae 2026-08-18) |
| Configuration | release; thin LTO; one codegen unit; `panic=abort`; symbols stripped |
| Native roots | 1 |
| Native closure packages | 1 |
| Baseline executable | 339,120 bytes |
| Enforced executable bound | 1,048,576 bytes |
| Idle RSS | N/A — there is no long-running process to measure |

The script first builds and runs the example, verifies its fixed marker output,
then selects only the configured default `och-core` root from all-present-feature
Cargo metadata. It fails unless the workspace has the three reviewed native
roots, `och-core`'s own closure remains one package, and the executable stays
within the bound. It does not build or measure `och-runtime`; Tokio is therefore
not attributed to this model baseline. Live results are written to
`target/baseline/baseline.txt`; release-cycle CI uploads that file so
platform-specific evidence is not confused with this initial macOS record.

The executable size above remains the historical M00 measurement. M01 changed
the workspace policy result to two native roots and a four-package union
closure (`och-core`, `och-runtime`, `tokio`, and `pin-project-lite`). M02-PR01b0
adds dependency-light `och-store`, making the current result three native roots
and a five-package union closure while the
measured `och-core` closure remains exactly one package. No runtime binary, RSS,
latency, throughput, ingress, or durability measurement is claimed.

The 1 MiB executable limit is a regression tripwire, not a performance target.
If a later reviewed architecture change legitimately changes the baseline,
update the script bound and this evidence in the same change with rationale.
