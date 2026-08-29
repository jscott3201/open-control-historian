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
computes the all-present-feature native closure from Cargo metadata, and fails if
the closure is not one package or the executable exceeds the bound. Live results
are written to `target/baseline/baseline.txt`; release-cycle CI uploads that file
so platform-specific evidence is not confused with this initial macOS record.

The 1 MiB executable limit is a regression tripwire, not a performance target.
If a later reviewed architecture change legitimately changes the baseline,
update the script bound and this evidence in the same change with rationale.
