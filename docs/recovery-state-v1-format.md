# Recovery State V1 format and conservative publication law

M02-PR03a adds one dependency-free, fixed-size report for the latest committed
automatic recovery. The report is diagnostics evidence, not registry, retry,
journal, catalog, or identity authority. It becomes authoritative only when an
accepted Manifest V4 references its exact slot, generation, length, and complete
checksum.

## Fixed inventory and bound

The store recognizes exactly three reusable finals and one staging name:

- `recovery-state-v1-slot-0.och`;
- `recovery-state-v1-slot-1.och`;
- `recovery-state-v1-slot-2.och`; and
- `recovery-state-v1.staging`.

Every final is exactly 96 bytes. Publication may replace only a slot not
referenced by either prospective valid manifest. Reads check the 96-byte ceiling
before allocation and canonical decode re-encodes the complete artifact exactly.
There is no list or unbounded recovery history.

## Exact 96-byte layout

All integers are unsigned big-endian. CRC-32C uses the Journal V1 Castagnoli
parameters.

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHRCV01` |
| 8 | 2 | version | unsigned `1` |
| 10 | 2 | record length | unsigned `96` |
| 12 | 16 | store identity | exact manifest `StoreId` |
| 28 | 8 | recovery generation | positive |
| 36 | 8 | source manifest generation | positive root that proved repair safety |
| 44 | 8 | journal generation | positive selected active generation |
| 52 | 8 | checkpoint generation | positive exact selected checkpoint slot generation |
| 60 | 8 | append sequence | selected manifest cutoff |
| 68 | 8 | end offset | selected manifest frame boundary, at least 28 |
| 76 | 8 | removed byte count | positive and strictly after the selected end |
| 84 | 1 | classification | `1` = committed-root suffix |
| 85 | 1 | action | `1` = removed active suffix |
| 86 | 2 | synchronized operation count | positive bounded primitive-operation evidence |
| 88 | 4 | reserved | zero |
| 92 | 4 | checksum | CRC-32C over bytes 0..92 |

Unknown versions/classes/actions, zero generations/counts, wrong scope, length,
reserved bytes, checksum, truncation, or trailing input refuse. Reports contain
no path, raw frame/observation/declaration/retry content, handle, platform error
string, or free-form text.

## Selection, mutation, and publication

The stable store lock is acquired before selection. Both manifest slots are read
and classified independently. The newest candidate must then validate its exact
registry, retry, catalog and seal metadata, generation inventory, active header
and identity, selected checkpoint/cutoff, retained declaration history, any prior
recovery report, and the narrow rotation law. No recovery mutation occurs before
all those checks pass.

The root-scoped active scan treats the manifest cutoff as the only adoption
boundary. It verifies every committed frame and may identify only bytes strictly
after that boundary. A valid suffix and a torn/malformed final candidate can be
removed; committed/interior corruption or a malformed candidate with later bytes
refuses. Recovery never derives authority from decoded suffix bytes, never
advances the checkpoint, and never changes registry or retry interpretation.

Accepted order is:

1. truncate the active artifact exactly to the selected manifest end and
   synchronize it;
2. when present, clear and synchronize only the strictly newer mechanical
   checkpoint slot, retaining the selected exact slot;
3. stage, synchronize, read back, canonically verify, rename, and directory-sync
   Recovery State V1 in an unreferenced slot;
4. stage, synchronize, read back, canonically verify, rename, and directory-sync
   Manifest V4 as the sole commit point;
5. adopt the V4/report in memory and remove only now-unreferenced bounded metadata.

A fault before the V4 rename returns no recovery success and may reopen to the
prior root or a typed non-mutating interrupted-publication refusal. A valid V4
rename reopens to exactly one report-bound recovered root even if its following
directory-sync call reported failure. Later opens retain the report but perform
no repeated action.

## Diagnostics and limits

`ManifestStoreInspection::recovery_report` and
`RuntimeInspection::recovery_report` expose the immutable report. Successful
runtime recovery remains `RuntimeHealth::Healthy`. Fresh stores have no report.
`ManifestStore::open_with_diagnostics` and error classification accessors expose
bounded startup classes without changing existing open signatures or exhaustive
error variants. Identity mismatch and stale restore intentionally share one
fail-closed class because no custody evidence exists yet.

Normal open remains bounded by two manifests, three slots per metadata family,
the configured active byte/record limits, and at most 64 sealed metadata/header
reads. It does not scan sealed payloads. This format adds no disk-pressure,
ENOSPC, degraded/read-only health, stale-restore acceptance, retention, or
destructive repair behavior.
