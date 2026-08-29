# M02-PR01b0 continuation: Journal V1 semantic frames

> **Successor note:** M02-PR01b1 now supplies the sole active-journal durable
> runtime vertical described as the hard boundary below. This file remains the
> historical framing record; current ownership and deferrals are in
> [M02-PR01b1](continuation-m02-pr01b1.md).

## Live outcome

M02-PR01b0 adds `och-store` as the third default native root with the sole inward
edge `och-store -> och-core`. `och-core` remains dependency-free; `och-runtime`
does not depend on `och-store`. The union native closure is three roots and five
packages because the new crate adds no third-party dependency.

`och-store` now owns the exact fixed Journal V1 header and independent admission
frame format documented in [Journal V1](journal-v1-format.md). Encoding consumes
only a borrowed already-authorized `CanonicalAdmission` and losslessly traverses
the governing declaration, envelope, retry, source batch/lifecycle, ordered
lineage, source gaps, values, times, quality/status, producer order, and all
artifact/content/provenance forms.

Hostile decode checks the declared payload ceiling before field allocation and
then applies exact inner length/count, primitive, tag, and cross-field checks. It
returns only `DecodedAdmissionV1`, whose declaration is a store-owned mirror and
which has no conversion to `CanonicalAdmission`, registry binding, or runtime
submission path. Deterministic re-encoding does not add authorization.

## Exact evidence

Focused store tests cover:

- exact 28-byte header and rich observed frame bytes;
- the independent primitive-only rich frame oracle and independent CRC-32C
  implementation/check vector;
- deterministic encode/decode/re-encode equality;
- all usable value families, unavailable with/without reason, important optional
  artifacts/idempotencies/timestamps/positions, revision two, observed, 64-gap,
  no-change, and 256-observation bounds;
- exact configured/hard payload ceilings, every truncation point, checksum,
  trailing input, sequence, magic/version/kind/flags, invalid identity, unknown
  tag, impossible length, and duplicate-evidence refusal.

The required PR gate additionally checks all default native members, strict
workspace clippy, full nextest, doctests, rustdoc links, dependency policy,
repository hygiene, cargo-deny bans/licenses/sources, and diff whitespace. No
release gate or durability/platform qualification is claimed by this record.

## Accepted PR01b split and hard boundary

The old M02-PR01b concept combined a complete byte grammar with filesystem,
writer, group-commit, receipt, and recovery behavior. The format and hostile
parser are independently reviewable load-bearing authority, so PR01b0 freezes
those semantics first. This split is not a delivered journal and does not permit
a second callable path beside the runtime writer.

M02-PR01b1 is the first complete active-journal durable vertical. It connects the
only runtime writer path to journal create/open/append, group commit, barriers,
durable cutoffs and receipts, and bounded reopen as one coherent vertical using
Journal V1. `WriterHandled` remains deliberately non-durable; the new distinct
durable stage is covered by the mechanical journal/checkpoint cutoff.

## Deferred ledger

- registry persistence/bootstrap, manifest generation, rotation, and handoff;
- successor generation/range publication, immutable artifacts, and physical
  reclamation;
- broad recovery events, full-disk qualification, durable retry horizons, and
  cross-platform power-loss qualification;
- query, rollup, retention/priority execution, adapters, and Studio/Engine work.

The removed historical bullets are delivered only by the reviewed PR01b1
successor, not retroactively by M02-PR01b0. No remaining ledger item is silently
implemented or claimed by either slice.
