# M02-PR03a current-V1 conservative recovery continuation

## Delivered boundary

Manifest V1 remains version one and exactly 160 bytes. Its former reserved bytes
`116..124` now carry a canonical optional Recovery State V1 slot and complete
artifact checksum. Recovery State V1 is an exact 128-byte, StoreId-scoped,
CRC-protected event with source/commit/report generations, source manifest CRC,
active generation/floor/cutoff, original length, removed bytes, closed
classification, and one truncate action.

The recognized inventory grows from 87 to 91 only through three report finals and
one staging name. Reports are non-authorizing. Ordinary append, registry, retry,
and rotation publication preserves the latest reference exactly. Consecutive
recoveries use different slots and increment report generation; both retained
manifest references protect their slots, and a later ordinary commit may clean
only the strictly older unreferenced report.

## Opening and crash behavior

Current-format preflight remains read-only before stable-lock creation or
acquisition. Under the stable lock, open resolves the existing rotation path
without combining it with suffix recovery. It decodes both manifest slots,
selects only the newest strict root, restores registry authority by public replay,
validates retry/catalog/seal/active/checkpoint/declaration/report evidence, and
then uses a root-aware dry scan while retaining the journal lock.

Only terminal short-prefix, invalid-exact-prefix, truncated-declared-frame, and
invalid-complete-frame-at-EOF shapes proceed. Valid post-root frames,
valid-plus-torn bytes, later candidates, sequence/identity mismatch, interior
corruption, damaged alternate authority, and ambiguity refuse unchanged. The
commit order is report publication, exact truncate plus journal sync, then
otherwise-identical Manifest V1 publication. The checkpoint is neither advanced
nor rewritten.

Complete report staging/finals and complete exact manifest staging/finals resume
narrowly, re-synchronizing complete staging before rename and a journal already
at cutoff before manifest publication. Partial or malformed staging and multiple/future/duplicate/mismatched
intent refuse. Successful clean reopen retains the latest report but performs no
new action or generation advance. Existing exact reusable retry/catalog cleanup
continues, and report cleanup is StoreId-matched, canonical, bounded, and strictly
older only.

## Diagnostics and evidence

`RecoveryReport` exposes only bounded immutable generations, active floor/cutoff,
original length, removed bytes, classification, and action. Store errors retain
their exact variants and add a path/content-free classification accessor. Store
inspection, worker readiness/shared inspection, and runtime inspection forward
the latest report. It is the latest durable event, not proof that the event
happened during the current open. Successful recovery remains `Healthy`; latest
state still restarts empty.

Focused evidence covers primitive-only report/present-manifest bytes, hostile
codec inputs, all closed suffix classes, generation-one and rotated recovery,
retry/registry/catalog preservation, report history through rotation, idempotent
reopen, valid/ambiguous suffix refusal with exact directory equality, repeated
report progression/cleanup, publication/truncate/manifest interruption points,
and runtime forwarding.

## Deferred successors

M02-PR03b disk pressure and degraded operation remain next. Stale-restore custody,
broad repair, migration, destructive reset, valid-suffix policy, final native
segments, query, manifest-backed latest reconstruction, retention/reclamation,
adapters/providers, and broader platform guarantees remain absent.
