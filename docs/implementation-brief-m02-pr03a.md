# M02-PR03a current-V1 conservative recovery implementation brief

## Objective

Add one manifest-rooted recovery transaction without changing Manifest V1's
version or 160-byte length, Retry State V1 bytes, Journal V1 bytes, checkpoint
authority, or `och-core`. Automatic recovery may remove only one proven terminal
invalid/torn suffix strictly beyond the selected current manifest cutoff.

## Authority transition

- Reassign Manifest V1 bytes `116..124` from zero reservation to one tagged
  optional Recovery State V1 slot/checksum reference.
- Add three reusable exact 128-byte Recovery State V1 finals and one staging name.
- Preserve the latest report reference through ordinary append, registry, retry,
  and rotation manifests.
- Require a recovery successor to change only manifest generation, report
  reference, and manifest checksum.
- Expose the latest committed report through store and runtime inspection while
  retaining `RuntimeHealth::Healthy` after success.
- Add a sanitized additive classification view on existing store errors without
  changing their exact variants.

## Transaction boundary

Preflight validates only current Store Format V1 and exact artifact
versions/bounds before stable-lock creation. Under the stable and active locks,
open resolves only the existing narrow rotation transaction, selects the newest
of strict consecutive manifest slots without fallback, and validates every
registry, retry, catalog/seal, active/checkpoint, declaration, inventory, and
report relationship before mutation.

The active owner performs a private root-aware dry scan. Complete valid post-root
frames and ambiguity refuse unchanged. For one closed terminal-invalid/torn
shape, store publishes the report, truncates and synchronizes exactly to the
unchanged cutoff, then commits the otherwise identical next manifest. Exact
staging/final crash windows converge or refuse; no intent can authorize another
authority family.

## Required evidence

- primitive-only absent/present Manifest V1 and Recovery State V1 bytes;
- hostile fixed fields, bounds, arithmetic, tags, checksum, scope, and trailing
  input;
- generation-one and rotated-active recovery with registry/retry/catalog and
  declaration preservation;
- valid/ambiguous post-root and damaged authority byte-for-byte refusal;
- report, truncate, manifest, adoption, and cleanup interruption behavior;
- ordinary manifest and rotation preservation of report identity;
- store/runtime inspection forwarding with healthy runtime status and idempotent
  clean reopen;
- unchanged Store Format V1, retry, journal, genesis, lock, latest-empty, and
  no-false-commit regressions.

## Exclusions

No migration, legacy decoder, manifest widening/version change, valid-suffix
adoption or discard, checkpoint rollback, older-root fallback, broad repair,
stale-restore custody, disk-pressure/degraded behavior, `och-core` change,
dependency, segment/query/latest reconstruction, retention/reclamation, adapter,
provider, Studio, or Engine work belongs to this slice.
