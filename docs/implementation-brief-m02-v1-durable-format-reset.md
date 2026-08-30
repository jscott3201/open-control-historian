# M02 current-only V1 durable-format reset implementation brief

## Objective

Replace the historical multi-version durable compatibility matrix with one
current V1 contract for every on-disk artifact family. Fence the reset with Store
Format V1 and preserve M02-PR02c append, durability, registry, retry, rotation,
catalog/seal, receipt, bounded-open, and runtime semantics.

## Authority transition

- Publish exact `store-format-v1.och` bytes before current genesis artifacts.
- Make Manifest V1 the sole 160-byte root with mandatory registry and retry
  references and an optional current catalog reference.
- Make Retry State V1 always include its 48-byte generation/floor/catalog replay
  extension.
- Make Journal Header V1 the sole 28-byte active and sealed header while leaving
  Journal V1 admission frames byte-for-byte unchanged.
- Delete production decoders, branches, public types, and snapshot inputs whose
  only purpose was opening or upgrading historical durable formats.
- Return path-free `UnsupportedStoreFormat` for markerless, historical, malformed,
  or mixed format evidence before stable-lock creation or durable mutation.

## Opening law

Opening starts with bounded read-only inventory and marker validation. Empty
`CreateNew` may create and synchronize the stable lock and publish the marker.
Only an exact complete marker staging file with the stable lock may finish that
rename. Validation is repeated under lock before current genesis or normal open.
The marker does not authorize old artifacts; present manifest, active header, and
retry artifacts must independently prove the current layouts.

Current reusable-slot cleanup and the existing narrow rotation convergence remain
valid only after the reset fence passes. Rejected stores receive no lock creation,
cleanup, truncation, rename, deletion, or repair.

## Required evidence

- independent exact marker, Journal Header V1, Manifest V1, empty and nonempty
  Retry State V1, registry, checkpoint, catalog, and sealed-journal bytes;
- create/register/append/barrier/reopen and repeated rotation/reopen behavior;
- cross-generation retry outcomes and original receipt/commit identity;
- historical manifest, retry, header, markerless premanifest, forged-marker, and
  mixed-format refusal with before/after inventory equality;
- marker/genesis and ordinary/rotation publication fault boundaries;
- hostile version, length, scope, reserved, checksum, and capacity inputs;
- store/runtime/core tests, doctests, formatting, strict clippy, repository
  policy, and diff hygiene.

## Exclusions

No migration or destructive reset, Recovery State, broad recovery, disk-pressure
mode, final native segment, query, latest reconstruction, retention/reclamation,
adapter/provider work, dependency change, `och-core` semantic change, runtime
ordering change, or universal platform/power-loss claim belongs to this slice.
