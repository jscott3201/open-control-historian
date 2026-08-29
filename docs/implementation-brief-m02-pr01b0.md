# M02-PR01b0 implementation brief: Journal V1 semantic frames

## Outcome

Create one native/default-member `och-store` crate that depends only on
`och-core` and establishes explicit Journal V1 semantic bytes for complete
already-authorized `CanonicalAdmission` values. The same crate must bound and
validate hostile decode into structurally complete non-authorizing inspection
records.

This is the byte-format authority transition split ahead of the active-journal
vertical. It changes no core semantics and adds no filesystem or durability API.

## Required contract

- fixed versioned store-scoped header and independently framed admissions;
- canonical big-endian fields, positive append sequence, explicit payload length,
  fixed 8 MiB ceiling, configurable lower decode ceiling, and CRC-32C;
- lossless traversal of every public canonical admission field;
- pre-allocation validation of outer payload length, inner lengths, and counts;
- closed parsing of every tag and exact structural relationship;
- decoded declaration/admission mirrors that cannot authorize, bind, mutate a
  registry, or become runtime commands;
- exact byte literals, deterministic re-encode, every value family, important
  optionals, observed/gap-only/no-change, maximum counts, hostile corruption,
  truncation, and a primitive-only independent byte oracle.

## Excluded authority

No path, filesystem, file open/create, append, parser streaming state,
synchronization, lock, blocking thread, group commit, barrier, durable cutoff,
receipt, registry bootstrap/persistence, manifest, rotation, reopen, recovery,
corruption policy, full-disk behavior, priority/byte admission, runtime edge, or
platform qualification belongs in PR01b0. No new third-party dependency or
`och-core` change is allowed.

M02-PR01b1 remains the first complete active-journal durable vertical. It must
use this framing inside the sole writer path rather than expose a callable
parallel journal implementation.
