# Sealed raw Journal V1 artifact

M02-PR02c seals one exact fully durable active Journal V1 range without inventing
the final native segment format. A sealed artifact is a byte-for-byte immutable
copy of the source active journal: the existing 28-byte header-version-2 record
followed by the existing independent Journal V1 admission frames. It has no new
wrapper, index, footer, compression, dictionary, or query structure.

## Identity and naming

The deterministic final name is
`sealed-journal-v1-g{generation:020}.och`; generation one therefore becomes
`sealed-journal-v1-g00000000000000000001.och`. The sole fixed staging name is
`sealed-journal-v1.staging`. No caller text or path participates in either name.
Product APIs expose only sanitized generation/catalog facts, never a path, raw
mutable handle, or content view.

The corresponding Generation Catalog V1 entry binds:

- exact source journal generation;
- exclusive global append-sequence floor and inclusive cutoff;
- exact source durable end offset;
- registry generation covering every retained declaration;
- complete artifact byte length and CRC-32C;
- sealed format tag `1`, meaning raw Journal V1.

The artifact length equals the source durable end and cannot exceed the configured
active limit or 512 MiB hard maximum.

## Streaming publication and verification

Rotation requires a nonempty active generation whose manifest cutoff, checkpoint,
append sequence, and end offset exactly agree. `och-store` exclusively creates the
staging file and streams the exact range with a fixed 64 KiB buffer while computing
CRC-32C. It synchronizes the staging artifact, then performs bounded readback that
verifies:

- header-version-2 magic, length, and exact `StoreId`;
- strict append sequence beginning after the declared exclusive floor and ending
  at the declared inclusive cutoff;
- every Journal V1 frame length, CRC, semantic decode, and exact historical
  declaration resolution under the retained registry;
- exact end offset, complete length, and complete checksum.

Only that verified staging artifact is renamed to the deterministic final name;
the directory is synchronized and the final artifact is verified again before a
catalog or Manifest V1 can commit it. Product code never opens a published seal
for mutation and this slice never deletes one.

## Open and scope

Normal committed open uses bounded Generation Catalog V1 bytes plus each sealed
file's metadata and 28-byte header. It deliberately does not scan every sealed
payload byte. The seal is retained durability evidence for retry and future
segment work, not a queryable immutable segment. Query/read APIs, final native
segment encoding, indexes, retention, reclamation, corruption repair, and broad
recovery remain separate successors.

Publication uses safe standard-library file operations: same-directory exclusive
staging creation, file synchronization, rename, and directory synchronization.
The claim is scoped to supported local filesystem behavior. It does not claim
universal power-loss durability, macOS `F_FULLFSYNC`, Windows qualification,
physical free-space guarantees, or safety against an adversarial directory writer.
