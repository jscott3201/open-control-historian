# Manifest V1 and registry snapshot format

The durable-format reset defines one current manifest layout. Manifest V1 is
exactly 160 bytes, uses big-endian integers, and is the committed root for the
active Journal V1 cutoff, complete registry snapshot, mandatory Retry State V1
snapshot, and optional current Generation Catalog V1. The root Store Format V1
marker is required before any manifest can be considered. Historical 128-byte
manifests and historical records whose version field is `2` or `3` are rejected;
they are not decoded, upgraded, deleted, or used as authority.

## Fixed inventory and publication

One mutable open retains `store-v1.lock` and the active-journal lock. Before the
stable lock is created or opened, a bounded read-only inventory validates the
Store Format V1 marker and current manifest, active-header, and retry versions.
The inventory is capped at 87 recognized files: the prior bounded current
inventory plus `store-format-v1.och` and `store-format-v1.staging`. Unknown files,
non-files, excessive entries, markerless nonempty directories, malformed markers,
and historical or mixed durable formats return the path-free
`UnsupportedStoreFormat` refusal without mutation.

After preflight, validation is repeated while the stable lock is held. Only exact
current marker/genesis publication and the existing narrow rotation transaction
can converge. Current postcommit reusable-slot cleanup remains permitted.

The two manifest, three registry, three retry, and three catalog finals are
bounded reusable slots. Candidates are exclusively staged, synchronized, read
back and canonically decoded, renamed over an unreferenced slot, and followed by
a same-directory sync. A manifest is published last as the commit point.

## Exact Manifest V1 bytes

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHMAN01` |
| 8 | 2 | version | unsigned `1` |
| 10 | 2 | record length | unsigned `160` |
| 12 | 16 | store identity | exact Store Format V1 `StoreId` |
| 28 | 8 | manifest generation | positive |
| 36 | 8 | active journal generation | positive |
| 44 | 8 | checkpoint generation | positive |
| 52 | 8 | durable append sequence | zero only at genesis |
| 60 | 8 | durable end offset | exact checkpoint frame boundary |
| 68 | 1 | registry slot | `0..2` |
| 69 | 3 | reserved | zero |
| 72 | 8 | registry generation | positive |
| 80 | 8 | registry artifact length | bounded and positive |
| 88 | 4 | registry artifact CRC-32C | complete artifact bytes |
| 92 | 1 | retry slot | `0..2` |
| 93 | 3 | reserved | zero |
| 96 | 8 | retry generation | positive |
| 104 | 8 | retry artifact length | bounded and positive |
| 112 | 4 | retry artifact CRC-32C | complete artifact bytes |
| 116 | 8 | reserved | zero |
| 124 | 8 | active exclusive sequence floor | zero at generation one; otherwise positive |
| 132 | 1 | catalog slot | `0..2`, or zero when absent |
| 133 | 3 | reserved | zero |
| 136 | 8 | catalog generation | positive when present; otherwise zero |
| 144 | 8 | catalog artifact length | positive when present; otherwise zero |
| 152 | 4 | catalog artifact CRC-32C | complete artifact bytes; otherwise zero |
| 156 | 4 | manifest CRC-32C | bytes `0..156` |

Generation one has sequence floor zero, no catalog, and a canonically zero
catalog body. It references registry generation one and mandatory empty Retry
State V1 generation one. A rotated root has a positive floor and an exact
Generation Catalog V1 reference. An empty successor's sequence and cutoff equal
its floor and its end offset is the 28-byte header boundary.

With two slots, the current candidate is the greater of exactly consecutive
manifest generations. A lone manifest is accepted only at generation one.
Registry, retry, and catalog references either remain byte-for-byte equal or
advance by exactly one generation into another slot under their respective
publication transition. Every referenced artifact must match slot, generation,
length, checksum, store scope, configured limits, and canonical content.

## Registry snapshot

Registry Snapshot V1 remains a 64-byte fixed header, bounded payload, and
four-byte CRC-32C over header plus payload. Its magic is `OCHREG01`, version is
`1`, header length is `64`, and it carries exact store identity, positive
generation, configured series/revision bounds, retained counts, exact payload
length, and zero reserved bytes. Histories are ordered by `SeriesId`; declarations
are ordered by revision and use the frozen Journal V1 declaration grammar. Public
`SeriesRegistry` replay must reproduce the snapshot exactly. Decoded journal
records never authorize registry state.

## Commit and failure order

An ordinary durable append synchronizes the journal and checkpoint, constructs
and publishes the exact next Retry State V1 snapshot, then publishes Manifest V1
naming that cutoff, registry, and retry state. Only after those steps are durable
may runtime durable receipts complete. Rotation then publishes the intent, raw
Journal V1 seal, empty successor, Generation Catalog V1, and alternate Manifest
V1 last before adopting the successor and narrowly cleaning redundant artifacts.

A failure after journal synchronization returns no false durable success and
terminally faults that live writer. Latest remains volatile and restarts empty.
Broad recovery, migration, destructive reset, native segments, reclamation,
retention, and disk-pressure policy remain outside this contract.
