# Manifest V1 and registry snapshot format

The durable-format reset defines one current manifest layout. Manifest V1 is
exactly 160 bytes, uses big-endian integers, and is the committed root for the
active Journal V1 cutoff, complete registry snapshot, mandatory Retry State V1
snapshot, optional current Generation Catalog V1, and optional latest Recovery
State V1 event reference. The root Store Format V1
marker is required before any manifest can be considered. Historical 128-byte
manifests and historical records whose version field is `2` or `3` are rejected;
they are not decoded, upgraded, deleted, or used as authority.

## Fixed inventory and publication

One mutable open retains `store-v1.lock` and the active-journal lock. Before the
stable lock is created or opened, a bounded read-only inventory validates the
Store Format V1 marker and current manifest, active-header, and retry versions.
The inventory is capped at 91 recognized files: the prior bounded current
inventory plus three Recovery State V1 finals and one staging name. Unknown files,
non-files, excessive entries, markerless nonempty directories, malformed markers,
and historical or mixed durable formats return the path-free
`UnsupportedStoreFormat` refusal without mutation.

After preflight, validation is repeated while the stable lock is held. Only exact
current marker/genesis publication, the existing narrow rotation transaction, and
the current-V1 terminal-suffix recovery transaction can converge. Current
postcommit reusable-slot cleanup remains permitted.

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
| 116 | 1 | recovery presence | `0` absent or `1` present |
| 117 | 1 | recovery slot | `0..2` when present; zero when absent |
| 118 | 2 | reserved | zero |
| 120 | 4 | recovery artifact CRC-32C | complete Recovery State V1 bytes; zero when absent |
| 124 | 8 | active exclusive sequence floor | zero at generation one; otherwise positive |
| 132 | 1 | catalog slot | `0..2`, or zero when absent |
| 133 | 3 | reserved | zero |
| 136 | 8 | catalog generation | positive when present; otherwise zero |
| 144 | 8 | catalog artifact length | positive when present; otherwise zero |
| 152 | 4 | catalog artifact CRC-32C | complete artifact bytes; otherwise zero |
| 156 | 4 | manifest CRC-32C | bytes `0..156` |

Generation one has sequence floor zero, no catalog, and a canonically zero
catalog body. It references registry generation one and mandatory empty Retry
State V1 generation one. Its recovery reference is canonically absent. A rotated root has a positive floor and an exact
Generation Catalog V1 reference. An empty successor's sequence and cutoff equal
its floor and its end offset is the 28-byte header boundary.

Recovery absence is the all-zero eight-byte body, not a legacy branch. Unknown
presence tags, absent nonzero bodies, present invalid slots, or nonzero reserved
bytes refuse. Ordinary append, registry, retry, and rotation manifests preserve
the latest recovery reference exactly.

With two slots, the current candidate is the greater of exactly consecutive
manifest generations. A lone manifest is accepted only at generation one.
Registry, retry, and catalog references either remain byte-for-byte equal or
advance by exactly one generation into another slot under their respective
publication transition. Every referenced artifact must match slot, generation,
length, checksum, store scope, configured limits, and canonical content.

For retained consecutive manifests, recovery references progress only absent to
absent, exact same present reference to itself, absent to report generation one,
or report generation `R` to a different-slot `R+1`. Present to absent refuses.
A changed report reference requires an otherwise byte-identical authority record:
only manifest generation, recovery reference, and manifest CRC may differ. The
report names the older source generation/checksum and exact newer committing
generation. Slots referenced by either retained manifest cannot be reused.

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

A failure after journal synchronization returns no false durable success. Typed
storage pressure puts the live writer in sticky reopen custody; other mutation
failures retain terminal fault behavior. Latest remains volatile and restarts empty.

Recovery first validates both manifests and every registry, retry, catalog/seal,
active/checkpoint, declaration, inventory, and referenced report relationship.
It then publishes/verifies an unreferenced Recovery State slot, truncates and
synchronizes only the proven terminal suffix, and publishes the otherwise
same-layout next manifest last. Complete report/manifest staging and finals are
resumed only when exact expected bytes prove the next transaction; malformed,
duplicate, future, or mismatched evidence refuses unchanged. A second clean open
does not advance either generation. Broad repair, migration, destructive reset,
native segments, reclamation, retention, stale-restore custody, and disk-pressure
runtime policy remain outside this contract. Store-only typed pressure evidence
and volatile reopen custody do not alter Manifest V1 bytes or authority.
