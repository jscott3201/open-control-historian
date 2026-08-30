# Retry State V1/V2 format and horizon law

M02-PR02b defines one dependency-free, manifest-rooted, durable retry
projection. It preserves the exact `och-core::RetryQualification` comparison and
adds only a bounded persistence policy: a replayable outcome tier followed by a
non-replayable expired/conflict guard. The sole blocking `ManifestStore` is the
mutator; ingress receives immutable committed snapshots. M02-PR02c preserves
exact V1 bytes and decode while adding V2 only when retained replay evidence
needs journal-generation, sequence-floor, and catalog identity.

All integers are big-endian. The complete artifact is capped at 2 MiB and uses
the Journal V1 CRC-32C parameters. Configured replay and guard capacities are
both positive and their sum cannot exceed 4,096.

## Header

The 64-byte header is followed by the exact declared payload and a four-byte
CRC-32C over header plus payload:

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHRET01` |
| 8 | 2 | version | unsigned `1` or `2` |
| 10 | 2 | header length | unsigned `64` |
| 12 | 16 | store identity | manifest `StoreId` |
| 28 | 8 | snapshot generation | positive |
| 36 | 4 | replay capacity | positive |
| 40 | 4 | guard capacity | positive; combined bound `4,096` |
| 44 | 4 | replay count | at most replay capacity |
| 48 | 4 | guard count | at most guard capacity |
| 52 | 8 | payload length | exact bytes before the trailing CRC |
| 60 | 4 | reserved | zero |

The reusable slot is supplied by the filename and Manifest V2 reference rather
than duplicated in the header. Slot, generation, artifact length, complete-byte
checksum, `StoreId`, and configured capacities must all match the manifest/open
configuration exactly.

## Frozen qualification grammar

Every entry starts with the same exact retry grammar already carried inside a
Journal V1 admission:

1. 16-byte `SeriesId`;
2. 16-byte `ProducerId`;
3. retry key as a big-endian `u32` byte length followed by exact UTF-8 bytes;
4. content format as a big-endian `u32` byte length followed by exact bytes;
5. content version as a `u128`;
6. exact 32-byte SHA-256 identity.

The existing core constructors validate all nominal identities, strings, and
content. Retry State V1/V2 does not normalize, derive, hash, or reinterpret them.

## Payload entries

The payload stores exactly `replay count` replay entries first, followed by
exactly `guard count` guard entries. One replay entry appends these fields to its
qualification:

| Length | Field | Contract |
| ---: | --- | --- |
| 8 | original append sequence | positive writer-assigned sequence |
| 8 | original frame end offset | positive exact append identity |
| 8 | original manifest generation | positive first covering manifest |
| 1 | original registry slot | `0..3` |
| 7 | reserved | zero |
| 8 | original registry generation | positive |
| 8 | journal generation | exactly `1` in V1; in V2, `1` when catalog identity is absent or the positive catalog-covered successor when present |
| 8 | checkpoint generation | positive |
| 8 | committed append sequence | covers the original append |
| 8 | committed end offset | covers the original frame end |
| 1 | original retry slot | `0..3` |
| 7 | reserved | zero |
| 8 | original retry generation | positive and not newer than this snapshot |

That 88-byte fixed suffix is the complete public `ManifestCommit` needed to
reconstruct the original handled and durable receipt. It intentionally carries
only the retry slot and generation, not retry artifact length/checksum. An
outcome can therefore identify the snapshot that first made it durable without
checksum recursion.

V2 appends exactly 48 bytes after that unchanged suffix for every replay entry:

| Length | Field | Contract |
| ---: | --- | --- |
| 8 | active exclusive sequence floor | zero for legacy generation one; positive for successors |
| 1 | catalog-present tag | `0` or `1` |
| 1 | catalog slot | zero when absent; otherwise `0..3` |
| 6 | reserved | zero |
| 8 | catalog generation | zero when absent; otherwise positive |
| 8 | catalog artifact length | zero when absent; otherwise positive and bounded |
| 4 | catalog complete-byte checksum | zero when absent |
| 12 | reserved | zero |

V2 extends every retained entry so one snapshot has a uniform fixed suffix.
Legacy outcomes retain zero floor and absent catalog exactly; successor outcomes
retain the full `GenerationCatalogReference` from their original commit. Encoding
selects V1 when no replay outcome has catalog identity and V2 otherwise. It never
rewrites an embedded commit merely to migrate formats.

A guard entry appends only the original positive append sequence, eight fixed
bytes total. It retains exact classification and FIFO order but no frame-end or
replayable receipt evidence.

Within each tier, append sequences increase strictly. Guard entries are
chronologically older than every replay entry even though replay entries occur
first on the wire. No scope/key may occur twice within or across tiers, including
changed-content duplicates. Each replay outcome must be covered by its retained
commit and scoped to the snapshot store and generation history. Trailing bytes,
nonzero reserved bytes, impossible counts or lengths, invalid ordering, bad
coverage, foreign scope, or a noncanonical re-encoding refuse.

The owning Manifest V2/V3 is also part of canonical decoding. Its retry reference
must equal the artifact slot/generation. Every retained append is at or below the
owning cutoff and the newest replay exactly reaches that cutoff and names that
retry reference. The retained guard-then-replay chronology is one contiguous
suffix; a nonempty guard requires a full replay tier. All outcomes from one
retry generation carry one exact commit, and transitions between retained retry
generations advance generation, slot, checkpoint, append, and end-offset evidence
exactly as publication does. Across journal generations, append sequence remains
globally contiguous while offsets and checkpoint generations restart and compare
only within one generation. Older outcomes must be covered by the exact sealed
range in the owning Manifest V3 catalog. Embedded manifest, registry, checkpoint, cutoff, or
retry evidence cannot be newer than the owning root. An empty snapshot is
reachable only at retry generation one; later retry generations are created only
by nonempty append batches.

Encoding first traverses the already bounded entries with a counting encoder,
then rejects any total above 2 MiB before payload allocation. Decode checks the
complete artifact ceiling, exact declared length, capacities, and counts before
allocating entry vectors. A successful decode must re-encode to identical bytes.
Caller-supplied pending evidence is refused in constant time unless its count is
positive, no greater than 4,096, and exactly spans the new append-sequence suffix;
only then may transition code inspect entries or copy the already-bounded tiers.

## FIFO horizon and classification

On a durable append batch, outcomes enter the replay tier in append-sequence
order and share the exact first manifest commit covering that batch. Replay
overflow promotes the oldest replay entry to the guard. Guard overflow evicts
the oldest guard. A scope/key becomes fresh only after it has left both tiers.
There is no clock, TTL, LRU, filesystem ordering, or hash-derived identity, and
replay/conflict/expired hits never refresh position.

Classification calls `RetryQualification::classify` exactly:

- equivalent replay entry: return `SubmissionDisposition::Replayed` with an
  immediately terminal receipt containing the original append identity and
  exact original `DurableCommit`;
- changed content for a replay or guard scope/key: return `RetryConflict` and
  retain the submitted command;
- equivalent guard entry: return typed `RetryExpired` and retain the command;
- absent from both tiers: treat the scope/key as fresh.

Replay makes no current/latest-state claim and performs no latest publication.
Ingress precedence remains closed, store mismatch, Journal V1 measurement,
outstanding 16-slot retry, durable replay, guard, count capacity, then byte
capacity. Outstanding equivalents continue sharing the original receipt until
one mutex-held batch transition verifies and installs the committed retry
snapshot, resolves all covered receipts, releases reservations, and then wakes
waiters.

## Publication and compatibility

New-store genesis publishes and verifies an empty generation-one Retry State V1
snapshot before Manifest V2. Every append barrier publishes the next retry
generation to a slot unreferenced by both valid manifests, then publishes
Manifest V2 or V3 naming it. Registry-only commits preserve the existing retry
reference. The empty successor V3 created by rotation also preserves the existing
V1 snapshot/reference because no retry outcome changed; the first successor
append emits V2 when its retained outcomes span generations.

Open applies the full owning-root law to every referenced artifact from both
valid manifest candidates, not only the selected current manifest. A
checksummed, internally decodable artifact therefore still refuses when its
outcomes or tier shape could not have been produced under that manifest root.

A legacy Manifest V1 has no retry reference and restores empty tiers. No retained
Journal V1 history is scanned or backfilled; pre-PR02b keys keep the former
no-restart-horizon contract until new V2 completions establish entries. Invalid,
foreign, options-mismatched, staged, or unrelated retry artifacts refuse
strictly. Narrow post-rotation convergence may remove one older valid predecessor
slot only after the committed V3 root and its retry/catalog evidence are verified.
Broad fallback, repair, time-based expiry, and unbounded retry history remain
outside this format.
