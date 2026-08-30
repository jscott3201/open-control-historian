# Manifest V1/V2 and registry snapshot format

M02-PR02a introduced Manifest V1 as the committed description of the
generation-one active Journal V1 range, mechanical checkpoint cutoff, and
complete canonical registry. M02-PR02b preserves those exact V1 bytes and adds
Manifest V2 as the root of one bounded Retry State V1 snapshot. All integers are
big-endian. Every checksum uses the Journal V1 CRC-32C parameters.

## Ownership and fixed inventory

One mutable open retains both `store-v1.lock`, which is never renamed, and the
existing active-journal lock. The stable lock is acquired before manifest
selection or mutation. The nonrecursive inventory is bounded at exactly 14
recognized entries:

- `store-v1.lock`;
- `active-journal-v1.och` and `active-journal-v1.checkpoint`;
- `manifest-v1-slot-0.och` and `manifest-v1-slot-1.och`;
- `series-registry-v1-slot-0.och`, `-1.och`, and `-2.och`;
- `retry-state-v1-slot-0.och`, `-1.och`, and `-2.och`;
- `manifest-v1.staging`, `series-registry-v1.staging`, and
  `retry-state-v1.staging`.

Unknown files, non-files, excessive entries, invalid nonzero artifacts, and
present staging artifacts fail closed. Errors expose operation, standard error
kind, and optional OS error only; paths are not public evidence. Open performs a
read-only bounded inventory pass before lock creation, then repeats it under the
stable lock. A newly created lock entry is directory-synchronized before
readiness.

The two manifest, three registry, and three retry finals are reusable bounded
slots. A registry or retry candidate can replace only a slot unreferenced by
both valid manifests. A new manifest replaces only the older alternate while an
independently valid current manifest remains. After a manifest commits, retry
slots not referenced by either valid manifest are removed and that removal is
directory-synchronized; broad repair of interrupted evidence remains absent.

## Active-header compatibility fence

Manifest stores require active-header version 2 while retaining the exact
28-byte Journal V1 header layout. Version 2 changes only bytes 8..10 from
unsigned `1` to unsigned `2`; magic, length, and `StoreId` are unchanged. Every
Journal V1 admission frame remains version 1 and byte-for-byte unchanged. The
old `JournalHeaderV1` decoder rejects version 2, so a PR01b1 binary fails closed
after upgrade. A PR02a binary likewise rejects Manifest V2 or the expanded retry
artifact inventory.

Under the stable lock and retained journal lock, premanifest bootstrap accepts a
valid V1 or V2 journal header. A nonempty recovered journal requires an exact
bounded caller-supplied `SeriesRegistrySnapshot`; an exact header-only store may
bootstrap an empty registry. Public core replay must reconstruct the snapshot
exactly, and every recovered declaration must resolve from that history before a
V1 header is rewritten and synchronized as V2. An interrupted V2/no-manifest
store requires the same proof.

## Shared manifest fields

Each manifest slot is exactly 128 bytes. V1 and V2 share bytes 0..92:

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHMAN01` |
| 8 | 2 | version | unsigned `1` or `2` |
| 10 | 2 | record length | unsigned `128` |
| 12 | 16 | store identity | validated UUIDv7 bytes |
| 28 | 8 | manifest generation | positive |
| 36 | 8 | journal generation | exactly `1` |
| 44 | 8 | checkpoint generation | positive mechanical generation |
| 52 | 8 | durable append sequence | zero only at genesis |
| 60 | 8 | durable end offset | exact checkpoint frame boundary |
| 68 | 1 | registry slot | `0..3` |
| 69 | 3 | reserved | zero |
| 72 | 8 | registry generation | positive |
| 80 | 8 | registry artifact length | `1..64 MiB` |
| 88 | 4 | registry artifact checksum | CRC-32C over complete artifact bytes |

Manifest V1 requires bytes 92..124 to be zero. Manifest V2 assigns them as:

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 92 | 1 | retry slot | `0..3` |
| 93 | 3 | reserved | zero |
| 96 | 8 | retry generation | positive |
| 104 | 8 | retry artifact length | `1..2 MiB` |
| 112 | 4 | retry artifact checksum | CRC-32C over complete artifact bytes |
| 116 | 8 | reserved | zero |
| 124 | 4 | manifest checksum | CRC-32C over bytes 0..124 |

Manifest V1 also uses the checksum at bytes 124..128. Its original bytes and
decoder remain unchanged. V2 requires the referenced retry artifact to match
slot, generation, length, checksum, `StoreId`, and configured capacities exactly.

Genesis has manifest, registry, and retry generation one and publishes an empty
Retry State V1 snapshot before Manifest V2. With two manifest slots, the current
candidate is the greater of exactly consecutive generations. A lone manifest is
accepted only at generation one. Equal, skipped, invalid, or ambiguous nonzero
candidates refuse. Manifest sequences may remain V1, transition once from V1 to
retry generation one, preserve one exact retry reference across registry-only
commits, or advance that reference by exactly one generation to a different
slot. V2 cannot regress to V1. The selected cutoff must equal the active
checkpoint exactly.

A legacy valid Manifest V1 remains openable with empty in-memory retry tiers and
no retry reference. Open does not scan or backfill its retained Journal V1
history. Registry-only commits may remain V1. The first newly durable append
publishes retry generation one and Manifest V2; after that transition every
manifest preserves a retry reference.

## Registry snapshot

The registry header is 64 bytes, followed by its declared payload and a four-byte
CRC-32C over header plus payload:

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHREG01` |
| 8 | 2 | version | unsigned `1` |
| 10 | 2 | header length | unsigned `64` |
| 12 | 16 | store identity | manifest `StoreId` |
| 28 | 8 | registry generation | positive |
| 36 | 4 | maximum series | at most `4,096` |
| 40 | 4 | maximum revisions | at most `16,384` |
| 44 | 4 | retained series count | within configured limit |
| 48 | 4 | retained revision count | within configured limit |
| 52 | 8 | payload length | exact remaining bytes before CRC |
| 60 | 4 | reserved | zero |

Payload histories are ordered by ascending `SeriesId`. Each history stores the
16-byte series identity, a positive `u32` declaration count, declarations in
ascending revision order using the frozen Journal V1 declaration grammar, then
one retirement tag. Tag zero ends the history; tag one is followed by the
retired revision and frozen declaration-evidence grammar. Unknown tags, wrong
scope, noncanonical order, count mismatch, trailing bytes, or a snapshot that
does not re-encode exactly after public core replay is invalid.

Open compares configured and manifest `StoreId` with every decoded registry and
retry artifact, including recognized unreferenced slots, before authority can be
adopted. Counts, lengths, hard maxima, and artifact ceilings are checked before
allocation. Tombstones and every historical declaration count toward limits;
nothing is evicted.

Every retry artifact referenced by either valid manifest candidate is validated
under that exact owning commit. Reference, cutoff, embedded outcome commits,
contiguous retained suffix, full-replay-before-guard shape, and retry-generation
progression must all be reachable from the publication law. Internal checksum
and canonical re-encoding alone are insufficient.

## Commit and failure order

A registry lifecycle mutation is applied to a replayed candidate authority. If
core accepts and changes it, the registry staging file is exclusively created,
bounded-written, synchronized, read back, decoded/replayed, exact-compared,
renamed over an unreferenced registry slot, and directory-synchronized before a
new manifest is published. The manifest preserves the current optional retry
reference.

Append validation remains historical: its declaration must exactly equal
`SeriesRegistry::resolve(series, revision)`. New bindings use the current active
registry. Unknown or altered historical authority fail-stops before durable
success; decoded records never authorize registry state.

For an append barrier, durable order is:

1. synchronize journal and alternate checkpoint;
2. refuse the caller-supplied pending range in constant time unless its positive
   count is at most 4,096 and exactly equals the append-sequence delta, then
   derive the FIFO retry candidate and anticipated manifest commit;
3. count, write, synchronize, read back, decode, and exact-compare Retry State V1;
4. rename it over a retry slot unreferenced by both manifests and synchronize the
   directory;
5. publish Manifest V2 naming the cutoff, registry, and retry snapshot;
6. clean now-unreferenced retry slots and synchronize that removal;
7. install the immutable retry projection and complete all covered receipts in
   one bounded runtime transition, then wake waiters.

A write, artifact-sync, readback, rename, directory-sync, transition refusal,
generation/slot exhaustion, cleanup failure, or any other error after journal
sync advances the cutoff returns no durable success and terminally faults that
live writer. A
failure after rename can leave a valid final slot, but this slice performs no
broad fallback, convergence, or repair. There is no mutable retry handle, second
queue, decoded-history backfill, or raw manifest/path/descriptor API.

## Deliberate limits

This slice adds only the bounded two-tier retry projection specified in
[Retry State V1](retry-state-v1-format.md). Latest remains volatile and empty
after reopen. Rotation/sealing, successor journal generations, immutable
segments, broad recovery/convergence, disk-pressure policy, query, rollups,
retention, and an unbounded or time-based retry horizon remain separate
successors recorded in the [PR02b continuation](continuation-m02-pr02b.md).
