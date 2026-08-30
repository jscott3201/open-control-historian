# Manifest V1/V2/V3/V4 and registry snapshot format

M02-PR02a introduced Manifest V1 as the committed description of the
generation-one active Journal V1 range, mechanical checkpoint cutoff, and
complete canonical registry. M02-PR02b preserves those exact V1 bytes and adds
Manifest V2 as the root of one bounded Retry State V1 snapshot. M02-PR02c
preserves the exact 128-byte V1/V2 records and adds 160-byte Manifest V3 as the
commit point for one successor active generation and Generation Catalog V1.
M02-PR03a preserves those encodings and adds 192-byte Manifest V4 as the commit
point for one bounded Recovery State V1 report. All integers are big-endian.
Every checksum uses the Journal V1 CRC-32C parameters.

## Ownership and fixed inventory

One mutable open retains both `store-v1.lock`, which is never renamed, and the
existing active-journal lock. The stable lock is acquired before manifest
selection or mutation. The nonrecursive inventory is hard-capped at 89 recognized
files. Legacy V1/V2 names remain:

- `store-v1.lock`;
- `active-journal-v1.och` and `active-journal-v1.checkpoint`;
- `manifest-v1-slot-0.och` and `manifest-v1-slot-1.och`;
- `series-registry-v1-slot-0.och`, `-1.och`, and `-2.och`;
- `retry-state-v1-slot-0.och`, `-1.och`, and `-2.och`;
- `manifest-v1.staging`, `series-registry-v1.staging`, and
  `retry-state-v1.staging`.

M02-PR02c additionally recognizes only deterministic successor active/checkpoint
pairs, three Generation Catalog V1 finals plus one staging name, up to 64 sealed
raw-Journal finals plus one staging name, and one fixed rotation-intent name. The
exact patterns are defined by [Journal V1](journal-v1-format.md),
[Generation Catalog V1](generation-catalog-v1-format.md), and
[sealed raw Journal V1](sealed-journal-v1-format.md).

M02-PR03a additionally recognizes three reusable
`recovery-state-v1-slot-{0,1,2}.och` finals and one
`recovery-state-v1.staging`. Their bytes are defined by
[Recovery State V1](recovery-state-v1-format.md).

Unknown files, non-files, excessive entries, invalid nonzero artifacts, and
present staging artifacts fail closed. Errors expose operation, standard error
kind, and optional OS error only; paths are not public evidence. Open performs a
read-only bounded inventory pass before lock creation, then repeats it under the
stable lock. A newly created lock entry is directory-synchronized before
readiness.

The two manifest and three registry, retry, catalog, and recovery finals are
reusable bounded slots. A metadata candidate can replace only
a slot unreferenced by both valid manifests. A new manifest replaces only the
older alternate while an independently valid current manifest remains. After a
manifest commits, retry and catalog slots not referenced by either prospective
valid manifest are removed and that removal is directory-synchronized. A crash
before catalog cleanup may leave only a canonically decoded strict prefix of a
referenced newer catalog; open verifies that exact relation and all root evidence
before removing it idempotently. A future, forked, unrelated, or otherwise
unreferenced catalog refuses; broad repair remains absent.

## Active-header compatibility fence

Manifest stores require active-header version 2 while retaining the exact
28-byte Journal V1 header layout. Version 2 changes only bytes 8..10 from
unsigned `1` to unsigned `2`; magic, length, and `StoreId` are unchanged. Every
Journal V1 admission frame remains version 1 and byte-for-byte unchanged. The
old `JournalHeaderV1` decoder rejects version 2, so a PR01b1 binary fails closed
after upgrade. A PR02a binary likewise rejects Manifest V2 or the expanded retry
artifact inventory; pre-PR02c binaries reject Manifest V3 and generation
artifacts. Pre-PR03a binaries reject Manifest V4 and recovery inventory.

Under the stable lock and retained journal lock, premanifest bootstrap accepts a
valid V1 or V2 journal header. A nonempty recovered journal requires an exact
bounded caller-supplied `SeriesRegistrySnapshot`; an exact header-only store may
bootstrap an empty registry. Public core replay must reconstruct the snapshot
exactly, and every recovered declaration must resolve from that history before a
V1 header is rewritten and synchronized as V2. An interrupted V2/no-manifest
store requires the same proof.

## Shared manifest fields

V1 and V2 remain exactly 128 bytes. V3 is exactly 160 bytes. V4 is exactly 192
bytes. All four share
bytes 0..92:

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHMAN01` |
| 8 | 2 | version | unsigned `1`, `2`, `3`, or `4` |
| 10 | 2 | record length | unsigned `128` for V1/V2; `160` for V3; `192` for V4 |
| 12 | 16 | store identity | validated UUIDv7 bytes |
| 28 | 8 | manifest generation | positive |
| 36 | 8 | journal generation | exactly `1` for V1/V2; greater than `1` for V3 |
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
V3 has the same mandatory retry body. Manifest V4 normally does too, but the
recovery-only successor of a legacy V1 root may retain bytes 92..124 as canonical
all-zero absence. Partially populated retry bytes are never absence and refuse.

Manifest V3 preserves bytes 0..124 with V2 retry fields and assigns the remaining
36 bytes as follows:

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 124 | 8 | active exclusive sequence floor | positive; at or below active cutoff |
| 132 | 1 | catalog slot | `0..3` |
| 133 | 3 | reserved | zero |
| 136 | 8 | catalog generation | positive |
| 144 | 8 | catalog artifact length | `1..4,164` |
| 152 | 4 | catalog complete-byte checksum | CRC-32C over complete catalog artifact |
| 156 | 4 | manifest checksum | CRC-32C over bytes 0..156 |

An empty V3 active has append sequence equal to its floor and end offset exactly
28. A nonempty active has a greater sequence and frame-boundary end. V3 binds the
exact active generation, local checkpoint generation/end, global sequence
floor/cutoff, registry, retry snapshot, and catalog. Public `ManifestCommit`
retains that floor and optional full catalog identity; legacy commits retain zero
floor and no catalog.

Manifest V4 preserves bytes 0..156 as the V2/V3 body. For an unrotated
generation-one root, bytes 124..156 are all zero; after rotation they retain the
V3 floor and catalog fields. V4 assigns its final 36 bytes as follows:

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 156 | 1 | recovery slot | `0..3` |
| 157 | 3 | reserved | zero |
| 160 | 8 | recovery generation | positive, no greater than manifest generation |
| 168 | 8 | Recovery State V1 length | exactly `96` |
| 176 | 4 | recovery complete-byte checksum | CRC-32C over all recovery bytes |
| 180 | 8 | reserved | zero |
| 188 | 4 | manifest checksum | CRC-32C over bytes 0..188 |

The V4 transition changes no registry, retry, catalog, active cutoff, sequence
floor, or embedded prior `ManifestCommit`. It advances the manifest by one and
binds the report whose source generation and cutoff equal the older manifest.
Later append, lifecycle, retry, and rotation manifests remain V4 and preserve the
reference until a later proven recovery supersedes it.

When that older root is Manifest V1, recovery preserves its absent retry
reference and empty in-memory retry tiers; it does not synthesize an empty retry
artifact or backfill outcomes. The retained V1/V4 pair proves the recovery-only
transition. Registry-only and rotation commits may continue to preserve that
absence. The first new durable append publishes retry generation one while
preserving the recovery reference. Any V4 with a retry reference follows the
unchanged strict V2/V3 reference and retry-comparison laws.

Genesis has manifest, registry, and retry generation one and publishes an empty
Retry State V1 snapshot before Manifest V2. With two manifest slots, the current
candidate is the greater of exactly consecutive generations. A lone manifest is
accepted only at generation one. Equal, skipped, invalid, or ambiguous nonzero
candidates refuse. Manifest sequences may remain V1, transition once from V1 to
retry generation one, preserve one exact retry reference across registry-only
commits, or advance that reference by exactly one generation to a different
slot. V2 cannot regress to V1. V3 requires exact successor generation/catalog
progression and cannot regress to V1/V2. Recovery references may transition
`None -> generation 1`, remain exact, or advance by exactly one into a different
slot; they never disappear. An absent retry may remain absent only through the
legacy V1 recovery path and its preserving successors; `None -> retry generation
1` remains the only establishment transition. The selected cutoff must equal the
active checkpoint exactly.

A legacy valid Manifest V1 remains openable with empty in-memory retry tiers and
no retry reference. Open does not scan or backfill its retained Journal V1
history. Registry-only commits may remain V1. The first newly durable append
publishes retry generation one and Manifest V2, or Manifest V4 when a recovery
reference must also be preserved; after that transition every manifest preserves
a retry reference.

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

Manifest slots are retained as two independent read outcomes: missing, valid,
corrupt, unsupported, identity-mismatched, or I/O-refused. Selection begins only
after both outcomes exist. A damaged, unsupported, or identity-wrong possible
newer slot refuses; an older parseable record never authorizes fallback. A lone
generation greater than one refuses. A lone generation-one root also refuses
when future unreferenced metadata proves that a registry-only or retry/catalog
successor may be missing. Equal journal cutoffs do not relax this law.

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
3. count, write, synchronize, read back, decode, and exact-compare Retry State V1/V2;
4. rename it over a retry slot unreferenced by both manifests and synchronize the
   directory;
5. publish Manifest V2/V3/V4 naming the cutoff, registry, and retry snapshot;
6. clean now-unreferenced retry slots and synchronize that removal;
7. install the immutable retry projection and complete all covered receipts in
   one bounded runtime transition, then wake waiters.

Rotation occurs only after that ordinary durable receipt batch has completed. Its
commit order is:

1. require an exact nonempty manifest/checkpoint cutoff and no unpublished append;
2. persist, synchronize, read back, and directory-sync the fixed 96-byte intent;
3. stream-build, synchronize, fully verify, rename, directory-sync, and verify
   the immutable raw-Journal seal;
4. exclusively create, synchronize, and read back the empty successor at the
   prior global sequence cutoff;
5. publish and verify the next Generation Catalog V1 slot while preserving the
   current retry snapshot when no retry semantics changed;
6. publish and directory-sync alternate Manifest V3/V4 last as the commit point;
7. adopt the successor, then narrowly remove predecessor duplicates, intent, and
   now-unreferenced catalog/retry slots.

The intent is never authority. Before the V3 commit, exact derivative candidates
are removed and the prior manifest remains authoritative. After it, catalog,
successor, registry, and retry evidence are verified under V3 before redundant
predecessor evidence is removed. Missing, mismatched, or ambiguous evidence
refuses unchanged; there is no broad fallback or repair.

The retained manifest pair independently proves every catalog advance after the
intent is gone. The appended entry must exactly describe the older manifest's
journal generation, sequence floor/cutoff, durable end and artifact length, and
registry generation. The newer root must be the next manifest generation, keep
the older registry and retry references unchanged, and name an empty checked
successor journal: generation `older + 1`, sequence floor and cutoff equal to the
older cutoff sequence, 28-byte end, and checkpoint generation `1`. This law
applies both to the first `None -> Catalog V1` transition and every later exact
one-entry catalog advance.

Conservative active recovery is a separate Manifest V4 transition. After both
manifest outcomes and every registry/retry/catalog/seal/recovery reference have
validated, active open scans against the selected manifest cutoff rather than
the permissive premanifest convergence policy. The committed prefix and exact
checkpoint slot must exist. A fully valid suffix, a torn final suffix, or one
malformed final candidate may be classified only after the root boundary;
committed/interior corruption and a malformed candidate followed by later bytes
refuse unchanged. At a zero sequence floor, the first complete frame must be
numbered exactly one before it can be classified as committed or removable;
higher-generation floors retain the exact existing successor rule. Accepted
recovery truncates and synchronizes exactly to the
manifest end, clears only a strictly newer mechanical checkpoint slot when one
exists, publishes Recovery State V1, and publishes Manifest V4 last. It never
adopts suffix records or checkpoints forward.

An interrupted recovery-state or manifest candidate remains non-authoritative.
Precommit publication faults may reopen only to a typed non-mutating
`InterruptedPublication`; a renamed valid Manifest V4 reopens to the one
report-bound root. Completed recovery does not repeat on later open.

A write, artifact-sync, readback, rename, directory-sync, transition refusal,
generation/slot exhaustion, cleanup failure, or any other error after journal
sync advances the cutoff returns no durable success and terminally faults that
live writer. A
failure after rename can leave a valid final slot, but this slice performs no
broad fallback, convergence, or repair. There is no mutable retry handle, second
queue, decoded-history backfill, or raw manifest/path/descriptor API.

## Deliberate limits

This authority now includes the bounded two-tier retry projection, raw-Journal
rotation/sealing, Generation Catalog V1, Manifest V4, and conservative
manifest-root suffix recovery. Latest remains volatile and empty after reopen.
Final native segments, sealed-history queries, destructive repair, stale-restore
acceptance, reclamation, disk-pressure/degraded policy, rollups, retention, and
an unbounded or time-based retry horizon remain separate successors.
