# Manifest V1 and registry snapshot format

M02-PR02a makes one bounded manifest the committed description of the still
generation-one active Journal V1 range, its mechanical checkpoint cutoff, and
the complete persisted canonical series registry. All integers are big-endian.
All checksums use the Journal V1 CRC-32C parameters. The fixed inventory is read
non-recursively and bounded before artifact allocation.

## Ownership and artifact inventory

One mutable open retains both `store-v1.lock`, which is never renamed, and the
existing active-journal lock. The stable lock is acquired before manifest
selection or mutation. A manifest store recognizes only:

- `store-v1.lock`;
- `active-journal-v1.och` and `active-journal-v1.checkpoint`;
- `manifest-v1-slot-0.och` and `manifest-v1-slot-1.och`;
- `series-registry-v1-slot-0.och`, `-1.och`, and `-2.och`;
- `manifest-v1.staging` and `series-registry-v1.staging`.

Unknown files, non-files, excessive entries, invalid nonzero artifacts, and
present staging artifacts fail closed. Errors expose operation, standard error
kind, and optional OS error only; paths are not public evidence.

Open first performs this bounded inventory pass read-only. Only a qualifying
inventory may gain `store-v1.lock`; after acquisition, a second pass under the
lock closes the selection race. A newly created lock entry is directory-synced
before later artifact selection or readiness.

The two manifest and three registry finals are explicitly reusable bounded
slots. They are not immutable artifact names. A registry candidate can replace
only a slot unreferenced by both valid manifests. A new manifest replaces only
the older alternate while an independently valid current manifest remains.

## Active-header compatibility fence

Manifest stores require active-header version 2 while retaining the exact
28-byte Journal V1 header layout. Version 2 changes only bytes 8..10 from
unsigned `1` to unsigned `2`; magic, length, and `StoreId` are unchanged. Every
Journal V1 admission frame remains version 1 and byte-for-byte unchanged. The
old `JournalHeaderV1` decoder rejects version 2, so a PR01b1 binary fails closed
after upgrade instead of writing without manifest authority.

Under the stable lock and then the retained journal lock, premanifest bootstrap
accepts a valid V1 or V2 header. A nonempty recovered journal requires an exact
bounded caller-supplied `SeriesRegistrySnapshot`; an exact header-only store may
bootstrap an empty registry. The snapshot is restored by replaying public core
`register`, `revise`, and `retire` operations and must compare exactly after
replay. Every recovered declaration must resolve exactly from that restored
history before the V1 header is rewritten and synchronized as V2. An interrupted
V2/no-manifest store requires the same proof.

## Manifest record

Each manifest slot is exactly 128 bytes:

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHMAN01` |
| 8 | 2 | version | unsigned `1` |
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
| 88 | 4 | registry artifact checksum | CRC-32C over its complete bytes |
| 92 | 32 | reserved | zero |
| 124 | 4 | manifest checksum | CRC-32C over bytes 0..124 |

Genesis has manifest and registry generation one. With two slots, the current
candidate is the greater of exactly consecutive generations. A lone manifest is
accepted only at generation one. Equal, skipped, invalid, or ambiguous nonzero
candidates refuse; PR02a performs no broad fallback or repair. The selected
cutoff must equal the active checkpoint's mechanical cutoff exactly.

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
| 44 | 4 | retained series count | within configured series limit |
| 48 | 4 | retained revision count | within configured revision limit |
| 52 | 8 | payload length | exact remaining bytes before CRC |
| 60 | 4 | reserved | zero |

Payload histories are ordered by ascending `SeriesId`. Each history stores the
16-byte series identity, a positive `u32` declaration count, declarations in
ascending revision order using the frozen Journal V1 declaration grammar, then
one retirement tag. Tag zero ends the history; tag one is followed by the
retired declaration revision and the frozen declaration-evidence grammar.
Unknown tags, wrong store/series scope, noncanonical order, count mismatch,
trailing bytes, or a snapshot that does not re-encode exactly after public core
replay is invalid.

Open compares the configured and manifest `StoreId` with every decoded registry
artifact, including referenced and recognized unreferenced slots, before any
snapshot can be adopted. A canonically encoded foreign-store snapshot therefore
refuses as `StoreMismatch` even when its manifest reference and checksum are
otherwise valid and the recovered journal is empty.

Counts, lengths, hard maxima, and the 64 MiB artifact ceiling are checked before
allocation or traversal. Tombstones and every historical declaration count
toward the persisted limits; nothing is evicted.

## Commit and failure order

A registry lifecycle mutation is applied to a replayed candidate authority. If
core accepts and changes it, publication is:

1. create the fixed registry staging name exclusively;
2. bounded write, synchronize, read back, decode, replay, and exact-compare;
3. rename over a registry slot unreferenced by both manifests;
4. synchronize the directory;
5. stage, synchronize, independently decode, and rename the next manifest over
   only the older alternate slot;
6. synchronize the directory, adopt the candidate authority, then return.

Append validation is historical: its declaration must exactly equal
`SeriesRegistry::resolve(series, revision)`. It never calls `bind`. New envelope
bindings use the current active registry through `SeriesRegistry::bind`.
Because registry history exists only on the blocking writer, synchronous ingress
acceptance covers resource and framing bounds but cannot perform this comparison.
An unknown or altered historical declaration is an intentional terminal
authority refusal: no append/publication/durable success is reported, both
receipt stages resolve `WriterStopped`, and the runtime fail-stops. A typed
per-command rejection would require a separately reviewed receipt/API transition
and is not silently introduced here.

Durable admission order is append, volatile publication, journal sync,
checkpoint sync, manifest publication naming that exact cutoff and registry
slot, then durable receipt and reservation release. A publication error reports
no success and terminally faults that live writer. PR02a reopen deliberately
refuses a checkpoint/manifest cutoff mismatch; convergence is owned by PR03a.

Registry lifecycle and bind requests share the runtime's one bounded control
gate with the append-to-publication handshake and the sole blocking writer. A
fixed 16-permit nonblocking admission precedes that gate for lifecycle and bind
requests. Each accepted permit is held through one writer response and is
released by completion, error, or cancellation; excess requests receive typed
`RegistryError::Capacity` without joining the mutex waiter population. There is
no second queue, mutable registry handle, decoded-record authority, or raw
manifest/path/descriptor API.

## Deliberate limits

PR02a persists existing `StoreId`, declaration `ProducerId`, and per-record
producer epochs only. It adds no store-global producer epoch. Latest remains
volatile and empty after reopen. Durable retry, rotation/sealing, immutable
segments, broad recovery, disk-pressure policy, queries, and retention are
separate successors recorded in the PR02a continuation.
