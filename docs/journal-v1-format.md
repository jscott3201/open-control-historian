# Journal V1 semantic format

Journal V1 is the first canonical byte representation of one already-authorized
`och_core::CanonicalAdmission`. M02-PR01b1 stores it in one bounded, locked,
generation-one active journal with a mechanical durable-high-water checkpoint.
The bytes and decoded records never grant authorization. All multibyte integers
use network byte order (big-endian). No value is inferred, normalized, generated,
compressed, dictionary-encoded, or hashed with a platform-dependent algorithm.

## Fixed active header and manifest fence

The header is exactly 28 bytes:

| Offset | Length | Field | V1 value |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHJNL01` |
| 8 | 2 | active-header version | unsigned `1` premanifest; unsigned `2` for manifest stores |
| 10 | 2 | header length | unsigned `28` |
| 12 | 16 | store identity | validated network-order UUIDv7 bytes |

Decode accepts exactly 28 bytes. Magic, selected version, header length, identity
version, identity variant, truncation, and trailing input fail closed. The V2
header changes only the version field; all admission frames remain format version
one. `JournalHeaderV1` rejects V2, so a premanifest writer fails closed after a
store is upgraded. V1/V2 bootstrap and committed publication are specified in
[Manifest V1](manifest-v1-format.md).

## Independent admission frame

Each admission is independently framed; no preceding frame state is needed to
decode its payload. The fixed prefix is 20 bytes:

| Offset | Length | Field | V1 contract |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | ASCII `OCHF` |
| 4 | 2 | format version | unsigned `1` |
| 6 | 1 | frame kind | `1` = canonical admission |
| 7 | 1 | flags | exactly zero |
| 8 | 8 | append sequence | positive unsigned integer |
| 16 | 4 | payload length | bytes following prefix, excluding CRC |
| 20 | variable | payload | at most 8,388,608 bytes |
| after payload | 4 | checksum | CRC-32C over prefix and payload |

When a previous append sequence is supplied to decode, the current value must be
its exact successor; zero, gaps, repeats, reversal, and successor overflow are
refused. `DecodeLimitsV1` may select a lower payload ceiling, including zero.
The declared length is checked against both ceilings before payload field
allocation or traversal. The frame input must end exactly after its checksum.

CRC-32C uses the Castagnoli polynomial in reflected form `0x82f63b78`, initial
register `0xffffffff`, byte-wise reflected processing, and final XOR
`0xffffffff`. The stored checksum is big-endian. The standard `123456789` check
value is `0xe3069283`.

## Active artifacts and bounds

The active generation is exactly unsigned `1`. Its mechanical state uses exactly
two files in one caller-supplied existing directory:

- `active-journal-v1.och`: one header followed by independent frames;
- `active-journal-v1.checkpoint`: exactly two 64-byte checkpoint slots.

Legacy premanifest create-new follows one exact order: exclusively create and lock the single
read/write journal, write and synchronize its header, exclusively create the
checkpoint, initialize its 128 bytes with generation-one genesis in slot zero and
an all-zero alternate slot, synchronize the checkpoint, then synchronize the
directory before readiness. Append mode is not used: every frame write seeks
explicitly to journal end on the retained locked handle.

Configured limits are checked before I/O and may only narrow these hard bounds:

| Contract | Hard V1 active bound |
| --- | ---: |
| Directory encoded path | 4,096 bytes |
| Frame payload | 8,388,608 bytes |
| Active journal including header | 536,870,912 bytes |
| Active admission records | 4,096 |

The runtime separately retains a fixed 16-command count window and explicit
finite outstanding encoded-byte limits. It computes the exact frame length with
the same canonical traversal but no frame allocation, atomically reserves count
and bytes first, then allocates/encodes and checks that the prepared length is
exact. Protected, normal, and bulk classes change reservation ceilings and
barrier demand only; append order stays FIFO.

## Durable checkpoint

Each 64-byte checkpoint slot is self-contained:

| Offset | Length | Field | V1 value |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHCP001` |
| 8 | 2 | version | unsigned `1` |
| 10 | 2 | slot length | unsigned `64` |
| 12 | 16 | store identity | Journal header `StoreId` |
| 28 | 8 | journal generation | unsigned `1` |
| 36 | 8 | slot generation | positive, strict monotonic |
| 44 | 8 | durable append sequence | zero only at genesis |
| 52 | 8 | durable end offset | exact frame boundary |
| 60 | 4 | checksum | CRC-32C over bytes 0..60 |

Slot generation one occupies slot zero; each barrier increments generation and
writes `(generation - 1) mod 2`. A barrier's required order is journal sync,
alternate checkpoint-slot write, checkpoint sync, then in-memory cutoff advance
and durable receipt resolution. Public `DurableCutoff` evidence carries this
mechanical checkpoint generation separately from the fixed journal generation.
The checkpoint is only store/journal binding and mechanical cutoff evidence. It
carries no declaration registry, retry outcome, latest state, or source
interpretation authority.

Legacy premanifest open-existing requires exact fixed artifacts and a retained writer lock. It
validates the header, checkpoint length, StoreId/generation/checksums, slot parity,
and strict consecutive generations. Two valid consecutive slots must also advance
both append sequence and end offset strictly. Any invalid or non-progressing
nonzero slot refuses; an invalid apparently newer slot never falls back to an
older valid cutoff. A lone valid slot is accepted only for generation one with an
unused zero alternate. If the checkpoint artifact is missing or exists with
exactly zero bytes, open-existing may create or initialize it only after validating
an exact header-only journal under the retained lock. A nonempty or invalid journal
without a checkpoint refuses without creating one. Every nonzero wrong checkpoint
length refuses unchanged. An existing 128-byte all-zero checkpoint has the same
header-only genesis restriction.

The scan is limited by configured payload, journal-byte, and record bounds before
allocation. Every frame StoreId and governing declaration StoreId must equal the
header. Invalid bytes inside the checkpoint cutoff refuse without mutation. A
valid suffix beyond an unambiguous cutoff is journal-synchronized and checkpointed
before readiness. An invalid unacknowledged suffix may be truncated only when it
is provably terminal; a complete malformed frame followed by any bytes or later
candidate makes recovery ambiguous and refuses without changing the file. Every
allowed truncation is synchronized before readiness. The scan never fabricates
`CanonicalAdmission`, registry history, latest state, or a completed retry cache.
If an append I/O failure may have changed journal bytes, that open
`ActiveJournal` is terminally faulted: it refuses later sequence assignment,
append, and synchronization. Only drop plus this validated reopen path may
truncate a proven torn terminal suffix and establish a new writer authority.

## Primitive encoding

- validated nominal identities are their exact 16 network-order UUID bytes;
- `u32`, `u64`, `u128`, and `i64` are fixed-width big-endian two's-complement or
  unsigned values as named;
- a string is `u32 byte_length` followed by exact UTF-8 bytes;
- a list is `u32 item_count` followed by exactly that many items;
- an optional value is tag `0` for absent or `1` followed by the value;
- a Boolean is tag `0` or `1`;
- closed enum tags are one byte and unknown tags are invalid;
- SHA-256 identity is the exact 32 externally supplied bytes; Journal V1 does
  not fetch content or compute that digest.

Core validation bounds are also wire bounds: declaration/source references use
at most 4,096 UTF-8 bytes before their 1,024-scalar validation; exact text uses at
most 16,384 UTF-8 bytes before its 4,096-scalar validation; portable tokens use
256 bytes; content formats 64 bytes; retry keys 128 bytes; observations and
lineages 256; canonical and source gaps 64; native status tokens 16. Counts and
byte lengths are refused before `Vec` or `String` allocation.

## Admission payload order

The payload stores fields in the following fixed traversal order. Nested rows
are repeated only for their explicit count.

1. Admission `StoreId`.
2. Governing declaration: `StoreId`, `SeriesId`, revision, optional predecessor,
   immutable source binding, revisionable payload, and declaration evidence.
   The source binding is provider string, optional projection string, and locator
   string. The payload is producer, collection mode, value family, quantity
   tri-state, unit tri-state, and optional application reference. Declaration
   evidence is effective timestamp and optional artifact.
3. Canonical envelope metadata: series, producer, collection mode, then kind.
   Observed kind carries observation count/items and gap count/items. No-change
   carries its real half-open time interval.
4. Retry qualification: series, producer, exact key, and content identity.
5. Source batch: schema string, non-zero version, and observed/no-change kind.
6. Capture lifecycle: system evidence/provider/projection; endpoint evidence,
   system link and locator; run evidence, endpoint link, start and optional
   completion; snapshot evidence, run link and artifact.
7. Admission evidence kind. Observed carries ordered lineage count/items followed
   by source-gap count/items. No-change has no following lineage or gap fields.

An observation stores identity; exact value; optional source, receive, and
effective times; quality level and independent flag bits; ordered native-status
tokens; optional producer position; and optional interval. Exact-value tags are
`1` real bits, `2` signed, `3` unsigned, `4` Boolean, `5` state, `6` text, `7`
artifact, and `8` unavailable with optional reason. Collection-mode tags are
`1` sampled, `2` change-only, `3` cumulative, `4` interval, and `5` event.
Value-family tags use `1` real through `7` artifact in the same family order.

Each retained lineage stores original `u8` ordinal, canonical `ObservationId`,
source observation evidence, raw record evidence, and normalized record evidence.
Optional source provenance artifact, new/redelivered transport, both source
idempotency records, all EvidenceId links, every artifact/content identity, and
the source gap reason are retained exactly. Canonical and source gap reasons use
separate closed tags because their semantics are not interchangeable.

## Decode authority and structural validation

Decode constructs `DecodedAdmissionV1`, not `CanonicalAdmission`. It reconstructs
validated core primitives where that adds bounded structural checking, but the
declaration is a store-owned mirror and no registry-issued capability exists.
There is no conversion into canonical runtime input.

Beyond primitive constructors, decode verifies exact store/series/producer/mode
scope, revision predecessor, source and envelope interval classifications,
projection-bearing declaration/lifecycle binding, declared value family,
lineage and gap counts, canonical observation association, increasing ordinals,
capture/record links, raw idempotency content, source gap ranges, and unique
capture/record EvidenceIds. It cannot fabricate missing declaration history or
authorize a historical/retired declaration. A decoded value may be inspected and
deterministically re-encoded only.

Unknown magic, version, kind, flags, tags, invalid UTF-8/identity/canonical
primitive, impossible count/length, structural mismatch, duplicate evidence,
truncation, checksum failure, and trailing bytes return closed sanitized
`JournalV1Error` variants without retaining caller input.

Manifest-rooted open adds stable-lock, fixed-inventory, restored-registry,
historical-declaration, and exact manifest/checkpoint cutoff validation around
this frozen framing. It deliberately does not change the payload grammar or turn
decoded evidence into lifecycle authority.
