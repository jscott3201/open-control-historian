# Retry State V1 format and horizon law

Retry State V1 is the sole current durable retry layout. It preserves exact
`och-core::RetryQualification` comparison and adds only a bounded replayable
outcome tier followed by a bounded non-replayable expired/conflict guard. Every
replay outcome carries the full current generation, floor, and catalog extension,
including canonical zero/absent fields for the unrotated generation-one case.
Historical Retry State layouts that omit this extension and artifacts whose
version is not `1` are rejected before stable-lock acquisition; they are not
decoded as compatibility formats or rewritten.

All integers are big-endian. The complete artifact is capped at 2 MiB and uses
CRC-32C. Configured replay and guard capacities are positive and their sum cannot
exceed 4,096.

## Header

The 64-byte header is followed by its exact payload and a trailing four-byte
CRC-32C over header plus payload:

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHRET01` |
| 8 | 2 | version | unsigned `1` |
| 10 | 2 | header length | unsigned `64` |
| 12 | 16 | store identity | manifest `StoreId` |
| 28 | 8 | snapshot generation | positive |
| 36 | 4 | replay capacity | positive |
| 40 | 4 | guard capacity | positive; combined bound `4,096` |
| 44 | 4 | replay count | at most replay capacity |
| 48 | 4 | guard count | at most guard capacity |
| 52 | 8 | payload length | exact bytes before trailing CRC |
| 60 | 4 | reserved | zero |

The filename and mandatory Manifest V1 reference supply the reusable slot. Slot,
generation, complete artifact length/checksum, `StoreId`, and configured
capacities must match exactly.

## Entries

Every entry begins with the frozen retry qualification grammar: 16-byte
`SeriesId`, 16-byte `ProducerId`, length-prefixed retry key, length-prefixed
content format, `u128` content version, and exact 32-byte SHA-256 identity. Product
code does not normalize, derive, hash, or reinterpret those fields.

The payload stores exactly the replay entries first and guard entries second. A
replay entry appends the original append sequence and frame end, original
manifest generation, registry slot/generation, journal and checkpoint
generations, committed cutoff sequence/end, and original retry slot/generation.
Those fields retain the original public `ManifestCommit` that completed the
receipt without recursively embedding retry artifact length or checksum.

Every replay entry then appends this fixed 48-byte extension:

| Length | Field | Contract |
| ---: | --- | --- |
| 8 | active exclusive sequence floor | zero for generation one; positive for successors |
| 1 | catalog-present tag | `0` or `1` |
| 1 | catalog slot | zero when absent; otherwise `0..2` |
| 6 | reserved | zero |
| 8 | catalog generation | zero when absent; otherwise positive |
| 8 | catalog artifact length | zero when absent; otherwise positive and bounded |
| 4 | catalog complete-byte CRC-32C | zero when absent |
| 12 | reserved | zero |

A guard entry appends only its original positive append sequence. Within each
tier, append sequences increase strictly. Guard entries are chronologically older
than all replay entries even though replay entries occur first on disk. A
scope/key cannot occur twice within or across tiers.

The owning Manifest V1 is part of canonical validation. Its retry reference must
equal the artifact slot/generation, retained outcomes must form one reachable
contiguous suffix, and the newest replay reaches the owning cutoff and names the
snapshot that made it durable. Cross-generation outcomes retain the exact
Generation Catalog V1 reference from their original commit; older outcomes are
covered by the corresponding sealed range. Generation, slot, checkpoint, cutoff,
floor, and catalog progression must match the publication law. A successful
decode must re-encode to identical bytes.

## FIFO horizon and publication

New durable outcomes enter replay in append order. Replay overflow promotes the
oldest replay entry to guard; guard overflow evicts the oldest guard. A key is
fresh only after leaving both tiers. There is no clock, TTL, LRU, hash-derived
identity, or position refresh on replay/conflict/expired hits.

Equivalent replay returns the exact original append identity and durable commit.
Changed content in either tier returns `RetryConflict`; an equivalent guard hit
returns `RetryExpired`; an absent key is fresh. Retry replay makes no latest-state
claim and latest remains volatile.

Genesis publishes mandatory empty Retry State V1 generation one before Manifest
V1. Every durable append barrier publishes the next retry generation before the
manifest naming it. Registry-only commits and rotations with no retry semantic
change preserve the exact current reference. Open validates every retry artifact
referenced by either valid manifest candidate. Broad repair, backfill from Journal
V1, migration, time-based expiry, and unbounded history remain outside this
format.
