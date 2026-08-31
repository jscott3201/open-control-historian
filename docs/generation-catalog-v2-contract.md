# Generation Catalog V2 design and authority contract

> Review barrier only. Current product code emits and accepts only Generation
> Catalog V1.

Generation Catalog V2 is the bounded Manifest V2 description of each committed
rotated generation's retained sealed Journal V1 and required Published Native
Segment V1. It is not directory-derived authority, a retention index, a query
root independent of Manifest V2, or permission to delete either artifact.

All multibyte fields are big-endian. CRC-32C uses the existing reflected
Castagnoli law in the
[Store Format V2 contract](store-format-v2-contract.md).

## Names and bound

The three reusable finals are:

- `generation-catalog-v2-slot-0.och`;
- `generation-catalog-v2-slot-1.och`; and
- `generation-catalog-v2-slot-2.och`.

The sole staging name is `generation-catalog-v2.staging`. A Manifest V2 catalog
reference carries the slot, positive generation, exact complete length, and
CRC-32C over the complete catalog artifact including its own trailing CRC.

The catalog contains `1..=64` entries. Its exact maximum is 5,188 bytes:

```text
64 + 64 * 80 + 4 = 5,188
```

Entry 65 refuses before mutation and never overwrites, drops, or reclaims prior
history.

## Exact 64-byte header

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHCAT02` |
| 8 | 2 | version | unsigned `2` |
| 10 | 2 | header length | unsigned `64` |
| 12 | 16 | store identity | exact Manifest V2 `StoreId` |
| 28 | 8 | catalog generation | positive; exactly equals entry count |
| 36 | 4 | entry count | `1..=64` |
| 40 | 8 | payload length | exactly `count * 80` |
| 48 | 16 | reserved | zero |

The header is followed by exactly `count` fixed entries and one four-byte
CRC-32C over the header and all entries. Exact total length is therefore
`64 + count * 80 + 4`.

## Exact 80-byte entry

| Offset within entry | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | journal generation | positive and contiguous from `1` |
| 8 | 8 | exclusive sequence floor | first entry exactly `0` |
| 16 | 8 | inclusive sequence cutoff | strictly greater than floor |
| 24 | 8 | raw source durable end offset | greater than the 28-byte Journal V1 header |
| 32 | 8 | registry generation | positive authority covering the complete range |
| 40 | 8 | retained raw-seal length | equals source end; at most 512 MiB |
| 48 | 4 | retained raw-seal CRC-32C | complete sealed Journal V1 bytes |
| 52 | 2 | raw format | `1` = sealed Journal V1 |
| 54 | 2 | segment format | `1` = exact Native Segment V1 `OCHSEG01` |
| 56 | 8 | complete segment length | exact Published Native Segment V1 bytes |
| 64 | 4 | complete segment CRC-32C | exact Segment V1 trailer checksum value |
| 68 | 12 | reserved | zero |

The segment checksum field is the trailing checksum stored by the exact Native
Segment V1 grammar, covering every segment byte before that trailer. The segment
length includes the trailer. The raw and segment generation, store, sequence
range, source end/length/checksum, and registry generation must agree exactly.

## Canonical range and artifact law

Entries are ordered by journal generation. Each generation is the exact successor
of the previous generation, and each prior inclusive cutoff equals the next
exclusive floor. A new catalog preserves every previous 80-byte entry exactly
and appends one entry; update, reorder, hole, overlap, alternate identity, or
replacement is noncanonical.

Every entry requires both canonical finals:

- `sealed-journal-v1-g{generation:020}.och`; and
- `native-segment-v1-g{generation:020}.och`.

There is exactly one of each, no orphan or gap, and no alternate name. A catalog
is not authority until named by Manifest V2. Once committed, the raw Journal V1
remains retained semantic and recovery evidence; the segment neither supersedes
it nor permits its deletion.

Before catalog publication, the writer fully validates the raw artifact, fully
hostile-parses the segment, reconstructs and verifies the segment's source link,
and exact-compares both identities with the candidate entry. A future V2 open
performs complete bounded streaming checksum and hostile parse of every committed
pair before authority adoption. This eager full-payload validation is mandatory,
not lazy. Its memory, workspace, writer-delay, and open-latency budgets are
`UNKNOWN` and block implementation until separately measured and owner-approved.

Missing, corrupt, foreign, malformed, excessive, forked, unrelated, ambiguous,
or pair-mismatched committed evidence refuses unchanged. Retained raw bytes are
never an implicit segment fallback or rebuild source during open.
