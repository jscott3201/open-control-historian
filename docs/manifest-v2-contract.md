# Manifest V2 design and authority contract

> Review barrier only. Current product code emits and accepts only Manifest V1.

Manifest V2 is the future 160-byte commit root for Store Format V2. It preserves
the exact Manifest V1 field positions while changing the manifest identity and
making its optional catalog reference resolve only Generation Catalog V2. It is
published last and is the sole point at which a rotation's raw seal, Published
Native Segment V1, Catalog V2, and empty successor become committed.

All multibyte fields are big-endian. CRC-32C uses the existing reflected
Castagnoli law stated in the
[Store Format V2 contract](store-format-v2-contract.md).

## Names and exact bytes

The two reusable finals are `manifest-v2-slot-0.och` and
`manifest-v2-slot-1.och`. The only staging name is `manifest-v2.staging`.

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHMAN02` |
| 8 | 2 | version | unsigned `2` |
| 10 | 2 | record length | unsigned `160` |
| 12 | 16 | store identity | exact Store Format V2 `StoreId` |
| 28 | 8 | manifest generation | positive |
| 36 | 8 | active journal generation | positive Journal V1 generation |
| 44 | 8 | checkpoint generation | positive Journal V1 checkpoint generation |
| 52 | 8 | durable append sequence | zero only at genesis |
| 60 | 8 | durable end offset | exact checkpoint frame boundary |
| 68 | 1 | registry slot | `0..=2` |
| 69 | 3 | reserved | zero |
| 72 | 8 | registry generation | positive |
| 80 | 8 | registry artifact length | bounded and positive |
| 88 | 4 | registry artifact CRC-32C | complete Series Registry V1 bytes |
| 92 | 1 | retry slot | `0..=2` |
| 93 | 3 | reserved | zero |
| 96 | 8 | retry generation | positive |
| 104 | 8 | retry artifact length | bounded and positive |
| 112 | 4 | retry artifact CRC-32C | complete Retry State V1 bytes |
| 116 | 1 | recovery presence | `0` absent or `1` present |
| 117 | 1 | recovery slot | `0..=2` when present; zero when absent |
| 118 | 2 | reserved | zero |
| 120 | 4 | recovery artifact CRC-32C | complete Recovery State V1 bytes; zero when absent |
| 124 | 8 | active exclusive sequence floor | zero at genesis; otherwise positive |
| 132 | 1 | catalog slot | `0..=2`, or zero when absent |
| 133 | 3 | reserved | zero |
| 136 | 8 | catalog generation | positive when present; otherwise zero |
| 144 | 8 | catalog artifact length | positive when present; otherwise zero |
| 152 | 4 | catalog artifact CRC-32C | complete Catalog V2 bytes; otherwise zero |
| 156 | 4 | manifest CRC-32C | bytes `0..156` |

The bytes at `132..156` can identify only one of the three
`generation-catalog-v2-slot-{0,1,2}.och` artifacts. A Catalog V1 artifact or name
cannot satisfy this reference. Registry, retry, and recovery references retain
their current V1 artifact families and exact laws. The active journal and
checkpoint remain Journal V1 artifacts.

## Genesis and rotation law

Generation-one V2 genesis has sequence floor zero and no catalog. Its catalog
body is all zero, and no raw seal or Published Native Segment V1 is present. It
references the canonical initial Series Registry V1 and mandatory empty Retry
State V1 snapshots under their retained laws.

Every committed non-genesis root produced by nonempty rotation has a positive
sequence floor and a present Catalog V2 reference. Every entry in that referenced
catalog must bind both its retained raw seal and its required Published Native
Segment V1. The last entry describes the prior nonempty source range; the root's
active generation is its exact empty Journal V1 successor. The active sequence
and cutoff equal the prior entry's inclusive cutoff, the active end is the
28-byte Journal Header V1 boundary, and the successor checkpoint generation is
the exact canonical initial generation.

Across the retained manifest pair, references may remain byte-identical or
advance only under the retained family-specific transition laws. Catalog advance
is exactly one generation to a different reusable slot and preserves every prior
Catalog V2 entry byte-for-byte while appending one entry.

## Commit and refusal law

The manifest candidate is exclusively staged, synchronized, bounded-read back,
canonically decoded, renamed over an unreferenced alternate slot, and followed by
a same-directory synchronization. Manifest V2 is published only after the intent,
retained raw seal, Published Native Segment V1, empty successor, and Catalog V2
have each completed their required synchronization and full validation. Manifest
publication is last; no earlier artifact is authority.

Open must select only exact consecutive Manifest V2 candidates and validate every
referenced family and cross-artifact relationship before adopting authority. A
missing, corrupt, foreign, malformed, or catalog-mismatched committed segment
refuses the V2 store unchanged. The retained raw seal does not provide fallback,
root substitution, segment rebuild, or degraded query/open behavior.

This contract changes no Manifest V1 byte and authorizes no compatibility decoder
or V1-to-V2 migration.
