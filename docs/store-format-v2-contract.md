# Store Format V2 design and authority contract

> Review barrier only. This document specifies a future format; it does not
> describe bytes accepted or emitted by the current implementation.

Store Format V2 is the new-store, current-only epoch in which every nonempty
rotation must publish both a retained sealed raw Journal V1 and a Published
Native Segment V1. Manifest V2 remains the sole commit point. The current
repository remains Store Format V1-only and must continue to treat every V2 name
and byte in this contract as unsupported or unknown inventory.

This contract authorizes no implementation. Implementation is blocked on the
separately reviewed resource and evidence prerequisites in the
[M03-PR03b implementation brief](implementation-brief-m03-pr03b.md).

## Primitive law

Every multibyte integer is unsigned and big-endian unless a referenced retained
V1 format says otherwise. CRC-32C uses the existing reflected Castagnoli law:
polynomial `0x82f63b78`, initial register `0xffffffff`, byte-wise reflected
processing, and final XOR `0xffffffff`. Stored checksums are big-endian. The
`123456789` check value is `0xe3069283`. A range such as `0..28` is half-open.

## Epoch marker

The final marker is `store-format-v2.och`. Publication uses the fixed
same-directory staging name `store-format-v2.staging`.

The marker is exactly 32 bytes:

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHFMT02` |
| 8 | 2 | version | unsigned `2` |
| 10 | 2 | record length | unsigned `32` |
| 12 | 16 | store identity | exact canonical `StoreId` |
| 28 | 4 | CRC-32C | bytes `0..28` |

Read is exact and bounded to 32 bytes. Wrong magic, version, length, store scope,
checksum, truncation, or trailing bytes is unsupported format evidence.

The stable lock remains exactly `store-v1.lock`; it is never renamed. Sharing
this name prevents a V1 opener and a future V2 opener from acquiring different
store-wide locks for one directory. Read-only format and inventory refusal still
precedes lock creation or acquisition.

V2 is valid only for a newly created V2 directory. This contract defines no
in-place or offline migration, upgrade, import, or compatibility decoder. A
future V2 opener must refuse a V1 directory unchanged, and current V1 code must
continue to refuse a V2 or mixed directory unchanged. A V1 directory remains
valid only under the unchanged current V1 implementation.

## Recognized namespace and exact bound

Store Format V2 recognizes these V2 families **instead of** the corresponding V1
families:

- `store-format-v2.och` and `store-format-v2.staging`, not the V1 marker names;
- `manifest-v2-slot-{0,1}.och` and `manifest-v2.staging`, not Manifest V1 names;
- `generation-catalog-v2-slot-{0,1,2}.och` and
  `generation-catalog-v2.staging`, not Generation Catalog V1 names; and
- `journal-rotation-v2.intent`, not the V1 intent name.

It retains the current names and laws for `store-v1.lock`, active Journal V1 and
checkpoint artifacts, Series Registry V1, Retry State V1, Recovery State V1,
sealed Journal V1 finals and staging, and their existing bounded reusable slots.
It additionally recognizes:

- up to 64 finals named
  `native-segment-v1-g{generation:020}.och`, for generations `1..=64`; and
- exactly one fixed `native-segment-v1.staging` name.

The inventory-entry arithmetic is exact:

| Component | Entries |
| --- | ---: |
| Current equivalent recognized inventory maximum | 91 |
| V2 marker/manifest/catalog/intent names replacing V1 names | `+0` |
| Published Native Segment V1 final names | `+64` |
| Published Native Segment V1 staging name | `+1` |
| **Store Format V2 recognized inventory maximum** | **156** |

Thus `91 + 64 + 1 = 156`. The 91 baseline is the implemented V1 inventory cap;
the four V2 family substitutions are count-neutral. The new count does not
authorize alternate spellings, extra staging names, orphan segments, a 65th
generation, or unrelated files. Unknown names, non-files, excessive entries,
V1/V2 mixtures, and malformed recognized artifacts refuse without mutation.

Genesis has no catalog, sealed raw generation, or published segment. After each
nonempty rotation, every committed Generation Catalog V2 entry names exactly one
retained raw seal and exactly one Published Native Segment V1 with the same
generation and source relationship.

## Rotation Intent V2

The future transaction uses one fixed `journal-rotation-v2.intent`. It is an
exactly 128-byte, non-authorizing record. All zero requirements are canonical.

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHROT02` |
| 8 | 2 | version | unsigned `2` |
| 10 | 2 | record length | unsigned `128` |
| 12 | 16 | store identity | exact marker `StoreId` |
| 28 | 8 | source journal generation | positive |
| 36 | 8 | successor journal generation | exactly source plus one |
| 44 | 8 | inclusive sequence cutoff | positive; exact committed source cutoff |
| 52 | 8 | source durable end offset | greater than Journal Header V1 |
| 60 | 8 | registry generation | positive; covers the complete source range |
| 68 | 8 | candidate catalog generation | positive; exact next Catalog V2 generation |
| 76 | 8 | source checkpoint generation | positive; exact mechanical cutoff generation |
| 84 | 2 | raw format | `1` = sealed Journal V1 |
| 86 | 8 | retained raw-seal length | exact source durable end offset |
| 94 | 4 | retained raw-seal CRC-32C | complete raw artifact bytes |
| 98 | 2 | segment format | `1` = exact Native Segment V1 `OCHSEG01` |
| 100 | 8 | complete segment length | exact Published Native Segment V1 length |
| 108 | 4 | complete segment CRC-32C | exact Segment V1 trailer checksum value |
| 112 | 12 | reserved | zero |
| 124 | 4 | intent CRC-32C | bytes `0..124` |

The segment checksum field is the Native Segment V1 trailing CRC-32C, which is
computed over every segment byte preceding that four-byte trailer. The raw
checksum covers every retained raw-Journal byte. The intent's raw and segment
identities must equal the candidate Catalog V2 entry and the fully verified final
artifacts exactly.

The intent never authorizes a seal, segment, catalog, successor, registry state,
or root. It is bounded transaction evidence used only for exact rollback or
postcommit cleanup under the retained locks. After Manifest V2 commits and the
complete manifest/catalog/raw/segment/successor relation validates, that proof
permits and requires idempotent removal of the exact predecessor active Journal
V1, its predecessor checkpoint, and exact redundant transaction staging. It never
permits removal of the retained raw-seal final or Published Native Segment V1
final.

`journal-rotation-v2.intent` remains present until predecessor and staging cleanup
is proven complete. Cleanup removes the predecessor active journal, synchronizes
the directory, removes its checkpoint, synchronizes the directory, removes exact
redundant transaction staging in publication order—raw seal, segment, catalog,
then manifest—synchronizing the directory after each present removal, and proves
the clean committed inventory before removing the intent last and synchronizing
the directory again. Every present staging artifact must exact-match its
intent/root derivative before removal. Repetition after a crash is idempotent only
when the committed root and still-present intent prove the exact cleanup prefix.
If the intent is absent while predecessor or staging evidence remains, or any
relation is ambiguous, open refuses unchanged rather than guessing. An absent
intent is canonical only with the exact already-clean committed inventory.

## Authority boundary

Exact Manifest V2 bytes are specified by the
[Manifest V2 contract](manifest-v2-contract.md), exact catalog bytes by the
[Generation Catalog V2 contract](generation-catalog-v2-contract.md), and the
persisted segment identity and no-fallback law by the
[Published Native Segment V1 contract](published-native-segment-v1-contract.md).

No Store Format V2 implementation, public API, current V1 format change,
migration, codec-backed segment, query integration, retention, reclamation,
repair, or fallback is authorized here.
