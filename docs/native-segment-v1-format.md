# Native Segment V1 candidate format

Native Segment V1 is a dependency-free, bounded, current-only offline candidate
derived from exactly one fully committed sealed raw Journal V1 generation. It
preserves every complete original Journal V1 frame byte and adds only mechanical
series, append, and recent-observation indexes. It has no compatibility decoder,
migration path, alternate version, compression, dictionary, or replacement value
codec.

A segment candidate is never Store Format V1 inventory or durable authority. No
Manifest V1 or Generation Catalog V1 field names it; `och-runtime` never opens or
submits it; and neither it nor its query projection can authorize registry or
durable query state, retention, reclamation, or deletion of the exact sealed raw
Journal V1 source. Publication,
crash convergence, durable query/cursor semantics, merged generations, retention,
and reclamation are explicit successors. An already hostile-validated parsed view
does support the separate bounded non-authorizing
[Native Segment V1 observation query](native-segment-query-v1.md); that proof
changes no format byte or durable authority.

## Primitive law and complete layout

All multibyte integers are big-endian. Counts are unsigned `u32`; every offset and
length is an unsigned `u64`; timestamps retain signed `i64` floor Unix seconds and
unsigned `u32` normalized nanoseconds. Offsets are absolute from artifact byte
zero. UUID fields are exact validated RFC 9562 UUIDv7 network-order bytes. Every
reserved byte is zero.

The sections are contiguous in this one canonical order, with no padding, gaps,
overlap, or trailing bytes:

1. fixed 192-byte header;
2. `series_count` fixed 64-byte series-directory entries;
3. one contiguous complete-frame block for each series-directory entry;
4. `frame_count` fixed 48-byte global append-directory entries;
5. series-grouped `observation_count` fixed 96-byte recent-observation entries;
6. one four-byte CRC-32C trailer.

The trailer is CRC-32C over every preceding segment byte. CRC-32C uses reflected
Castagnoli polynomial `0x82f63b78`, initial register `0xffffffff`, byte-wise
reflected processing, and final XOR `0xffffffff`; the stored value is big-endian.
The `123456789` check value is `0xe3069283`.

## Fixed header

| Offset | Length | Field | Segment V1 contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHSEG01` |
| 8 | 2 | version | exactly `1` |
| 10 | 2 | header length | exactly `192` |
| 12 | 4 | flags | exactly zero |
| 16 | 16 | StoreId | exact source Journal Header V1 identity |
| 32 | 8 | source journal generation | positive |
| 40 | 8 | exclusive sequence floor | source catalog value |
| 48 | 8 | inclusive sequence cutoff | greater than floor |
| 56 | 8 | source registry generation | positive catalog value |
| 64 | 8 | source durable end offset | exact end after the final frame |
| 72 | 8 | source artifact length | equals source end offset |
| 80 | 4 | source artifact checksum | CRC-32C over complete raw Journal V1 bytes |
| 84 | 4 | frame count | exactly `cutoff - floor`, in `1..=4096` |
| 88 | 4 | series count | in `1..=frame_count`, at most `4096` |
| 92 | 4 | observation-index count | at most `1,048,576` |
| 96 | 8 | series-directory offset | exactly `192` |
| 104 | 8 | series-directory length | exactly `series_count * 64` |
| 112 | 8 | block-region offset | exact end of series directory |
| 120 | 8 | block-region length | sum of every complete source frame length |
| 128 | 8 | append-directory offset | exact end of block region |
| 136 | 8 | append-directory length | exactly `frame_count * 48` |
| 144 | 8 | recent-directory offset | exact end of append directory |
| 152 | 8 | recent-directory length | exactly `observation_count * 96` |
| 160 | 8 | complete artifact length | exact recent end plus four-byte CRC |
| 168 | 24 | reserved | all zero |

The source end, source artifact length, source checksum, range, and frame count are
not hints. Parser validation reconstructs the raw Journal Header V1 followed by
frames in global append order and requires the exact retained length and checksum.

## Series directory and frame blocks

Series entries are ordered by exact `SeriesId` bytes ascending. Every source frame
belongs to exactly one entry and every entry owns exactly one nonempty contiguous
block. Within a block, complete original Journal V1 frames appear exactly once in
append-sequence ascending order, with no block header or replacement payload
codec.

| Entry offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 16 | SeriesId | exact owning series |
| 16 | 8 | block offset | canonical contiguous block start |
| 24 | 8 | block length | exact sum of complete frame lengths |
| 32 | 4 | frame count | positive number of frames in this block |
| 36 | 4 | observation count | exact recent entries for this series; may be zero |
| 40 | 8 | recent slice offset | canonical contiguous slice start |
| 48 | 8 | recent slice length | exactly series observation count times `96` |
| 56 | 8 | reserved | all zero |

Complete frames retain their own declaration revision/binding/evidence, envelope,
retry, batch, capture, observation, quality/status, producer-position, lineage,
gap/no-change, append sequence, and frame CRC. Decode therefore remains the same
non-authorizing `DecodedAdmissionV1` boundary as Journal V1.

## Global append directory

Entries are strictly ordered by append sequence and cover every sequence from
`floor + 1` through `cutoff` exactly once. Each entry must name the exact series
block frame discovered by bounded frame traversal.

| Entry offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | append sequence | strict global order |
| 8 | 16 | SeriesId | owning series block |
| 24 | 8 | frame offset | absolute complete-frame start |
| 32 | 8 | frame length | exact complete original frame bytes |
| 40 | 4 | frame ordinal | zero-based ordinal in the series block |
| 44 | 4 | reserved | all zero |

## Recent-observation directory

Entries are grouped in series-directory order. Within each series they are sorted
by the existing `RawObservationOrderKey`—effective time, receive time, then
`ObservationId`—descending. Equal raw keys are ordered by append sequence
descending, then observation ordinal ascending. Append sequence, observation
ordinal, frame ordinal, frame offset, and frame length provide stable tie and
location evidence. A frame with gaps or no-change but no observations remains in
its block and the append directory and contributes no recent entry.

| Entry offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 16 | SeriesId | owning series |
| 16 | 8 | effective floor Unix seconds | signed `i64` |
| 24 | 4 | effective nanoseconds | normalized `< 1,000,000,000` |
| 28 | 8 | receive floor Unix seconds | signed `i64` |
| 36 | 4 | receive nanoseconds | normalized `< 1,000,000,000` |
| 40 | 16 | ObservationId | final raw-order component |
| 56 | 8 | append sequence | stable source frame identity |
| 64 | 4 | observation ordinal | zero-based envelope ordinal |
| 68 | 4 | frame ordinal | zero-based owning block ordinal |
| 72 | 8 | frame offset | exact complete-frame location |
| 80 | 8 | frame length | exact complete-frame length |
| 88 | 8 | reserved | all zero |

## Hard bounds and checked allocation

One segment derives only from one sealed source. The source hard bounds are 512
MiB including its 28-byte Journal Header V1, 4,096 frames, and 256 observations
per admission. A series must own at least one frame, so series count is at most
4,096. Observation entries are at most `4,096 * 256 = 1,048,576`.

The exact maximum complete segment is `637,993,128` bytes:

```text
192
+ 4,096 * 64
+ (536,870,912 - 28)
+ 4,096 * 48
+ 1,048,576 * 96
+ 4
```

Counts are checked before directory allocation. Every multiplication, addition,
offset conversion, source range, and complete length is checked before candidate
allocation or hostile-input index allocation. These bounds are fixed consequences
of current source authority and are not configurable.

## Source proof, parser law, and authority

The pure builder first requires exact `SealedGeneration` length/end/checksum,
Journal Header V1 StoreId, nonempty range, exact frame boundaries and CRCs,
strict consecutive global append sequences, exact store and one-series scope,
Journal V1 semantic decode, and byte-identical canonical decoded re-encoding. It
rejects a suffix or any count/range mismatch before grouping. A `BTreeMap` groups
by exact series identity; every emitted directory is explicitly canonical.

The hostile parser verifies header/version/flags/reserved bytes, exact total
length and checksum, all hard bounds and canonical section arithmetic before
allocation. It traverses each block as complete Journal V1 frames, decodes and
byte-identically re-encodes every frame, rebuilds global append and recent indexes,
and requires byte-for-byte matching directory evidence. It also reconstructs the
source raw-journal length and checksum in append order. Invalid input returns only
closed path- and content-free errors.

`ManifestStore::build_segment_candidate_v1` is a read-only convenience: it
selects only an exact committed catalog entry, performs existing sealed Journal V1
validation, bounded-reads that immutable raw artifact, and invokes the same pure
builder. It rejects active, unknown, and uncommitted generations and never writes,
renames, synchronizes, cleans up, or changes manifest/catalog/registry/retry/write
state. Candidate bytes remain outside the recognized store inventory.

Query execution is not part of this byte format. The current-only bounded query
contract is defined separately in
[Native Segment V1 observation query](native-segment-query-v1.md).
