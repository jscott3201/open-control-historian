# Native Segment V1 bounded observation query

## Scope and input

The Native Segment V1 observation query is a dependency-free, current-only read
contract over one already hostile-validated `SegmentV1<'_>`. A validated
`SegmentObservationQueryV1` selects exactly one `SeriesId`, optionally supplies an
existing canonical `TimeInterval`, and requests a positive result limit. Zero and
limits above `MAX_SEGMENT_QUERY_RESULTS_V1` refuse before segment inspection or
result allocation; that constant is exactly 16.

This contract does not open or build a store candidate. It performs no filesystem
I/O, runtime command, raw-Journal fallback, generation merge, or durable segment
lookup. Unvalidated bytes must first pass `parse_segment_v1`; malformed bytes
therefore never gain a public query path.

## Selection and ordering

The selected series is located in the validated SeriesId-ascending directory by
binary search. Its exact contiguous recent-observation slice is derived with
checked range arithmetic. No other series' recent entries or frames are scanned
or decoded.

Entries retain Native Segment V1 canonical order: `RawObservationOrderKey`
descending—effective time, receive time, then `ObservationId`—followed for equal
raw keys by append sequence descending and observation ordinal ascending. An
optional interval is applied only to `entry.raw_order_key().effective()` through
`TimeInterval::contains`, so the start endpoint is inclusive and the end endpoint
is exclusive. Signed pre-epoch timestamps retain the same normalized law.

Frames containing only gaps or no-change evidence have no observation-directory
entry and cannot become observation results. Unknown series and valid intervals
with no match return an empty non-truncated result.

## Exact result and truncation evidence

Each `SegmentObservationQueryItemV1` exposes:

- its proven `SegmentObservationEntryV1`;
- the exact non-authorizing `DecodedAdmissionV1` containing the item;
- the canonical `Observation` selected by `observation_ordinal`; and
- the corresponding `DecodedObservationLineageV1`.

Before returning an item, query execution rechecks the global append entry,
append sequence, series scope, frame offset/length/ordinal, observation ordinal,
raw order key, `ObservationId`, store scope, and canonical/source lineage links.
Impossible index/frame/decode inconsistency returns the closed path- and
content-free `InconsistentSegment` error and never becomes a partial result.
Decoded evidence has no conversion to `CanonicalAdmission` and authorizes no
registry, retry, runtime submit, receipt, manifest, or reclamation state.

Items are immutable and recent first. `is_truncated` and its `has_more` alias are
true exactly when at least one further matching index entry exists after the
materialized limit. Execution inspects only through that first extra match and
does not decode it. This slice provides no continuation token; a caller may issue
another independent bounded query but cannot resume from a cursor.

## Bounds and complexity

Result and decoded-frame-cache allocation are `O(limit)` and the limit is at most
16. Admissions are standard-library `Arc`-shared internally, so observations from
one selected frame retain one decoded admission allocation and every distinct
selected frame is decoded at most once. At most `limit` distinct decoded frames
are retained; a large admission is never cloned once per result item.

Series lookup is `O(log S)`. For `I` inspected entries in the selected series,
append lookup is `O(I log F)` and filtering is constant work per entry; interval
filters with sparse or no matches may necessarily inspect the complete selected
series slice. `D` frame decodes occur, where `D <= materialized results <= 16`.
No work term includes unrelated series observations or frame decoding.

## Authority and deferrals

Querying does not mutate candidate bytes or parsed directories and has no store,
filesystem, manifest, catalog, registry, retry, latest, receipt, or runtime side
effect. The sealed raw Journal V1 remains the sole durable authority. Durable
segment publication/naming/sync/recovery, store query convenience, raw fallback,
multi-generation merge, cursor pagination, gap/no-change results, retention,
reclamation, compression, rollups, memory mapping, and adapters remain deferred.
