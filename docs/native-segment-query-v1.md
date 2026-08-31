# Native Segment V1 bounded observation query

## Scope and input

The Native Segment V1 observation query is a dependency-free, current-only read
contract over one already hostile-validated `SegmentV1<'_>`. A validated
`SegmentObservationQueryV1` selects exactly one `SeriesId`, optionally supplies an
existing canonical `TimeInterval`, and requests a positive result limit. Zero and
limits above `MAX_SEGMENT_QUERY_RESULTS_V1` refuse before segment inspection or
result allocation; that constant is exactly 16.

The parsed-view method itself does not open or build a store candidate and performs
no filesystem I/O. Unvalidated bytes must first pass `parse_segment_v1`; malformed
bytes therefore never gain a public query path. The separate store composition
below reuses that exact parser and query and adds no alternate path.

## Exact store composition

`ManifestStore::query_sealed_generation_observations_v1` synchronously selects
exactly the supplied generation through
`ManifestStore::build_segment_candidate_v1`. Consequently only the authoritative
committed Generation Catalog V1 entry can select a source: active, unknown,
uncommitted, foreign, corrupt, excessive, missing, or unreadable evidence refuses
without a raw-file or directory fallback.

The method then parses the newly built current-V1 candidate with the same store
identity and invokes `SegmentV1::query_observations_v1` with the caller's already
validated query. `ManifestStoreSegmentQueryV1Error` preserves the exact
`SegmentV1Error` from selection/build/parse or the exact
`SegmentObservationQueryV1Error` from post-parse execution. The error sum, its
accessors, and its display/source chain are closed, path-free, and content-free.

Only `SegmentObservationQueryResultV1` leaves the method. Its copied entries and
`Arc`-owned decoded admissions remain valid after the candidate bytes and borrowing
parsed view drop. The composition creates no additional full source or candidate
clone beyond the existing builder/parser path.

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

Those result bounds do not bound the store composition's overall memory or
latency. It performs a heavyweight synchronous full-generation sealed-source
validation/read, candidate build, hostile parse, and query. At maximum current-V1
bounds, simultaneous source/candidate/parser working memory can exceed 700 MB even
when the query limit is one. Allocation exhaustion has no new typed pressure or
recovery contract; no configurable budget or streaming path exists in this slice.

## Authority and deferrals

Querying does not mutate candidate bytes or parsed directories. The store
composition reads only the selected committed sealed source and does not write,
synchronize, rename, clean up, or mutate inventory, manifest, catalog, registry,
retry, recovery, latest, receipt, or write-custody state. It remains available as
read-side inspection while the live store requires validated reopen and does not
reinterpret read failures as typed storage pressure. The sealed raw Journal V1
remains the sole durable authority. Durable segment publication/naming/sync/
recovery/authority, raw fallback, multi-generation merge, runtime query, cursor
pagination, gap/no-change results, retention, reclamation, compression, rollups,
memory mapping, and adapters remain deferred.
