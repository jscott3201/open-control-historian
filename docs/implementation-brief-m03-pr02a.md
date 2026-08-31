# M03-PR02a bounded Native Segment V1 observation-query implementation brief

## Objective

Prove the Native Segment V1 recent-observation indexes and consumer semantics by
adding one dependency-free bounded read over an already parsed in-memory
`SegmentV1<'_>`. The caller selects one exact series, an optional canonical
effective-time interval, and a positive limit no greater than 16.

## Delivered product boundary

- `SegmentObservationQueryV1` validates the fixed result bound before allocation.
- `SegmentV1::query_observations_v1` finds one validated per-series slice, retains
  canonical recent-first order, and applies `TimeInterval::contains` to effective
  time before frame decode.
- `SegmentObservationQueryResultV1` returns at most 16 immutable exact observation
  items plus evidence that at least one additional match exists, without a cursor.
- Each item exposes its index entry, shared non-authorizing decoded admission,
  exact ordinal-selected canonical observation, and associated decoded lineage.
- Exact append/frame/series/ordinal/order/identity/lineage cross-checks convert an
  impossible post-parse inconsistency into one closed sanitized error.
- A standard-library shared frame cache keeps result allocation `O(limit)` and
  decodes/retains at most `limit` distinct selected frames.

The complete public law is in
[Native Segment V1 bounded observation query](native-segment-query-v1.md).

## Evidence boundary

Focused deterministic tests cover zero/17 refusal, limits 1/16, truncation with no
extra decode, same-frame sharing, distinct-frame decode bounds, canonical raw
ordering and stable ties, start-inclusive/end-exclusive negative-time filtering,
exact values/times/quality/status/producer position and source evidence, selected
series isolation, unknown/no-match/gap-only/no-change-only emptiness, repetition,
byte immutability, and test-only impossible index inconsistencies. Existing
hostile parser tests continue to prove malformed bytes cannot construct a public
query target.

## Explicit exclusions

There is no format-byte change, Store Format/Manifest/Catalog segment authority,
publication/naming/staging/sync/recovery path, filesystem query, runtime command,
raw Journal fallback, multi-generation merge, cursor, gap result, retention,
reclamation, compression, memory mapping, adapter, dependency, unsafe code, or
`och-core` change.
