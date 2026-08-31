# M03-PR02b one-generation ManifestStore query implementation brief

## Objective

Add one synchronous read-only `ManifestStore` bridge that queries exactly one
already-committed sealed raw-Journal generation by composing the existing
current-V1 candidate builder, hostile parser, and bounded M03-PR02a observation
query. Return only the existing owned non-authorizing result.

## Delivered product boundary

- `ManifestStore::query_sealed_generation_observations_v1` selects only the exact
  supplied Generation Catalog V1 entry through `build_segment_candidate_v1`; it
  has no raw-file, directory, active-generation, unknown-generation, or merge
  fallback.
- The newly built candidate passes `parse_segment_v1` with the same store identity
  before `SegmentV1::query_observations_v1` can execute.
- `ManifestStoreSegmentQueryV1Error` preserves exact closed `SegmentV1Error`
  evidence for selection/build/parse and exact
  `SegmentObservationQueryV1Error` evidence for post-parse inconsistency.
- Only `SegmentObservationQueryResultV1` escapes. Entries are copied and decoded
  admissions are `Arc`-owned, so candidate/parser temporaries drop before return
  without cloning a full source or candidate.
- The bridge remains read-side inspection in sticky reopen custody. It performs no
  durable mutation, cleanup, pressure transition, or typed reinterpretation of
  read failures.

The complete selection, result, authority, and resource law is in
[Native Segment V1 bounded observation query](native-segment-query-v1.md).

## Resource boundary

Result and decoded-cache allocation remain `O(limit)` with `limit <= 16`, but the
bridge is not overall memory- or latency-proportional to that limit. It performs a
heavyweight synchronous full-generation source validation/read, candidate build,
hostile parse, and query. Maximum current-V1 source/candidate/parser working memory
can exceed 700 MB. Allocation exhaustion has no new typed recovery contract, and
this slice adds no configurable budget or streaming path.

## Evidence boundary

Focused deterministic evidence uses two committed sealed generations of one
series to prove independent exact selection, recent order, values, append lineage,
independent source lineage ordinals, limit-one truncation, and owned-result
lifetime. Active/unknown, missing, and corrupt selected sources retain closed exact
errors without fallback. Repeated success and refusal preserve inspection,
registry/retry snapshots, artifact names/bytes, write custody, and reopen behavior;
an internal pressure fixture proves sealed read inspection remains available while
the handle requires reopen. Existing M03-PR02a unit tests remain the authority for
fine-grained interval/order/cache/impossible-index behavior.

## Explicit exclusions

There is no format, Store Format/Manifest/Catalog field, durable segment artifact
or authority, runtime command/API, cursor, multiple-generation result, pin,
retention/reclamation, raw deletion, codec/compression/compaction, configurable
resource budget, streaming path, typed allocation pressure, dependency, unsafe
code, or `och-core` semantic change.
