# M03-PR02a bounded Native Segment V1 observation-query continuation

## Delivered boundary

`och-store` now owns one current-only bounded observation query over an already
hostile-validated in-memory `SegmentV1<'_>`. One exact `SeriesId` is selected from
the canonical series directory; an optional existing `TimeInterval` filters
effective time with the canonical `[start, end)` law. Results preserve the
segment's raw-order-descending and stable-tie order and stop at the caller's
positive limit, which cannot exceed 16.

Each result item binds its exact recent index entry to the matching global append
entry, decoded frame, envelope observation ordinal, and source lineage. Decoded
admissions are shared per frame, retained and decoded at most once per selected
frame, and remain non-authorizing. Truncation means exactly that at least one
additional matching index entry exists; only the first extra match is inspected,
that item is not decoded, and no cursor is created.

## Evidence and bounds

Focused unit and public-surface evidence covers both hard-limit endpoints and
refusals, selected-series isolation, out-of-order effective/receive times, raw-key
and append/ordinal ties, negative interval boundaries, exact canonical values and
source/capture/declaration/retry evidence, same- and distinct-frame sharing,
unknown/no-match/no-change/gap emptiness, deterministic repetition, candidate-byte
immutability, and closed refusal of test-only impossible parsed-index changes.
Result/cache allocation is `O(limit)`, series lookup is logarithmic, unrelated
series frames are never decoded, and interval scanning is bounded by the selected
series' already validated index slice.

## Remaining boundary

The query is not durable segment authority and does not read store inventory.
Segment publication/naming/reference, startup or crash convergence, store/runtime
query integration, raw Journal fallback, multiple-generation merge, cursor
pagination, gap/no-change result semantics, retention/pins/reclamation, raw
deletion, compression, rollups, memory mapping, adapters/providers, and a Store
Format successor remain absent. The exact sealed raw Journal V1 remains the sole
durable authority.
