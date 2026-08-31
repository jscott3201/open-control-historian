# M03-PR02b one-generation ManifestStore query continuation

## Delivered boundary

`och-store` now exposes one synchronous read-only query of exactly one committed
sealed raw-Journal generation. The caller supplies the generation and an already
validated `SegmentObservationQueryV1`. Selection passes only through the committed
Generation Catalog V1 and existing candidate builder, then the same-store hostile
parser, then the existing bounded parsed-view query. There is no source fallback,
generation scan, or merge.

The public closed error sum retains exact source/build/parse `SegmentV1Error`
classification and exact post-parse `SegmentObservationQueryV1Error`
classification. The returned `SegmentObservationQueryResultV1` owns copied index
entries and shared decoded admissions, so it remains usable after the candidate
and borrowing parsed view drop. Results remain transient non-authorizing
inspection; the sealed raw Journal V1 is still the sole durable historical
authority.

## Read-only and resource evidence

Two sealed generations of the same series prove independent values, recent order,
append sequence, source lineage ordinal, repetition, and no merge. Limit one proves
truncation and owned-result lifetime. Active and unknown generations refuse before
filesystem fallback; repeated missing and corrupt selected-source refusals retain
exact segment evidence. Successful and refused queries leave inspection,
registry/retry snapshots, recognized names/bytes, and write custody unchanged and
reopen normally. Sealed inspection also remains available after injected mutating
pressure places the live handle in sticky reopen custody.

The result/cache allocation law remains `O(limit)` for a positive limit no greater
than 16. The bridge itself remains heavyweight synchronous full-generation work:
maximum source/candidate/parser working memory can exceed 700 MB even for limit
one. Allocation exhaustion has no typed recovery behavior in this slice.

## Remaining boundary

Durable segment publication/naming/reference/authority, startup or crash
convergence for segment artifacts, runtime query ownership, raw fallback,
multiple-generation merge, cursor pagination, gap/no-change result semantics,
pins/retention/reclamation, raw deletion, compression/codecs, compaction/rollups,
streaming/resource budgets, typed allocation pressure, memory mapping,
adapters/providers, and a Store Format successor remain absent.
