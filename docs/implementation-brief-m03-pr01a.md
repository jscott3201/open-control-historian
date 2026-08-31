# M03-PR01a Native Segment V1 foundation implementation brief

## Objective

Add one dependency-free, bounded, current-only Native Segment V1 candidate format
to `och-store`. The slice derives one deterministic in-memory/offline candidate
from one fully committed sealed raw Journal V1 generation, preserves complete
original frames, and adds exact series, global append, and recent-observation
indexes without creating durable segment authority.

## Delivered product boundary

- `build_segment_v1` proves exact sealed source length/end/checksum, Journal V1
  store scope, complete framing/CRC/sequence range, one-series frame scope, and
  byte-identical decoded re-encoding before deterministic grouping.
- `parse_segment_v1` treats all bytes as hostile, checks canonical non-overlapping
  layout before bounded allocation, re-proves every complete frame and directory,
  and returns borrowing non-authorizing inspection/index access.
- `PreparedSegmentV1`, `SegmentV1Inspection`, fixed directory entry types, closed
  `SegmentV1Error`, and public hard layout/bound constants expose no mutable
  registry handle or canonical-admission conversion.
- `ManifestStore::build_segment_candidate_v1` selects only one exact committed
  catalog entry and uses existing sealed-file validation plus the pure builder. It
  performs no store mutation or publication.
- The complete byte law is frozen in
  [Native Segment V1](native-segment-v1-format.md).

## Exact authority statement

The sealed raw Journal V1 artifact remains the sole durable authority. Segment
bytes are not recognized Store Format V1 inventory, are absent from Manifest V1
and Generation Catalog V1, are never opened by runtime, and cannot authorize
declarations, admissions, receipts, retry/latest state, query results, retention,
reclamation, or raw-journal deletion.

## Evidence boundary

Focused evidence uses an independent primitive-only segment byte oracle that does
not import or call production segment build/parse code. One-series revision/gap
fixtures and a multiple-series committed store fixture compare exact bytes,
exercise complete-frame round trips and recent ordering, and prove repeated build
determinism. Hostile parser/source tests cover truncation/trailing data, corrupt
frames/checksums, source metadata mismatch, count/layout/order/location changes,
active/unknown catalog selection, hard bounds, and unchanged store inventory and
reopen state.

## Explicit exclusions

There is no Store Format successor, marker/inventory/manifest/catalog segment
reference, segment filename, publication/staging/sync/recovery path, automatic
runtime build, query/result/cursor API, multi-generation merge, compression,
memory mapping, retention/pins/reclamation, compaction, rollup, raw deletion,
adapter/provider, or `och-core` change.
