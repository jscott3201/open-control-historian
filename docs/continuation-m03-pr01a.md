# M03-PR01a Native Segment V1 foundation continuation

## Delivered boundary

`och-store` now owns one exact current-only Native Segment V1 candidate format.
One sealed raw generation becomes one deterministic set of SeriesId-ascending
blocks containing unchanged complete Journal V1 frames, one strict global append
directory, and series-grouped recent indexes ordered by canonical raw observation
order. Source and parser proof both require byte-identical Journal V1 decoded
re-encoding, so no second canonical declaration/value/evidence codec exists.

At the PR01a boundary the public parsed view was non-authorizing and exposed
bounded metadata and index/frame inspection only; exact frame decode still produces
`DecodedAdmissionV1`, not `CanonicalAdmission`. Closed errors retain no paths,
observations, or unbounded strings. The PR02a successor adds only the bounded
non-authorizing query linked below.

## Store integration and evidence

The optional store bridge stayed a small read-only operation. It selects a sealed
generation from the already-committed in-memory catalog, runs existing bounded
sealed-journal validation, reads exact bytes, and invokes the pure builder. Tests
prove active and unknown generations refuse, every store artifact name/byte is
unchanged by candidate build, no segment artifact appears, and current Store
Format V1 state reopens unchanged.

Independent primitive-only oracles cover one-series and multiple-series exact
bytes. Fixtures retain observation collections, gaps, no-change frames, revised
declarations, differing exact values, quality/status/position, retry, batch,
capture, and lineage evidence through unchanged original frame bytes. Repetition,
recent tie ordering, source corruption/suffix/range mismatch, hostile header and
directory changes, checksum damage, and hard-bound refusal are deterministic.

## Remaining boundary

Segment publication and naming, durable inventory/reference authority, startup or
crash convergence, runtime build, cursor semantics, multiple-generation
merge, compression, memory mapping, retention/pins/reclamation, raw-journal
deletion, rollups, adapters/providers, and a Store Format successor remain absent.
The exact sealed raw Journal V1 remains the sole durable authority. M03-PR02a now
supplies only the bounded non-authorizing parsed-candidate observation query
recorded in [its continuation](continuation-m03-pr02a.md); durable query and every
other deferral above remain absent.
