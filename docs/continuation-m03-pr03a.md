# M03-PR03a transient typed-value block codec proof continuation

## Delivered boundary

`och-store` now compiles one private `typed_value_block` module only for crate
tests. Its encoder and decoder prove exact raw records for every current
`ValueFamily`, including unavailable with and without a reason under every
family. Available Boolean-only blocks additionally prove LSB-first bit-pack and
positive-`u16` RLE with checked deterministic complete-length selection.

The decoder treats bytes as hostile, limits complete input to 8 MiB and output to
256 values, reconstructs through current core model validation, recomputes the
canonical winner, and refuses malformed or valid-but-noncanonical alternatives.
The encoder preflights lengths without allocating competing candidate buffers.

## Independent evidence

An independently authored standard-library primitive oracle encodes and decodes
the same bounded fixtures without importing either product crate. Tests compare
product bytes and oracle bytes, compare reconstructed primitive values, enforce
the oracle-source import boundary, and pin a fixed real-bits golden.

Deterministic evidence covers raw exactness across every family, unavailable
under every family, real edge patterns and NaN payloads, integer extrema,
composed/decomposed Unicode, opaque state tokens, complete supplied artifact
identity, raw/packed/RLE winners, raw and compact ties, unavailable fallback,
repetition, maximum count, fixed-header attacks, truncation/trailing data,
Boolean and padding errors, every RLE structural refusal, invalid UTF-8/model
bounds, family mismatch, nonwinning codecs, checked overflow, and the 8 MiB cap.
Existing independent Journal V1 and Native Segment V1 suites remain the byte
regression authority for those unchanged formats.

## Authority and successor boundary

There is no public API or product caller. No proof byte reaches a file, store
inventory, Journal frame, Native Segment candidate, manifest/catalog, sealing,
query, runtime, recovery, retention, reclamation, or raw deletion path. Journal
V1 remains the sole canonical-admission byte grammar and Native Segment V1 still
contains exact original Journal frames plus indexes.

The proof makes no compatibility or benchmark claim. Designing any persisted
typed-value block successor requires a separately reviewed format and authority
transition; it may replace this private evidence rather than preserve it.
