# M03-PR03a transient typed-value block codec proof implementation brief

## Objective and owner decision

Prove, only inside `och-store` crate tests, one bounded typed-value block codec
for exact raw values in every current `ValueFamily` and deterministic Boolean
bit-pack/RLE selection. The owner selected this transient proof knowing that a
future reviewed persisted block format may replace it.

This is design evidence, not a product API, persisted format, Native Segment V1
revision, Journal V1 alternative, compatibility promise, or performance claim.
Journal V1 remains the sole canonical-admission byte grammar. Native Segment V1
continues to retain complete original Journal V1 frames plus non-authorizing
indexes.

## Input and exact-value boundary

The private encoder accepts exactly one `ValueFamily` and a nonempty slice of at
most `och_core::MAX_SOURCE_OBSERVATION_CONTEXTS` (256) `ExactValue`s. Every value
must pass `ValueFamily::admits`; `Unavailable` is therefore accepted under every
family. Total proof bytes are capped at the existing 8 MiB
`MAX_ADMISSION_PAYLOAD_V1` bound.

Raw proof records retain only exact values:

- every `RealBits::to_bits` pattern, including signed zero, subnormals,
  infinities, and NaN payloads, without floating-point arithmetic;
- full signed and unsigned 64-bit values;
- exact Booleans, bounded state class/member tokens, and unnormalized UTF-8;
- nominal artifact UUID plus the exact supplied format token, full `u128`
  version, and supplied 32-byte digest, without content access or hashing; and
- unavailable with an exact optional bounded reason, distinct from absent
  observations, gaps, and no-change evidence.

No observation, time, quality/status, producer position, identity scope,
declaration, provenance, collection, lifecycle, retry, gap, no-change, or Journal
frame field enters this proof.

## Boolean and canonical-selection boundary

Only a Boolean-family block made entirely of available Boolean values is compact
eligible. Bit-pack uses least-significant-bit-first order and zero unused bits.
RLE uses alternating `(Boolean, positive u16 run)` records whose exact sum equals
the count. Any unavailable value forces raw.

The encoder computes checked complete lengths for raw, packed, and RLE before
allocating only the selected output. A compact codec must be strictly smaller
than raw including the common proof framing. Raw wins every tie involving raw;
bit-pack wins a packed/RLE tie. The decoder validates the hostile framing and
bounds before allocating at most 256 output values, reconstructs exact model
values, recomputes this selection, and refuses a valid but nonwinning codec.
Errors are the closed input-free set `InvalidBlock`, `Bounds`, and
`FamilyMismatch`.

The exact proof-local grammar and refusal law are recorded in
[Transient typed-value block codec proof](typed-value-block-codec-proof.md).

## Implementation and independent evidence boundary

- `crates/och-store/src/typed_value_block.rs` is reachable only through a
  `#[cfg(test)]` private module in `och-store`'s library tests.
- `crates/och-store/tests/support/typed_value_block_oracle.rs` is an independently
  authored standard-library primitive oracle. Its source imports neither product
  crate, and a test enforces that separation.
- Deterministic unit evidence covers all raw domains and unavailable under every
  family, real edge bits, integer extrema, Unicode distinctions, state and
  artifact identity, all three Boolean winners/ties, repetition, hostile fields,
  malformed compact records, model bounds, noncanonical selection, count and
  byte caps, and checked arithmetic.
- Existing independent Journal V1 and Native Segment V1 tests remain explicit
  regressions and no golden in either format changes.

## Explicit exclusions

There is no public or production symbol, dead-code allowance, file I/O, format
inventory identifier, sealing/build/query/runtime caller, durable authority,
publication, recovery, compatibility decoder, migration, compaction, retention,
reclamation, raw deletion, dependency, unsafe/SIMD code, float algebra,
dictionary, delta codec, cross-block state, or benchmark/future-winner claim.
No `och-core`, Journal, Native Segment, Manifest, Catalog, generation, runtime, or
Cargo dependency file changes are permitted by this slice.
