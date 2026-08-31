# Transient typed-value block codec proof

## Status and authority

This document specifies only private crate-test evidence compiled by
`och-store` unit tests. The bytes below are never written, named in inventory,
published, sealed, queried by product code, or accepted as durable authority.
They are not Native Segment V1, Journal V1, a Store Format version, or a
compatibility promise. A future reviewed persisted successor may replace this
proof wholesale.

## Bounds and proof-local framing

One block carries exactly one current `ValueFamily` and `1..=256` exact values.
The complete block is at most 8 MiB. Multibyte proof fields are little-endian.

| Offset | Length | Proof-local field |
| ---: | ---: | --- |
| 0 | 4 | internal magic `TVBP` |
| 4 | 1 | internal version, exactly `1` |
| 5 | 1 | family: Real `0`, Signed `1`, Unsigned `2`, Boolean `3`, State `4`, Text `5`, Artifact `6` |
| 6 | 1 | codec: raw `0`, Boolean bit-pack `1`, Boolean RLE `2` |
| 7 | 1 | reserved zero |
| 8 | 2 | exact positive value count, at most 256 |
| 10 | 2 | reserved zero |
| 12 | 4 | exact payload length |
| 16 | variable | exact payload, with no trailing byte |

The internal magic and version are hostile-input discriminators for this test
proof only; they do not allocate a durable format identity or version lineage.

## Raw payload

For every non-Boolean family, each record starts with one marker: `0` means an
available family value, `1` means unavailable without reason, and `2` means
unavailable followed by a positive `u16` byte length and exact reason bytes.
Boolean raw records use one byte for `false` (`0`), `true` (`1`), unavailable
without reason (`2`), or unavailable-with-reason (`3`, then the same reason
length/bytes). Other marker values are invalid.

Available non-Boolean bodies are:

| Family | Exact raw body after marker `0` |
| --- | --- |
| Real | `u64` from `RealBits::to_bits` |
| Signed | full `i64` |
| Unsigned | full `u64` |
| State | `u16 class length + class bytes + u16 member length + member bytes` |
| Text | `u32 UTF-8 byte length + exact bytes` |
| Artifact | 16 UUID bytes + `u16` format length/bytes + full `u128` version + 32 supplied digest bytes |

Decode applies the current model constructors and bounds. It performs no Unicode
normalization, floating-point operation, content fetch/hash, or semantic token
interpretation.

## Boolean compact payloads

Compact codecs are eligible only when every value is an available Boolean.

- **Bit-pack:** payload byte zero is the fixed LSB-first marker `1`. Remaining
  bytes hold value zero in bit zero of byte zero, then ascending bits/bytes. All
  unused high bits in the final byte must be zero.
- **RLE:** payload byte zero is the fixed `u16` run-width marker `2`. Each record
  is one Boolean byte (`0` or `1`) followed by a positive little-endian `u16`
  run. Runs must alternate values and their checked sum must equal the header
  count exactly.

Zero or over-count runs, adjacent equal runs, invalid Boolean bytes, count
mismatch, truncation, and trailing bytes refuse.

## Deterministic selection and hostile decode

Selection compares complete framed lengths after checked arithmetic. Raw wins a
tie with either compact candidate. A compact candidate must therefore be
strictly smaller than raw. If both compact candidates have the same winning
length, bit-pack wins. Selection allocates no candidate buffer; only the chosen
output is allocated.

Decode first checks the complete 8 MiB cap and every fixed header field, family,
count, declared payload length, and exact total length. It then allocates at most
256 result entries, validates and reconstructs exact values, recomputes the same
winner, and rejects a syntactically valid nonwinner. Failures expose only the
closed proof-private classifications `InvalidBlock`, `Bounds`, and
`FamilyMismatch`, without paths or attacker-controlled text.

## Non-contracts

This evidence says nothing about compression ratios outside the checked
fixtures, future codec choice, persistence, compatibility, Native Segment bytes,
canonical admission framing, store authority, query execution, runtime behavior,
retention, reclamation, compaction, or platform I/O.
