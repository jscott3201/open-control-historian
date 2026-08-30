# Generation Catalog V1 format

Generation Catalog V1 is the bounded Manifest V3 authority for immutable sealed
raw-Journal generations. It is not a directory listing, recovery event log, or
retention index. `och-store` is its sole mutator. All integers are big-endian and
all checksums use the Journal V1 CRC-32C parameters.

## Artifacts and bound

Three reusable final slots are recognized:

- `generation-catalog-v1-slot-0.och`;
- `generation-catalog-v1-slot-1.och`;
- `generation-catalog-v1-slot-2.och`.

`generation-catalog-v1.staging` is the only catalog staging name. A candidate
uses a slot unreferenced by both valid manifests. A Manifest V3 reference carries
slot, positive catalog generation, complete artifact length, and CRC-32C over the
complete artifact including its own trailing CRC. At most 64 entries are retained;
the exact largest artifact is 4,164 bytes. Entry 65 refuses without overwriting,
deleting, or reclaiming history.

## Header

The 64-byte header is followed by fixed 64-byte entries and a four-byte CRC-32C
over header plus entries:

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHCAT01` |
| 8 | 2 | version | unsigned `1` |
| 10 | 2 | header length | unsigned `64` |
| 12 | 16 | store identity | Manifest `StoreId` |
| 28 | 8 | catalog generation | positive; equals retained entry count |
| 36 | 4 | entry count | `1..64` |
| 40 | 8 | payload length | exactly `count * 64` |
| 48 | 16 | reserved | zero |

The filename supplies the reusable slot. Decode checks slot `0..3`, exact total
length, count and payload relationships, complete checksum, store scope, and
canonical re-encoding before allocation or authority adoption.

## Fixed entry

Each entry is exactly 64 bytes:

| Offset within entry | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | journal generation | positive and contiguous from `1` |
| 8 | 8 | exclusive sequence floor | first entry exactly `0` |
| 16 | 8 | inclusive sequence cutoff | strictly greater than floor |
| 24 | 8 | source durable end offset | greater than the 28-byte header |
| 32 | 8 | registry generation | positive authority covering this range |
| 40 | 8 | sealed artifact length | equals source end; at most 512 MiB |
| 48 | 4 | sealed artifact checksum | CRC-32C over complete raw artifact |
| 52 | 2 | sealed format | `1` = raw Journal V1 with header version 2 |
| 54 | 10 | reserved | zero |

Entries are sorted by journal generation. Each generation is the exact successor
of its predecessor and each prior inclusive cutoff equals the next exclusive
floor. The catalog retains all prior entries byte-for-byte when it appends one
new entry; no update, reorder, hole, overlap, or replacement is canonical.
Across the retained manifest pair, that appended entry must also equal the older
manifest's journal generation, active sequence floor, durable sequence/end,
artifact length, and registry generation. The newer manifest must preserve the
registry/retry references and bind the exact empty checked successor with local
checkpoint generation one. This proof applies to first and later rotation even
after the intent has been removed.

## Publication and open

After the sealed artifact and empty successor are synchronized, the candidate is
counted, written to the fixed staging name, synchronized, bounded-read back,
decoded and exact-compared, renamed over the selected slot, and directory-synced.
Manifest V3 publication is the later commit point. A catalog is not authority
until named by a valid manifest.

Normal open reads each referenced catalog artifact and validates the length and
header of each named sealed file. It does not checksum or decode every sealed
payload byte. Full streaming checksum/framing/declaration verification occurs
before catalog and manifest authority are published. Missing, foreign,
noncanonical, excessive, future, forked, unrelated, or ambiguous catalog
evidence refuses. The sole unreferenced exception is a canonically decoded
strict prefix of a referenced newer catalog left by interruption after an
ordinary manifest commit; open verifies the exact prefix and root law before
removing it idempotently.
