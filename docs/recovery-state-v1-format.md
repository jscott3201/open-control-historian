# Recovery State V1 format

Recovery State V1 is bounded, non-authorizing event evidence for one current-only
Manifest V1 recovery transaction. It records removal of a proven terminal
invalid/torn active-journal suffix. It never reconstructs registry, retry, latest,
receipt, declaration, or checkpoint authority.

## Inventory and manifest reference

The three reusable finals are:

- `recovery-state-v1-slot-0.och`;
- `recovery-state-v1-slot-1.och`;
- `recovery-state-v1-slot-2.och`.

Publication uses `recovery-state-v1.staging`. Manifest V1 bytes `116..124` are
the tagged optional reference: byte `116` is `0` absent or `1` present, byte
`117` is slot `0..2` when present, bytes `118..120` are zero, and bytes
`120..124` are CRC-32C over the complete 128-byte report. Canonical absence is
eight zero bytes. A report is authority only after a manifest with that exact
slot/checksum commits.

Both retained manifests protect their referenced slots. Ordinary manifest
commits preserve the latest reference. A recovery changes it only from absent to
report generation one or from report generation `R` to different-slot `R+1`.
Present to absent refuses. Strictly older unreferenced canonical reports may be
removed only after all authority is proven.

## Exact 128-byte record

All integers are unsigned big-endian.

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHRCV01` |
| 8 | 2 | version | unsigned `1` |
| 10 | 2 | record length | unsigned `128` |
| 12 | 16 | store identity | exact canonical `StoreId` |
| 28 | 8 | report generation | positive |
| 36 | 8 | source manifest generation | positive |
| 44 | 8 | committing manifest generation | exactly source plus one |
| 52 | 4 | source manifest CRC-32C | source Manifest V1 checksum field |
| 56 | 8 | active journal generation | positive |
| 64 | 8 | active exclusive sequence floor | exact source root floor |
| 72 | 8 | checkpoint generation | positive, retained unchanged |
| 80 | 8 | append sequence | source root inclusive cutoff |
| 88 | 8 | committed end offset | source root frame boundary |
| 96 | 8 | original journal length | greater than committed end and at most 512 MiB |
| 104 | 8 | removed bytes | positive original length minus committed end |
| 112 | 1 | classification | closed tag below |
| 113 | 1 | action | `1`, truncate to committed root |
| 114 | 10 | reserved | zero |
| 124 | 4 | record CRC-32C | bytes `0..124` |

Classification tags are closed: `1` short frame prefix, `2` invalid exact frame
prefix, `3` truncated declared frame, and `4` invalid complete frame ending
exactly at EOF. Unknown tags refuse. Decode uses a fixed 128-byte bound, checked
arithmetic, canonical re-encoding, exact store scope, zero reserved bytes, and no
untrusted allocation.

## Recovery and convergence law

Under the stable store lock and retained active-journal lock, open first proves
both manifests, registry replay/re-encoding, retry roots and embedded commits,
catalog progression, sealed metadata/header inventory, active inventory/header,
both checkpoint slots, exact source cutoff, historical declarations, and every
referenced report. The root-aware journal scan is read-only until those checks
complete.

Only one terminal shape may proceed. A short prefix must match every available
fixed magic/version/kind/flags/next-sequence byte; known prefix corruption is not
a torn-write proof. Any complete valid post-root frame,
valid-plus-torn bytes, malformed prefix with extra bytes, interior corruption,
identity or sequence mismatch, checkpoint disagreement, later candidate, or
ambiguity refuses without mutation. Recovery then:

1. writes, synchronizes, reads back, decodes, renames, and directory-synchronizes
   an unreferenced report slot;
2. truncates exactly to the selected manifest end offset and synchronizes the
   journal without changing the checkpoint;
3. publishes the otherwise byte-identical next Manifest V1 to the alternate slot;
4. adopts only after manifest readback/publication/directory synchronization, then
   performs bounded reusable-slot cleanup.

A complete report staging file can finish its deterministic report publication.
An unreferenced exact next final plus the original suffix resumes truncation; the
same final plus a journal already at cutoff re-synchronizes the journal before it
resumes the manifest. Complete staging is synchronized again before rename;
complete exact manifest staging after truncation can finish. A committed final is selected as
the newest root. Partial/malformed staging, duplicate/future/mismatched intent, or
any other shape refuses unchanged. Reopening an already committed recovery does
not repeat truncation or advance a generation.

## Diagnostics and limits

Store and runtime inspection expose only immutable `RecoveryReport` fields and a
sanitized additive open classification. The report is the latest durable event;
it is not proof that recovery occurred during the current open. Successful
recovery remains runtime `Healthy`. No path, payload, raw canonical content,
handle, unbounded string, or unbounded report history is exposed.

This contract is based on standard-library file synchronization, same-directory
rename, directory synchronization, and retained process locks. It does not claim
universal power-loss behavior, Windows semantics, physical free-space behavior,
or safety against an adversarial concurrent directory writer.
