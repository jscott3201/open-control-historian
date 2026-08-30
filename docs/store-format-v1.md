# Store Format V1

Store Format V1 is the root reset-epoch fence for every durable artifact family.
It is not migration metadata and does not make arbitrary colocated artifacts
current. New stores publish only this epoch; stores created before the reset are
unsupported and are never upgraded, reset, deleted, truncated, or cleaned by
open.

## Marker bytes

The final marker is `store-format-v1.och`. Publication uses the same-directory
`store-format-v1.staging` name and the existing exclusive-create, synchronize,
bounded readback, exact decode, rename, and directory-sync discipline.

The marker is exactly 32 bytes and all integer fields are big-endian:

| Offset | Length | Field | Contract |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `OCHFMT01` |
| 8 | 2 | version | unsigned `1` |
| 10 | 2 | record length | unsigned `32` |
| 12 | 16 | store identity | exact canonical `StoreId` |
| 28 | 4 | CRC-32C | bytes `0..28` |

Read is exact and bounded to 32 bytes. Wrong magic, version, length, identity
scope, checksum, canonical identity, truncation, or trailing bytes returns the
path-free `ManifestStoreError::UnsupportedStoreFormat`.

## Preflight and refusal

`ManifestStore::open` performs a bounded read-only inventory before creating or
opening `store-v1.lock`. `OpenExisting` requires a valid final marker, except that
the exact interrupted state containing only the stable lock and a valid marker
staging file may finish the marker rename. `CreateNew` accepts an empty directory
or an exact provable current marker/genesis publication window. It never
overwrites a nonempty unsupported directory.

The marker is necessary but not sufficient. Before lock acquisition, production
also checks every present manifest is the fixed 160-byte Manifest V1, every active
journal carries Journal Header V1, and every retry artifact is the always-extended
Retry State V1 layout. Present Recovery State artifacts are also bounded to exact
128-byte V1 records and the configured `StoreId`; preflight recognizes their
format but does not authorize them. A forged valid marker paired with historical, malformed,
or mixed artifacts is unsupported.

When no manifest exists but the generation-one active journal does, a separate
read-only proof runs before the active-journal writer lock. The journal must be
exactly the 28-byte current header with the configured `StoreId` and no suffix.
Its checkpoint must be absent, zero-length, or the exact canonical generation-one
genesis slot followed by the zero alternate slot. Any suffix, non-genesis cutoff,
foreign scope, malformed/trailing checkpoint, or ambiguous slot refuses unchanged;
only the absent and zero-length checkpoint cases may then be initialized.

After preflight, the stable lock is acquired and inventory and marker validation
are repeated. Only current genesis publication, current reusable-slot cleanup,
the narrow existing rotation transaction, and the manifest-rooted terminal-suffix
recovery transaction may mutate. The recognized inventory maximum is 91: the
prior 87 plus three Recovery State finals and one staging name. A rejected directory is
left byte-for-byte and name-for-name unchanged. Error displays contain operation
classes and standard I/O kinds where applicable, never paths or artifact content.

## Crash boundary

If marker publication fails before a complete marker exists, reopen returns the
same typed refusal without mutation. If the exact complete staging marker or
final marker exists with the stable lock and no conflicting evidence, reopen can
finish publication and current genesis. Later genesis staging files are never
guessed: unpublished staging refuses unchanged, while already renamed canonical
registry, retry, or manifest finals can be validated and completed.

Recovery staging/finals are not authority without a committed Manifest V1
reference. Under both retained locks, one complete exact next intent may converge;
partial, malformed, duplicate, future, mismatched, or ambiguous intent refuses
unchanged. Cleanup removes only canonically decoded, StoreId-matched, strictly
older unreferenced reports after all authority is proven.

This is a process/filesystem ordering contract based on the repository's standard
file I/O and directory synchronization discipline. It is not a universal
power-loss, adversarial-directory-writer, Windows, or physical-media guarantee.
