# M02-PR01b1 implementation brief: active-journal durability

## Outcome and authority transition

M02-PR01b1 makes the reviewed Journal V1 format the only public runtime writer
path. One `HistorianRuntime::open(StoreOptions)` owns one immutable `StoreId`, one
Tokio coordinator, one dedicated blocking store writer, one fixed reaper, and one
bounded generation-one active journal. The prior public volatile start path is
removed. This is a storage/lifecycle authority transition, not a core-model one:
`och-core` and every canonical semantic remain unchanged.

The accepted flow is:

```text
CanonicalAdmission
  -> exact nonallocating Journal V1 length
  -> atomic count/class/global byte reservation
  -> prepared frame allocation
  -> FIFO blocking append
  -> volatile latest publication decision / Handled
  -> group barrier and mechanical checkpoint / Durable
  -> reservation release
```

No public path accepts a bare envelope, caller-selected declaration, decoded
journal record, or non-durable parallel writer.

## Configuration and bounds

`StoreOptions` validates an existing directory, exact `StoreId`, create-new or
open-existing mode, `ActiveJournalLimits`, `ByteReservationLimits`, and
`GroupCommitPolicy` before worker I/O. Journal V1 payload remains hard-bounded at
8 MiB; the active journal at 512 MiB and 4,096 records; and the store path at
4,096 encoded bytes. Callers may only narrow those limits. Runtime count capacity
remains exactly 16 distinct outstanding commands. Runtime options check the
borrowed encoded path length before cloning or retaining the caller's `PathBuf`.

Byte admission is explicit and exact. Protected work may use the global ceiling;
normal work cannot consume the protected reserve; bulk work cannot consume the
protected or normal reserves. A frame-length traversal performs no frame
allocation. A new command reserves its exact length atomically before allocation,
then preparation proves that encoded length. Slots and bytes remain retained
through pending durability. Priority changes reservation and barrier demand only;
it never changes FIFO order or canonical meaning.

Group commit has explicit nonzero maximum delay, record, and byte thresholds plus
active-session age demand. Record demand cannot exceed 16 and group bytes cannot
exceed the global outstanding-byte ceiling. Protected or immediate work demands
a barrier. Active byte/record/age exhaustion reports rotation required; PR01b1
does not publish a successor generation.

## Store ownership and durable order

`och-store::ActiveJournal` is the sole synchronous owner of the retained locked
journal and checkpoint handles. The active generation and artifact names are
fixed. The journal uses one read/write handle with explicit end seeks, never
append mode. The blocking worker alone assigns strict monotonic append sequence
and compares both admission and declaration StoreId with the journal header.

Create-new exclusively creates and synchronizes the header, generation-one
genesis checkpoint, and directory entries before readiness. Each later durable
barrier is ordered exactly:

1. append complete Journal V1 frames;
2. synchronize the journal;
3. write the alternate checkpoint slot;
4. synchronize the checkpoint;
5. publish the cutoff, resolve durable receipts, and release reservations.

The two 64-byte checkpoint slots carry only version, store/journal identity, slot
generation, append sequence, end offset, and CRC-32C. They never carry registry,
retry, declaration, source, or latest authority. `DurableCutoff` exposes the
checkpoint slot generation separately from journal generation. Consecutive valid
slots must strictly advance append sequence and end offset.

## Reopen and failure law

Open-existing retains the process-safe journal lock and validates the fixed
header/checkpoint layout before bounded scan. It scans no more than configured
bytes, records, or payload length and allocates only after prefix bounds. Every
decoded frame and declaration StoreId must equal the header. Corruption before
the durable cutoff refuses without mutation. Any invalid nonzero checkpoint slot
refuses instead of falling back ambiguously. Header-only interrupted genesis may
be initialized, including safe creation of its missing checkpoint after the exact
header is validated under lock. An existing zero-byte checkpoint may be initialized
under the same exact header-only rule; every nonzero wrong length refuses without
mutation. Otherwise a nonempty or invalid journal without a checkpoint refuses
without creating one.

A proven valid suffix beyond an unambiguous checkpoint is synchronized and
checkpointed before readiness. Only a proven terminal invalid unacknowledged
suffix may be truncated; a complete malformed frame with later bytes refuses
unchanged. Every truncation is synchronized before readiness. Recovered values are
bounded `DecodedAdmissionV1` inspection evidence. They cannot be submitted,
fabricate registry history, seed a completed durable retry cache, or rebuild
latest; latest always restarts empty.

Write, journal-sync, checkpoint-write, checkpoint-sync, publication, task, and
worker failures stop admission, resolve outstanding stages truthfully, and never
advance a false durable cutoff. Generic I/O evidence retains only operation,
`ErrorKind`, and optional raw OS error—never paths or caller content. Graceful
shutdown closes admission, drains FIFO, forces a final barrier, seals latest, and
joins coordinator and worker. Drop/cancellation signals fail-stop without joining;
the fixed reaper owns eventual worker join and lock release. Every coordinator
failure uses the same nonblocking stop plus worker-wake path, so a retained runtime
sender cannot strand the blocking worker or its file lock. An append I/O failure
that may have changed bytes terminally poisons the open `ActiveJournal`; it refuses
later sequence assignment, append, and synchronization until drop and validated
reopen.

## Receipt and inspection contract

`wait_handled` returns append identity only after append plus the existing exact
volatile publication decision. The legacy `wait` is an explicitly non-durable
handled alias. `wait_durable` returns journal identity, append identity, and a
covering durable cutoff plus its mechanical checkpoint generation only after the
ordered barrier. Equivalent outstanding retries share both stages. A group
timeout cannot advance over a frame still awaiting publication acknowledgement;
after acknowledgement an elapsed deadline may flush immediately. Cancellation of
a receipt does not revoke accepted work. A failed preparation stops and wakes any
receipt that coalesced while the slot was preparing and releases its exact bytes.

Path-free inspection exposes store/journal identity, active bytes/records,
append and durable cutoffs, pending count/bytes, successful sync count, and
healthy/rotation-required/faulted/stopped status. It does not expose paths,
values, retry keys, or source content.

## Evidence and exclusions

Focused evidence must cover create/open/identity/layout/locking, count and byte
boundaries, FIFO and retry precedence through durability, all barrier triggers,
handled-before-durable staging, shutdown and cancellation, store fault points,
checkpoint/reopen/truncation/corruption, StoreId scope, redaction, real
cross-process locking, and process kill after handled and durable stages. The full
PR gate remains required; the release gate is outside this slice.

This PR deliberately excludes `och-core` changes, registry persistence/bootstrap,
manifests, successor rotation, immutable segments, physical reclamation, broad
recovery events, long-term retry history, query, adapters, Studio/Engine changes,
new dependencies, Tokio feature widening, and universal physical-power-loss
claims.
