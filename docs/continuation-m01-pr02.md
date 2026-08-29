# M01-PR02 bounded ingress implementation and evidence record

## Delivered outcome

M01-PR02 connects frozen `och-core::CollectionEnvelope` and
`RetryQualification` values to each `och-runtime` private writer. It adds the
ordinary native `och-runtime -> och-core` dependency while preserving the exact
Tokio 1.53.1 declaration with defaults disabled and only `rt` plus `sync`. The
native union closure remains two roots and four packages because core was already
a root.

## Public contract

- `IngressCommand` owns exactly one envelope and retry qualification. Construction
  checks equal series and producer scope. A mismatch is sanitized and recovers
  both inputs. The runtime never hashes or verifies caller-supplied
  `ContentIdentity` against envelope bytes.
- `HistorianRuntime::ingress` returns a custom cloneable `HistorianIngress` with
  synchronous `try_submit`. It exposes no Tokio type and does not keep the writer
  alive. An outliving clone rejects with recoverable `Closed`.
- `MAX_OUTSTANDING_COMMANDS` is 16 and includes queued plus in-flight distinct
  retry qualifications. First distinct admissions are FIFO. Submission never
  waits and promises no fairness among racing callers.
- Under one short private admission lock, open ingress compares every outstanding
  retry qualification before checking capacity. Exact core `Equivalent` retries
  discard the incoming whole envelope and share the first receipt; `Conflict`
  rejects without replacement; `Distinct` consumes a free slot or returns `Full`.
- `SubmissionDisposition` distinguishes `Queued` and `Coalesced`. Cloneable
  receipts terminate exactly once as `WriterHandled` or `WriterStopped`.
  `WriterHandled` means only that the private volatile writer consumed and dropped
  the command. Receipt cancellation never cancels work.

The retry window exists only while the original is queued or in flight. Terminal
completion atomically releases the slot/key, so the same qualification can become
new work afterward. There is no retry history, durable horizon, restart
deduplication, same-series latest replacement, envelope merge, split, or
reconstruction.

## Bound and lifecycle implementation

Each runtime preallocates a 16-entry private slot table and uses a fixed 16-entry
FIFO index ring. A single `Notify` wakes the sole writer. Each accepted slot owns
one fixed atomic terminal state; equivalent storms add neither slots nor a runtime
waiter vector. Receipt wait futures are caller-owned. Notification registration,
queue inspection, and terminal atomics close lost-wakeup races, and caller wakers
are notified only after the admission mutex is released.

Startup publishes no ingress before writer state and admission are ready.
Graceful `shutdown(self)` closes admission under the same lock as submission,
drains all earlier accepted work, resolves receipts handled, and then joins.
Runtime Drop and cancelled shutdown close admission, resolve all unresolved work
stopped, and abort without blocking or detaching. Work-item and writer failure
guards make early exit, cancellation, panic, and mutex poison idempotently fail
closed. Terminal state is single-assignment, so later shutdown cannot overwrite
handled work.

## Deterministic evidence

Current-thread tests use private one-shot gates and bounded yields without clocks,
sleep, randomness, network, filesystem, or external processes. They cover:

- scope mismatch sanitization, exact recovery, and zero slot use;
- pending receipts, multiple shared waiters, FIFO handling, and receipt-wait
  cancellation;
- all 16 slots, recoverable seventeenth `Full`, equivalent-at-full precedence,
  conflict rejection, and independent same-series/different-key work;
- 1,024 equivalent submissions retaining one runtime slot/work item/state and
  discarding each duplicate envelope;
- equivalent/conflict classification while in flight and key release only after
  terminal completion;
- submit-before-close acceptance, submit-after-close recovery, cloned-handle
  closure, complete drain before joined shutdown, runtime Drop, and cancelled
  shutdown;
- handled-state preservation plus stopped pending/in-flight receipts on early
  exit, abort, panic, poison, and ordinary Drop;
- two isolated runtime instances and repeated hostile full/equivalent/conflict/
  close sequences with the fixed bound unchanged.

## Excluded

There is no persistence, durability, journal/storage/query/wire behavior, content
hashing, durable receipt/history, restart recovery, capacity configuration,
blocking/async admission wait, public queue metrics, sender/task/executor exposure,
state publication, registry, latest replacement, snapshot, or read handle. The
[M01-PR03 continuation](continuation-m01-pr03.md) owns the next publication/read
contract without changing these ingress meanings.
