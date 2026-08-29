# M02-PR01a store-scoped canonical-admission runtime

## Live outcome and accepted split

M02-PR01a is the first bounded successor after the M00-PR05 source/capture hard
stop. It accepts the necessary split of the former broad M02-PR01: this slice
changes only the live runtime command authority before any journal bytes exist.
Every `HistorianRuntime` now starts with one explicit immutable `StoreId`, and
`IngressCommand` owns exactly one already-authorized `CanonicalAdmission`. The
old bare `CollectionEnvelope` plus caller-supplied `RetryQualification` command
path and its runtime construction error no longer exist.

This is a volatile authority transition, not durable admission. M02-PR01b remains
responsible for one complete active-journal durable vertical. It must replace or
extend the active writer path coherently; it must not add a separately callable
parallel journal path that can diverge from runtime admission, retry, receipt, or
shutdown authority.

## Source ownership and exact runtime contract

`och-core::SeriesRegistry` remains the sole declaration revision, retirement,
and binding authority. `CanonicalAdmission` remains the only complete native
record that combines its registry-issued declaration snapshot with the original
validated envelope, exact request retry qualification, source schema and capture
lifecycle, and observed/no-change provenance. The runtime accepts this record as
immutable evidence; it does not consume or mutate the registry, select a historic
declaration, or reinterpret source evidence.

The runtime owns only these new decisions:

- `HistorianRuntime`, its `HistorianIngress`, read handles, and every empty,
  staged, retained-old, current, or sealed `LatestSnapshot` expose the same
  immutable `StoreId`;
- `IngressCommand::new` is infallible because core has already validated
  declaration, source, envelope, and retry scope; borrowed and consuming
  accessors preserve the complete admission;
- after acquiring the existing state authority, `try_submit` preserves `Closed`
  precedence and then rejects a foreign store as recoverable `StoreMismatch`
  before retry classification or capacity; the complete incoming admission is
  returned and slots/latest state do not change;
- outstanding-only retry comparison reads `admission.retry()` exactly;
- volatile publication reads only `admission.envelope()` and preserves the
  existing exact final-positioned-observation rules;
- declaration and source/capture evidence remain owned by the command until
  terminal completion, coalescing discard, or recoverable refusal.

The latest registry remains keyed by `SeriesId` because its enclosing runtime is
already store-scoped. Its exact `SeriesMetadata` equality check remains only a
volatile read-optimization invariant. It gains no declaration-lifecycle,
historic-binding, source-interpretation, or durability authority.

## Exact proof

The runtime tests construct complete `CanonicalAdmission` fixtures only through
public `och-core` registry, declaration binding, source/capture, observed/gap, and
no-change APIs. There is no production bypass or test-only core constructor. The
33 deterministic runtime tests include:

- exact command round-trip equality for the complete admission, including its
  declaration, source/capture lifecycle, observation lineage, and retry evidence;
- foreign-store refusal with exact admission recovery, zero slot/latest mutation,
  foreign equivalent/conflicting/distinct retry shapes at full capacity, and
  proof that `Closed` still wins;
- exact StoreId retention in empty, old, advanced, sealed, and independent-runtime
  snapshots plus runtime, ingress, and read-handle accessors;
- the complete prior lifecycle, fixed-16 ingress, retry precedence/FIFO,
  publication ordering, fixed-16 latest, graceful seal, cancellation, poison,
  failure, and abort-only Drop suite migrated to real canonical admissions
  without weakened assertions.

Strict runtime clippy, focused tests, runtime nextest, workspace doctests, and the
repository PR gate are the required exact-head evidence. The release gate is not
part of this slice.

## No durable claims

`WriterHandled` still proves only volatile command consumption and completion of
the latest-publication decision. It does not prove bytes written, stable storage,
barrier completion, durability, reopenability, retry persistence, queryability,
or restart recovery. Store-scoped snapshots remain volatile observation evidence,
not current/held values or durable history.

## Deferred ledger and M02-PR01b hard boundary

M02-PR01b must deliver a complete active-journal durable vertical before any
durable receipt is claimed. The following remain explicitly deferred:

- journal bytes, canonical parser/validation, storage layout, and filesystem I/O;
- the dedicated blocking writer thread and its bounded handoff from the caller's
  executor;
- group commit, barriers, durable cutoffs, durable receipts, and journal reopen;
- explicit byte-budget and priority admission, which the fixed command count
  does not prove;
- registry persistence and bootstrap;
- manifests, journal rotation, and seal handoff;
- recovery, corruption/torn-write handling, full-disk behavior, and durable retry
  state/horizon;
- platform and filesystem qualification.

M02-PR01b0 subsequently supplied only the exact bounded Journal V1 semantic
bytes and hostile non-authorizing decoder. The filesystem, sole-writer,
group-commit, receipt, reopen, recovery, and qualification items above remain
the M02-PR01b1 complete active-journal durable vertical; PR01b0 does not change
this record's non-durable runtime outcome.

Persistent registry/bootstrap, manifest/rotation ownership, and durable retry
must stay aligned with the planned M02-PR02 authority even when PR01b defines the
active journal needed to exercise them. Query, rollup, retention policy, Studio
adapters, and other previously recorded gaps also remain outside this slice. No
deferred behavior is implied by the new runtime input boundary, and the ignored
`_roadmap/` directory remains local and unpublished.
