# Continuation note: M01-PR03 latest publication and snapshots

M01-PR02 stops after bounded volatile command handling. A `WriterHandled` receipt
means only that the one private writer consumed and dropped an accepted envelope;
it exposes no latest state and proves no persistence, durability, or query result.

M01-PR03 may add a separately reviewed in-memory registry/latest publication
contract and snapshot/read handles. Before implementation it must own exact series
keying, publication eligibility, ordering authority, atomic visibility, immutable
snapshot shape, reader/writer races, bounds, shutdown behavior, and recovery from
partial publication failure. It must preserve frozen core envelope semantics and
must not infer latest order from arrival, timestamps, UUID order, or absent
producer evidence.

That slice must not change the 16-command ingress bound, outstanding-only retry
window, duplicate-envelope discard, receipt outcomes, caller-owned executor, or
single private writer. It also must not imply persistence, restart recovery,
durable retry/history, storage/journal formats, query planning, wire/serialization,
network services, adapters, or same-series evidence replacement without an
explicitly reviewed contract.
