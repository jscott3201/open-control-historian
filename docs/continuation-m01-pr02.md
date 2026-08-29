# Continuation note: M01-PR02 bounded ingress

M01-PR01 stops at caller-executor lifecycle. The writer task and its lifecycle
channels remain private, and successful readiness or shutdown says nothing about
command acceptance, data draining, persistence, or durability.

M01-PR02 may add a separately reviewed bounded ingress contract that connects the
frozen `och-core` model to the private writer. Before implementation it must own
and test exact command shape, capacity, backpressure or coalescing behavior,
acceptance and receipt meaning, cancellation, shutdown races, and retry/failure
semantics. That slice may add the direct `och-runtime -> och-core` native edge but
must preserve the exact direct Tokio exception and caller-owned executor.

M01-PR02 must not infer public status observation, latest-value publication,
snapshots, registry/read handles, persistence, storage/journal formats, query,
wire/serialization, adapters, or restart behavior. Those remain separate future
contracts. It must retain one private writer per runtime instance and must not
expose task handles, Tokio senders, queue internals, writer state, task identity,
or executor handles.
