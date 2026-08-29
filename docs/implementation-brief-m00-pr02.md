# M00-PR02 canonical model implementation brief

## Outcome

This delivery replaces the empty `och-core` anchor with the dependency-free
canonical model documented in [the model contract](model-contract.md). It defines
exact native semantics and bounded atomic validation while leaving lifecycle,
storage, persistence, query, wire, and adapter choices unmade.

## Included

- four nominal parse-only RFC 9562 `UUIDv7` identity families;
- exact real bits, signed/unsigned integers, Boolean, state, text, artifact, and
  unavailable values;
- external format/version/SHA-256 content identity without hashing behavior;
- normalized signed Unix seconds/nanoseconds and checked exact millisecond helpers;
- closed quality levels, independent flags, and ordered opaque native status;
- full-range canonical decimal producer epoch/sequence and explicit positions;
- immutable five-mode series metadata and the exact raw observation order tuple;
- non-empty half-open interval, sequence-gap, and change-only no-change evidence;
- bounded atomic envelope validation with private invalid states;
- explicit scoped, content-qualified retry comparison and redacted keys;
- deterministic boundary, hostile-input, ordering, mode, collection, and retry
  tests, plus a compile-fail nominal-family doctest;
- current README, architecture, agent instructions, hygiene policy, and a precise
  M00-PR03 evidence handoff.

All constructors return the closed sanitized `ModelError` without retaining
hostile input. Collection bounds are checked before secondary validation
allocation; accepted strings and vectors are deterministically compacted through
boxed ownership so retained capacity equals logical length; accepted traversals
remain linear or tightly bounded.

## Excluded

Runtime/tasks/channels, storage/journal/segments, persistence and wire formats,
serde, hashing implementations, query engines, Arrow/Parquet/DataFusion/Flight,
gRPC/protobuf, SQL/databases, cloud/object providers, memory mapping, Studio or
Engine dependencies, adapters, UUID generation, content canonicalization, retry
durability, and donor code remain absent.

## Acceptance commands

Focused model evidence:

```console
cargo +1.98.0 test -p och-core --locked
cargo +1.98.0 nextest run -p och-core --locked --profile ci --no-tests=fail
cargo +1.98.0 test --workspace --doc --locked
```

Dependency and canonical repository evidence:

```console
cargo +1.98.0 run --locked -p och-policy -- check --manifest-path Cargo.toml
./scripts/gate.sh pr
./scripts/gate.sh release
git diff --check
```

Nextest remains the primary runner and doctests remain separate. M00-PR03, not
these implementation tests, owns independent oracle and golden evidence.
