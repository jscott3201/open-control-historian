# Continuation note: M00-PR03

M00-PR02 stops after the reviewed dependency-free canonical model and its focused
constructor/invariant tests. M00-PR03 owns independent oracle, golden, and fixture
builder evidence for that public contract; it must not pretend that tests calling
the implementation's own comparison or validation logic are independent.

## Frozen inputs to the next slice

The independent evidence must cover the public semantics in
[the model contract](model-contract.md), including:

- canonical lowercase RFC 9562 `UUIDv7` text/bytes and nominal family separation;
- exact `RealBits`, integer extremes, text/token bounds, unavailable content, and
  external content identity;
- normalized negative and positive timestamps plus exact checked milliseconds;
- independent quality/native status and full-range canonical producer numbers;
- the raw tuple `(effective, receive, observation ID)` separately from producer
  position order;
- all five immutable collection modes and interval metadata rules;
- half-open no-change and gap boundaries;
- 256-observation and 64-gap maxima plus every atomic rejection class;
- the exact Equivalent/Conflict/Distinct retry matrix and key redaction.

Fixture builders should make valid cases explicit and make one invariant fail at
a time. An independent oracle must compute expected ordering and validation from
the written contract rather than delegate to `CollectionEnvelope::observed`,
`Observation::raw_order_key`, or `RetryQualification::classify`. Golden evidence
must be deterministic, reviewable, platform-neutral, and explicit about its own
schema; it is not a Historian wire or persistence compatibility promise.

## Still excluded

M00-PR03 does not own runtime lifecycle, durable retry state, persistence or wire
formats, serialization dependencies, hashing, storage/query behavior, adapters,
UUID generation, Studio/Engine integration, donor code, or platform services.
If independent evidence exposes an ambiguity or contradiction in the frozen
contract, stop and request a reviewed model decision rather than encode an
assumption in fixtures.
