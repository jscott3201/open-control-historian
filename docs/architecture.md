# Canonical native-model architecture

## Present topology

M00-PR01 established two workspace roles and one product boundary; M00-PR02 fills
that native boundary with the reviewed dependency-free canonical model:

```text
default workspace selection
        |
        v
  och-core (native)       och-policy (tooling)
  canonical model         cargo_metadata + parsing support
  no dependencies
        ^
        |
  future adapters (not created yet)
```

[`och-core`](../crates/och-core/) owns exact platform-independent contracts for
identity, values/content, time, quality/status, producer ordering, collection
modes, interval/gap/no-change evidence, bounded atomic envelopes, and retry
comparison. It retains no product dependencies. Its only executable remains a
baseline example used to verify buildability and measure a native binary bound;
that example is not a runtime or supported product command.

[`och-policy`](../tools/och-policy/) is private repository tooling. It appears in
the full workspace so clippy and tests cover it, but root `default-members`
selects only `och-core`. Consequently the tool's Cargo metadata/parsing
dependencies do not masquerade as native product dependencies.

## Direction and ownership

Package roles are explicit rather than inferred from directory names:

- **native** owns platform-independent product contracts and implementation;
- **adapter** will own edge/platform integration and may depend inward on native;
- **tooling** owns repository policy, generation, or validation and is outside
  the product closure.

The permitted future product edge is `adapter -> native`. A `native -> adapter`
or `native -> tooling` path is a dependency inversion and fails policy. Adapters
also cannot be selected implicitly through workspace defaults. No placeholder
adapter crates exist today because an empty package would imply unsupported
platform scope without proving behavior.

Within `och-core`, modules follow semantic ownership rather than runtime layers:

- `identity`, `bounded`, and `value` retain exact validated primitives;
- `time`, `quality`, and `position` retain independent evidence domains;
- `observation` defines immutable series modes, observations, and raw order;
- `collection` performs bounded atomic cross-item validation;
- `retry` compares explicit scope, key, and external content identity;
- `error` exposes only closed sanitized validation failures.

Invalid scalar ranges are excluded by constructors. Invariants involving series
mode or multiple items are enforced only by `CollectionEnvelope`, whose evidence
fields are private. The model does not create IDs, hash bytes, infer time or
producer order, infer held values/deltas/resets, or translate native extensions.
See the [canonical model contract](model-contract.md).

## Intentionally absent

There is currently no runtime, task/channel system, journal, segment, store,
persistence or wire format, query engine, network service, SQL layer,
cloud/object provider, embedded database, memory mapping, Studio/Engine link,
adapter, or donor-code compatibility layer.

Those omissions keep the reviewed canonical model independent of lifecycle and
platform choices and prevent large implementation dependencies from becoming
architectural facts before their contracts are reviewed. M00-PR03 is limited to
independent model evidence; see [its continuation note](continuation-m00-pr03.md).
