# Foundation architecture

## Present topology

M00-PR01 establishes two workspace roles and one product boundary:

```text
default workspace selection
        |
        v
  och-core (native)       och-policy (tooling)
  no dependencies         cargo_metadata + parsing support
        ^
        |
  future adapters (not created yet)
```

[`och-core`](../crates/och-core/) is deliberately empty of semantic APIs and
product dependencies. Its only executable is a baseline example used to verify
buildability and measure an upper bound on the foundation binary. It is not a
runtime or supported product command.

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

## Intentionally absent

There is currently no canonical observation identity or value model, time or
quality semantics, ordering/collection/gap contract, runtime, journal, segment,
store, persistence, query engine, network service, SQL layer, cloud/object
provider, embedded database, memory mapping, Studio/Engine link, or donor-code
compatibility layer.

Those omissions keep semantic authority available to M00-PR02 and prevent large
implementation dependencies from becoming architectural facts before their
contracts are reviewed. See [the continuation note](continuation-m00-pr02.md).
