# Native dependency policy

The native closure law is executable through the private `och-policy` package:

```console
cargo +1.98.0 run --locked -p och-policy -- check --manifest-path Cargo.toml
```

## Sources of truth

The root `[workspace.metadata.och-policy]` table owns three non-overlapping
package lists: native, adapter, and tooling. Every workspace package independently
declares `package.metadata.och.role`. Product packages also declare
`unsafe-policy = "forbid"` and `missing-docs-policy = "deny"`; source lints remain
the compiler-enforced mechanism, while metadata makes absence fail closed during
the graph check.

The root `default-members` selection must consist only of native packages, and
every native package must be selected. This explicit comparison distinguishes
the default product build from all workspace tooling. A future adapter belongs
in workspace membership and adapter ownership but not in `default-members`.

## Traversal law

The checker asks Cargo for locked, all-present-feature metadata and starts only
from configured native roots. It walks resolved package IDs, using a visited set
so cycles and shared dependencies terminate deterministically. It reports the
dependency path for each violation.

Resolved package identity—not the dependency alias—is compared with forbidden
package names and prefixes. Therefore renaming a dependency cannot conceal it;
the alias is retained in diagnostics to make the declaring edge easy to find.
Native paths may reach another native package or an allowed external package,
but cannot reach a workspace adapter/tooling package or a configured forbidden
identity.

The forbidden set currently protects the foundation from async runtime, Arrow,
Parquet, DataFusion/Flight, gRPC/protobuf, SQL/PostgreSQL, object/cloud provider,
embedded database, and memory-mapping families named in root metadata. This
metadata check is the authoritative native-closure proof. `cargo-deny` provides
the complementary bans, licenses, sources, and release advisory policy for the
whole lockfile; it does not replace direction-aware traversal.

## Test obligations

The fixture suite proves positive traversal with shared/cyclic structure and
negative direct, transitive, renamed-identity, adapter-reversal, tooling,
implicit-default, unsafe-policy, missing-role, and forbidden-prefix cases. An
integration test loads the actual workspace with Cargo metadata and proves the
current native closure is exactly the `och-core` root.

When roles or dependencies change, update policy metadata and tests together.
Do not weaken the forbidden list or add broad exceptions merely to admit a new
dependency; justify the boundary change in architecture documentation first.
