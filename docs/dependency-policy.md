# Native dependency policy

The native closure law is executable through the private `och-policy` package:

```console
cargo +1.98.0 run --locked -p och-policy -- check --manifest-path Cargo.toml
```

## Sources of truth

Schema 2 of the root `[workspace.metadata.och-policy]` table owns three
non-overlapping package lists: native, adapter, and tooling. Every workspace
package independently declares `package.metadata.och.role`. Product packages
also declare `unsafe-policy = "forbid"` and `missing-docs-policy = "deny"`;
source lints remain the compiler-enforced mechanism, while metadata makes absence
fail closed during the graph check.

`dependency-free-native-packages = ["och-core"]` separately names the canonical
model root whose resolved dependency list must remain empty. This proves
`och-core`'s closure independently of the other native root.

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
identity except for the single structured exception below.

M01-PR02 adds the ordinary inward native edge `och-runtime -> och-core`. It needs
no exception: both package identities are declared native, and `och-core` remains
independently dependency-free. Because both were already traversal roots, this
edge does not enlarge the union native closure.

## Exact Tokio exception

Tokio remains in `forbidden-packages`. One structured exception admits resolved
package identity `tokio` only when it is the immediate dependency of the
`och-runtime` traversal root:

```toml
forbidden-dependency-exceptions = [
    { source = "och-runtime", target = "tokio", default-features = false, features = ["rt", "sync"] },
]
```

The graph checker resolves the edge to package identity before correlating it
with the workspace manifest declaration, so an alias cannot conceal or broaden
the target. A usable exception requires exactly one unconditional, normal,
non-optional direct declaration, `default-features = false`, and declared features
exactly `rt` plus `sync`. It also compares the resolved Tokio node's enabled
feature set with that exact set, catching feature unification from any other
edge. Cargo metadata 1 for the checked-in manifest reports declaration features
`["rt", "sync"]` and resolved Tokio node features `["rt", "sync"]`; no implicit
feature is omitted from the executable comparison.

The exception does not apply to `och-core -> tokio`, another native root, a
transitive `och-runtime -> helper -> tokio` path, or a path entering
`och-runtime` from a different root. Exceptions with missing, malformed, or extra
fields, empty/duplicate feature entries, duplicate source/target pairs,
non-native sources, non-forbidden targets, mismatched declaration settings,
broadened resolved features, or no matching direct traversal fail closed. An
exception is counted as used only after all identity, declaration, and resolved
feature checks pass. Removing the direct edge therefore requires removing the
exception in the same change.

The forbidden set currently protects the foundation from unreviewed async
runtime, Arrow, Parquet, DataFusion/Flight, gRPC/protobuf, SQL/PostgreSQL,
object/cloud provider, embedded database, and memory-mapping families named in
root metadata. This metadata check is the authoritative native-closure proof.
`cargo-deny` provides the complementary bans, licenses, sources, and release
advisory policy for the whole lockfile; it does not replace direction-aware
traversal.

## Test obligations

The fixture suite proves positive traversal with shared/cyclic structure and
negative direct, transitive, renamed-identity, adapter-reversal, tooling,
implicit-default, unsafe-policy, missing-role, and forbidden-prefix cases. It
also proves the exact direct Tokio exception, aliases, core/other-native and
transitive rejection, dependency-free roots, malformed/duplicate/unused
exceptions, non-native sources, non-forbidden targets, manifest default-feature,
kind, optionality, target, exact-feature failures, and resolved feature
broadening. An integration test loads the actual workspace with Cargo metadata
and proves both the feature contract and two native roots with a four-package
union closure: `och-core`, `och-runtime`, `tokio`, and `pin-project-lite`.

When roles or dependencies change, update policy metadata and tests together.
Do not weaken the forbidden list or add broad exceptions merely to admit a new
dependency; justify the boundary change in architecture documentation first.
