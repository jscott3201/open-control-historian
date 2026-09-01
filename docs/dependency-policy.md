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
`och-core`'s closure independently of the other native roots.

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

M02-PR01b0 adds the ordinary inward native edge `och-store -> och-core`. It
needs no exception and adds no third-party package. `och-store` is a third
default native root; sharing `och-core` grows the union closure by only
`och-store` itself.

M02-PR01b1 adds the ordinary inward native edge `och-runtime -> och-store` so the
only public runtime writer uses the reviewed Journal V1 and active-journal owner.
Both are native default roots, so the edge needs no exception and adds no package
to the three-root, five-package union closure. `och-runtime` also retains its
direct `och-core` edge for canonical types; neither edge changes `och-core`'s
separately enforced dependency-free status.

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

The exception does not apply to `och-core -> tokio`, `och-store -> tokio`, another native root, a
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

## Non-default native evidence feature and private tooling consumer

M03-PR03f adds the exact `m03-pr03e-native-harness` feature to `och-store` and
`och-runtime`, with explicit `default = []` in both manifests. Runtime forwards
only `och-store/m03-pr03e-native-harness`. This is an owner-reviewed,
rustdoc-hidden, unsupported current-V1 instrumentation prerequisite for a later
private tooling package; it is not a product capability or a general extension
API.

M03-PR03g1 extends the existing `och-v2-evidence` tooling package rather than
adding a package. It adds one normal direct dependency on `och-runtime` with
`default-features = false` and only `m03-pr03e-native-harness`, plus one normal
direct Tokio declaration at the exact existing `1.53.1` pin with default features
disabled and only `rt` and `sync`. This tooling edge drives a current-thread
executor around the hidden facade for narrow success/pressure smoke and is not a
native-product exception. It supplies no collector or report machinery.
The private V2 executor's opaque child capability is an internal Rust-privacy
boundary within this tooling package; it adds no package edge, supported API, or
claim about arbitrary present or future filesystem-I/O source.

Standard Cargo feature unification during explicit workspace validation remains
exactly `rt` plus `sync`. The existing native `och-runtime -> tokio` structured
exception therefore also fails if the tooling edge broadens Tokio. Root defaults
remain exactly the three native crates, the native closure remains five resolved
packages, native code retains no edge to tooling, and no `sha2` package is added.

## Test obligations

The fixture suite proves positive traversal with shared/cyclic structure and
negative direct, transitive, renamed-identity, adapter-reversal, tooling,
implicit-default, unsafe-policy, missing-role, and forbidden-prefix cases. It
also proves the exact direct Tokio exception, aliases, core/other-native and
transitive rejection, dependency-free roots, malformed/duplicate/unused
exceptions, non-native sources, non-forbidden targets, manifest default-feature,
kind, optionality, target, exact-feature failures, and resolved feature
broadening. An integration test loads the actual workspace with Cargo metadata,
exact-compares both tooling declarations and the lockfile edge, rejects `sha2`,
and proves both the feature contract and three native roots with a five-package
union closure: `och-core`, `och-runtime`, `och-store`, `tokio`, and
`pin-project-lite`.

When roles or dependencies change, update policy metadata and tests together.
Do not weaken the forbidden list or add broad exceptions merely to admit a new
dependency; justify the boundary change in architecture documentation first.
The actual-workspace fixture also checks the evidence feature's exact empty
defaults, runtime-to-store forwarding, unchanged native dependency declarations,
default-member containment, unchanged package membership, and absence of a
`sha2` lockfile addition.
