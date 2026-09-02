# M03-PR03g2 complete private structural harness infrastructure

## Objective and authority

M03-PR03g2 extends only the existing private `och-v2-evidence` tooling package.
It closes the structural matrix, event/result/report validators, bounded report
transaction, and parent-owned crash campaign required by the accepted PR03e plan.
It changes no `crates/` source, dependency, default member, product API, Store
Format V1 byte, or accepted Store Format V2 byte.

The checked structural result is `STRUCTURAL_SYNTHETIC`. It is not Linux native
measurement or acceptance evidence. `COLLECTION_AUTHORIZED=false`,
`MEASURED_NATIVE_EVIDENCE=false`, every `PR03E-M01..M11` row remains
`UNSATISFIED`, `V2_PRODUCT_AUTHORITY=false`, and writer-delay, eager-open, RSS,
total-runtime, external-workspace, threshold, budget, and SLO values remain
`UNKNOWN`.

The PR03e plan-acceptance anchor is the actual full merge SHA
`af67792cbd28eb74cead673a0044c5f54d27ee6c`. The dispatch shorthand
`af677928` does not resolve at this repository revision; the merge subject and
history resolve the accepted plan to the full SHA above.

## Closed structural inventory

The existing 173-row `FaultId::ALL` registry remains the sole descriptor and
source-site authority. The harness derives, rather than duplicates, all 487
fault applicability/result rows: 173 pre-operation errors, 173 child-crash
targets, seven non-pressure short writes, 120 typed pressure pre-operation
overlays, and 14 typed pressure short-write overlays.

The exact 639-row matrix is precomputed before store-child mutation:

| Category | Rows |
| --- | ---: |
| PR03e crosswalk | 11 |
| writer demand/trigger cases | 5 |
| literal TRACE/ELIGIBILITY/TREE fixtures | 28 |
| registry-derived fault applicability | 487 |
| literal timing events | 29 |
| bounds | 13 |
| hostile inventories/relations | 18 |
| resource obligations | 21 |
| report obligations | 18 |
| platform/exclusion obligations | 9 |
| **Total** | **639** |

The nonempty structural timing forest contains exactly 173 event rows, six
structural summaries, and six complete resource-ledger rows. It includes all five
writer demand/path cases, pending-empty pre-append barriers, post-publication
batch joins, the exact ordinary and P0-P7/P6-child order, and all 64 eager-open
pair ordinals. Pure validators enforce the direct-parent table, root sentinel,
one parent, acyclicity, containment, sibling order/touch, post-rename committed
classification, classification precedence, and exact eligibility equivalence.
Every named accepted/rejected fixture in the plan is executed.

## Parent-owned crash lifecycle

For each of the 173 literal targets, the parent exclusively creates and retains
the cleanup-owning `V2StoreChild`. The spawned process receives only a private,
non-cleaning child-worker view. Request and ready records remain under the
out-of-band evidence `control` directory. The child publishes exact readiness
only after the selected operation succeeds, then blocks inside boundary finish
before return. The parent validates target/token/PIDs, abruptly kills, waits and
reaps, fingerprints immediately, performs descriptor-selected reopen/convergence,
validates the path-free witness, and makes exactly one explicit child cleanup
attempt. A child never owns or performs store cleanup and cannot flush a report.

Each case first executes the applicable synthetic transaction, rollback, or
eager-open prefix through the predecessor of the selected descriptor. Reopen acts
on that exact post-kill inventory; it never clears or reconstructs the child.
Preflight cases prove unchanged authority, precommit mutations remove only exact
uncommitted candidates with intent last and return to the exact prior fingerprint,
and successful Manifest rename/postcommit cases execute the remaining adoption and
cleanup successors. Committed raw/segment/catalog/manifest finals are hash-compared
across convergence, and a committed case may not return to the prior fingerprint.
Occurrence counters resume from the executed prefix for repeated rollback/open
boundaries.

The parent and child never combine monotonic clocks and no killed-child stop event
is fabricated. Operation errors retain precedence over cleanup errors; successful
operation plus cleanup failure returns the cleanup failure. Once spawned, a child
failure still enters bounded termination and reap before the parent's one cleanup
attempt. After proven reap, any termination error invalidates structural evidence
before immediate fingerprinting, convergence, witness construction, or report
publication; only a successful observation with zero termination errors may
continue. Kill/observe errors do not bypass bounded wait/reap; a child whose reap
cannot be proved returns `REPLAN` while retaining its private subtree rather than
cleaning beneath a possibly live process. The hidden child independently checks
that `cases`, `control`, its selected store child, and request/ready entries are
direct non-symlink objects beneath the canonical evidence root, with another
check immediately before worker mutation. These checks are finite std-only
containment checks, not a universal filesystem race guarantee. Each completed
command leaves `cases` and `control` empty.

## Bounded report transaction

The exact report bundle has seven UTF-8 data files—`run.kv`,
`timing-samples.tsv`, `timing-summary.tsv`, `resource-ledger.tsv`,
`fault-registry.tsv`, `fault-results.tsv`, and `matrix.tsv`—plus `SHA256SUMS`
covering exactly those seven in sorted relative-name order. The writer constructs
and validates the complete bundle before filesystem mutation, writes only to an
out-of-band staging directory, synchronizes and bounded-rereads every file,
validates the staging bundle, renames it, synchronizes the report parent, and
validates the final bundle. A repeated structural run preserves the validated
final while staging, renames it to one exact private prior directory before final
selection, and synchronizes the parent after every authority-changing rename or
removal. Handled failure rolls back to the prior bundle and removes transaction
artifacts; the next open reconciles only state-machine-reachable staging/prior
states and rejects invalid or ambiguous combinations.

The validator enforces 64 MiB/bundle, 16 MiB/data file, 4,096-byte physical line,
1,024-byte scalar, exact files, exact keys/columns/enums/counts, safe relative
identities, source hashes, and safe-Rust SHA-256 checksums. It rejects unlisted or
non-file entries, absolute or parent paths, source/checksum mismatch, unsafe text,
identity/environment dumps, credentials, canonical payload bytes, raw/segment
bytes, and core dumps. Reports retain only closed values, sanitized identities,
and hashes.

## Private commands

The g1 foundation command remains non-authorizing and report-free. Its summary
now truthfully calls the 173 crash sites the separate g2 target set rather than
future deferred work.

```console
cargo +1.98.0 run --locked -p och-v2-evidence -- \
  native-harness-check --root target/private-v2-harness
```

That command executes the complete structural/fault/crash proof and writes the
validated `STRUCTURAL_SYNTHETIC` bundle. The hidden child command rejects direct,
missing, duplicate, malformed, or descriptor-mismatched invocation.

The private collection command name and exact parser remain reserved, but this PR
contains no measured collector:

```console
cargo +1.98.0 run --locked -p och-v2-evidence -- \
  native-collect --root <dedicated-evidence-root> \
  --harness-sha <accepted-clean-40-hex-sha> \
  --measured-source-sha <clean-40-hex-sha> \
  --tree-status CLEAN --authorization POST_ACCEPTANCE_G2
```

Every syntactically complete invocation returns `REPLAN` before evidence-root or
bundle creation. Caller-provided SHA, tree-status, or authorization strings are
never converted into `ACCEPTANCE_CANDIDATE` or
`MEASURED_NATIVE_EVIDENCE=true`; the report writer accepts only its internal
`STRUCTURAL_SYNTHETIC` context. A later separately accepted collector must
independently establish exact clean tracked/untracked state and source SHAs,
collect all mandatory cold/warm/witness tiers and three deterministic repetitions
for every fault/mode/pressure row, and feed those actual observations through the
same validators. Until then no collection is implemented or authorized.

## Stop conditions and successor

The harness returns `REPLAN` rather than truncating when the complete bundle
cannot fit its fixed caps. Any capability escape, child cleanup ownership,
unreaped spawned child, incomplete registry/matrix, unclosed report text, product
or format dependency, measured/structural ambiguity, or failure of the exact
campaign remains a hard stop.

Acceptance of this implementation does not make the fail-closed command a
collector. A later dedicated Linux x86_64 collector implementation and invocation,
its complete acceptance-candidate bundle, owner review, and a fresh owner
checkpoint still precede any V2 product plan.
