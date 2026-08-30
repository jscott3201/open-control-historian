# M02 current-only V1 durable-format reset continuation

## Delivered boundary

The store now publishes an exact 32-byte Store Format V1 root marker and recognizes
one current layout for each artifact family. Manifest V1 is 160 bytes with a
mandatory Retry State V1 reference. Retry State V1 always stores the 48-byte
generation/floor/catalog extension on replay outcomes. Journal Header V1 is the
sole 28-byte header, and Journal V1 admission frame bytes are unchanged.

Production header-upgrade, historical manifest/retry decode, premanifest registry
snapshot bootstrap, and compatibility-only public accessors were removed.
`ManifestCommit::retry_state` is mandatory. Catalog optionality remains because it
represents the current unrotated-versus-rotated state, not compatibility.

## Refusal and convergence

A bounded read-only preflight precedes stable-lock creation or acquisition.
Markerless nonempty directories, malformed or unsupported markers, historical
manifest/header/retry layouts, and old/current mixtures return path-free
`UnsupportedStoreFormat` without changing names or bytes. A valid marker is
necessary but does not authorize mismatched artifacts. `CreateNew` also refuses
unsupported nonempty stores unchanged.

An empty create synchronizes the stable lock, publishes and verifies the marker,
then publishes current Journal Header V1, checkpoint, registry, mandatory empty
retry state, and Manifest V1. An exact complete marker staging file can finish its
rename; incomplete marker staging refuses unchanged. Later exact renamed genesis
finals can be validated and completed, while unpublished staging refuses rather
than being guessed. Normal current cleanup and narrow rotation convergence remain
after under-lock repeated validation.

## Preserved behavior

The sole writer, global append sequence, generation-local offset/checkpoint
generations, declaration authority, exact byte reservation, bounded FIFO durable
retry replay and guard, raw-Journal sealing, catalog capacity, group barriers,
handled/durable receipt split, no-false-durable-success failure law, volatile
latest publication, empty latest restart, graceful shutdown, and fail-stop Drop
remain unchanged.

## Deferred successors

Recovery from the superseded recovery proposal will be redesigned against this
current-only epoch. Migration, destructive repair, broad recovery, disk pressure,
read-only degradation, final native segments, query, latest reconstruction,
retention/reclamation, adapters/providers, and broader platform guarantees remain
absent.

Historical M02-PR02a/b/c implementation and continuation documents remain records
of their delivered revisions only. Their compatibility statements are superseded
and are not current opening authority.
