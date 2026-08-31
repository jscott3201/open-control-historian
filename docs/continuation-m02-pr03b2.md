# M02-PR03b2 runtime pressure lifecycle continuation

## Delivered boundary

`och-runtime` now exports compact copyable source, operation, and evidence types.
Runtime inspection reports the composed store write state and first retained
pressure evidence. The blocking store worker centralizes live store-terminal
handling so inspection and sticky health are established before fail-stop wakes
receipts or sends responses. Later coordinator failure and repeated stop cannot
replace first evidence or `StoragePressure` health.

Existing admission truth is unchanged. Pressure before handled stops both stages;
pressure during flush retains the exact handled append while stopping only the
durable stage. Already durable outcomes remain exact, all reservations release,
new ingress/control calls return their existing closed variants, future latest
capture is unavailable, and caller-held snapshots remain immutable and usable.

## Shutdown and evidence

Consuming pressure shutdown waits for the existing reaper before returning exact
`ShutdownError::StoragePressure` evidence, so successful return of that error also
proves the blocking worker was joined and its store lock released. Tokio task
panic/cancellation and unavailable reaper signaling retain their existing generic
truth and are not masked by stale pressure.

Focused deterministic tests cover active/manifest evidence parts, append and
flush stage bounds, first-wins repetition, closed control, sanitized output,
reaper hold/release, same-lock reopen, generic-fault separation, and unchanged
latest/receipt/reservation facts. The private hook is entirely `cfg(test)` and no
`och-store`, ingress, latest, dependency, or durable-format production file changed.

## Remaining boundary

Pressure retry/clear, continued degraded ingress, durable pressure state,
stale-restore custody, broad repair, retention/reclamation, final segments, query,
providers, adapters, and platform-wide physical capacity guarantees remain absent.
