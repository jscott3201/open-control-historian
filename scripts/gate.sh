#!/usr/bin/env bash
# Canonical local/CI gate. PR mode stays lean; release mode owns fresh and clean evidence.
set -euo pipefail

readonly ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly TOOLCHAIN="1.98.0"
readonly NEXTEST_VERSION="0.9.143"
readonly DENY_VERSION="0.20.2"
readonly MODE="${1:-pr}"

if [[ "${MODE}" != "pr" && "${MODE}" != "release" ]]; then
    echo "usage: $0 [pr|release]" >&2
    exit 2
fi

cd -- "${ROOT}"

for command in cargo rustc python3 git; do
    if ! command -v "${command}" >/dev/null 2>&1; then
        echo "required command is unavailable: ${command}" >&2
        exit 1
    fi
done

rustc_version="$(rustc "+${TOOLCHAIN}" --version)"
if [[ "${rustc_version}" != rustc\ 1.98.0* ]]; then
    echo "Rust 1.98.0 is required; found: ${rustc_version}" >&2
    exit 1
fi
nextest_version="$(cargo "+${TOOLCHAIN}" nextest --version)"
if [[ "${nextest_version}" != cargo-nextest\ ${NEXTEST_VERSION}* ]]; then
    echo "cargo-nextest ${NEXTEST_VERSION} is required; found: ${nextest_version}" >&2
    exit 1
fi
deny_version="$(cargo "+${TOOLCHAIN}" deny --version)"
if [[ "${deny_version}" != cargo-deny\ ${DENY_VERSION}* ]]; then
    echo "cargo-deny ${DENY_VERSION} is required; found: ${deny_version}" >&2
    exit 1
fi

run() {
    local label="$1"
    shift
    echo
    echo "==> ${label}"
    "$@"
}

run "rustfmt" cargo "+${TOOLCHAIN}" fmt --all -- --check
run "locked default-member build" cargo "+${TOOLCHAIN}" build --locked
run "locked default-member check" cargo "+${TOOLCHAIN}" check --locked
run "strict workspace clippy" cargo "+${TOOLCHAIN}" clippy --workspace --all-targets --all-features --locked -- -D warnings
run "workspace tests through nextest" cargo "+${TOOLCHAIN}" nextest run --workspace --locked --profile ci --no-tests=fail
# Nextest intentionally does not run doctests.
run "workspace doctests" cargo "+${TOOLCHAIN}" test --workspace --doc --locked
run "native metadata dependency policy" cargo "+${TOOLCHAIN}" run --locked -p och-policy -- check --manifest-path "${ROOT}/Cargo.toml"
run "rustdoc and intra-doc links" env RUSTDOCFLAGS="-D warnings" cargo "+${TOOLCHAIN}" doc --workspace --no-deps --locked
run "repository docs, local links, file size, and no-secret checks" python3 "${ROOT}/scripts/check_repository.py"
run "license, source, and ban policy" cargo "+${TOOLCHAIN}" deny check bans licenses sources
run "tracked diff whitespace" git diff --check

if [[ "${MODE}" == "release" ]]; then
    # Advisory checking is intentionally network-capable and belongs to the heavy gate.
    run "fresh release advisory policy" cargo "+${TOOLCHAIN}" deny check advisories
    run "clean build state" cargo "+${TOOLCHAIN}" clean
    run "clean locked default-member build" cargo "+${TOOLCHAIN}" build --locked
    run "workspace default-feature check" cargo "+${TOOLCHAIN}" check --workspace --locked
    run "workspace no-default-feature check" cargo "+${TOOLCHAIN}" check --workspace --no-default-features --locked
    run "workspace all-present-feature check" cargo "+${TOOLCHAIN}" check --workspace --all-features --locked
    run "native baseline measurement and bounds" "${ROOT}/scripts/measure-baseline.sh"
fi

echo
echo "${MODE} gate passed"
