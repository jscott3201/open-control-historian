#!/usr/bin/env bash
# Measure the deliberately tiny native example without claiming runtime behavior.
set -euo pipefail

readonly ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly TOOLCHAIN="1.98.0"
readonly OUTPUT_DIR="${ROOT}/target/baseline"
readonly OUTPUT_FILE="${OUTPUT_DIR}/baseline.txt"
readonly METADATA_FILE="${OUTPUT_DIR}/metadata.json"
readonly MAX_BINARY_BYTES=1048576

mkdir -p -- "${OUTPUT_DIR}"

cargo "+${TOOLCHAIN}" build \
    --manifest-path "${ROOT}/Cargo.toml" \
    --locked \
    --release \
    -p och-core \
    --example baseline

binary="${ROOT}/target/release/examples/baseline"
if [[ ! -x "${binary}" ]]; then
    echo "baseline executable is missing or not executable: ${binary}" >&2
    exit 1
fi

expected_output="OpenControl Historian native boundary baseline"
actual_output="$("${binary}")"
if [[ "${actual_output}" != "${expected_output}" ]]; then
    echo "baseline executable produced unexpected output: ${actual_output}" >&2
    exit 1
fi

binary_bytes="$(wc -c < "${binary}" | tr -d '[:space:]')"
if (( binary_bytes > MAX_BINARY_BYTES )); then
    echo "baseline executable is ${binary_bytes} bytes; limit is ${MAX_BINARY_BYTES}" >&2
    exit 1
fi

cargo "+${TOOLCHAIN}" metadata \
    --manifest-path "${ROOT}/Cargo.toml" \
    --format-version 1 \
    --all-features \
    --locked > "${METADATA_FILE}"

read -r native_roots core_closure_packages <<< "$(python3 - "${METADATA_FILE}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as metadata_file:
    metadata = json.load(metadata_file)

native_names = set(metadata["metadata"]["och-policy"]["native-packages"])
packages = {package["id"]: package for package in metadata["packages"]}
roots = [
    package_id
    for package_id in metadata["workspace_default_members"]
    if packages[package_id]["name"] == "och-core"
    and packages[package_id]["name"] in native_names
]
if len(roots) != 1:
    raise SystemExit("och-core must be one configured default native root")
nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
visited = set(roots)
pending = list(roots)
while pending:
    package_id = pending.pop()
    for dependency in nodes[package_id]["dependencies"]:
        if dependency not in visited:
            visited.add(dependency)
            pending.append(dependency)
print(len(native_names), len(visited))
PY
)"

if [[ "${native_roots}" != "3" ]]; then
    echo "workspace has ${native_roots} native roots; expected current boundary is 3" >&2
    exit 1
fi
if [[ "${core_closure_packages}" != "1" ]]; then
    echo "och-core closure contains ${core_closure_packages} packages; expected dependency-free baseline is 1" >&2
    exit 1
fi

{
    echo "OpenControl Historian native boundary baseline"
    echo "machine: $(uname -srm)"
    echo "rustc: $(rustc "+${TOOLCHAIN}" --version)"
    echo "profile: release (thin LTO, one codegen unit, panic=abort, symbols stripped)"
    echo "workspace native roots: ${native_roots}"
    echo "measured native root: och-core"
    echo "och-core closure packages: ${core_closure_packages}"
    echo "baseline executable bytes: ${binary_bytes}"
    echo "baseline executable limit bytes: ${MAX_BINARY_BYTES}"
    echo "idle RSS: N/A (the measurement example has no long-running process to measure)"
} | tee "${OUTPUT_FILE}"
