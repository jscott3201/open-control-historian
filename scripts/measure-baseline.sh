#!/usr/bin/env bash
# Measure the deliberately tiny native anchor without claiming runtime behavior.
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

closure_packages="$(python3 - "${METADATA_FILE}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as metadata_file:
    metadata = json.load(metadata_file)

native_names = set(metadata["metadata"]["och-policy"]["native-packages"])
packages = {package["id"]: package for package in metadata["packages"]}
roots = [
    package_id
    for package_id in metadata["workspace_default_members"]
    if packages[package_id]["name"] in native_names
]
nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
visited = set(roots)
pending = list(roots)
while pending:
    package_id = pending.pop()
    for dependency in nodes[package_id]["dependencies"]:
        if dependency not in visited:
            visited.add(dependency)
            pending.append(dependency)
print(len(visited))
PY
)"

if [[ "${closure_packages}" != "1" ]]; then
    echo "native closure contains ${closure_packages} packages; expected foundation baseline is 1" >&2
    exit 1
fi

{
    echo "OpenControl Historian native boundary baseline"
    echo "machine: $(uname -srm)"
    echo "rustc: $(rustc "+${TOOLCHAIN}" --version)"
    echo "profile: release (thin LTO, one codegen unit, panic=abort, symbols stripped)"
    echo "native roots: 1"
    echo "native closure packages: ${closure_packages}"
    echo "baseline executable bytes: ${binary_bytes}"
    echo "baseline executable limit bytes: ${MAX_BINARY_BYTES}"
    echo "idle RSS: N/A (the anchor has no long-running process to measure)"
} | tee "${OUTPUT_FILE}"
