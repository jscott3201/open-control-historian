#!/usr/bin/env bash
# Reproducible local/manual resource evidence; ordinary PR CI is not acceptance.
set -euo pipefail

readonly ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly TOOLCHAIN="1.98.0"
readonly RSS_TARGET_BYTES="167772160"
cases="min,representative"
repetitions="3"

while (($# > 0)); do
    case "$1" in
        --cases)
            cases="${2:?--cases requires a comma-separated value}"
            shift 2
            ;;
        --repetitions)
            repetitions="${2:?--repetitions requires a value}"
            shift 2
            ;;
        *)
            echo "usage: $0 [--cases min,representative,...] [--repetitions N]" >&2
            exit 2
            ;;
    esac
done

if [[ ! "${repetitions}" =~ ^[1-9][0-9]*$ ]] || ((repetitions > 20)); then
    echo "repetitions must be in 1..=20" >&2
    exit 2
fi

IFS=',' read -r -a case_list <<<"${cases}"
for fixture_case in "${case_list[@]}"; do
    case "${fixture_case}" in
        min|min-observed|representative|max-records|max-series|max-observations|max-bytes) ;;
        *)
            echo "unsupported evidence case: ${fixture_case}" >&2
            exit 2
            ;;
    esac
done

readonly EVIDENCE_ROOT="${OCH_V2_EVIDENCE_ROOT:-${ROOT}/target/v2-evidence}"
readonly REPORT_ROOT="${EVIDENCE_ROOT}/reports"
readonly SAMPLES="${REPORT_ROOT}/samples.tsv"

cd -- "${ROOT}"
cargo "+${TOOLCHAIN}" build --release --locked -p och-v2-evidence
readonly TOOL="${ROOT}/target/release/och-v2-evidence"
"${TOOL}" prepare-root --root "${EVIDENCE_ROOT}" >/dev/null
mkdir -p -- "${REPORT_ROOT}"

os_name="$(uname -s)"
arch_name="$(uname -m)"
case "${os_name}" in
    Darwin)
        readonly TIME_MODE="darwin-time-l"
        readonly RSS_SOURCE="/usr/bin/time -l maximum resident set size"
        readonly RSS_NATIVE_UNITS="bytes"
        physical_memory_bytes="$(sysctl -n hw.memsize)"
        cpu_description="$(sysctl -n machdep.cpu.brand_string | tr '\t\n' '  ')"
        filesystem_type="$(python3 - "${EVIDENCE_ROOT}" <<'PY'
import os
import re
import subprocess
import sys
path = os.path.realpath(sys.argv[1])
selected = ('', 'UNKNOWN')
for line in subprocess.check_output(['mount'], text=True).splitlines():
    match = re.match(r'.+ on (.+) \(([^, )]+)', line)
    if match:
        mount_point, kind = match.groups()
        if (path == mount_point or path.startswith(mount_point.rstrip('/') + '/')) and len(mount_point) >= len(selected[0]):
            selected = (mount_point, kind)
print(selected[1])
PY
)"
        ;;
    Linux)
        readonly TIME_MODE="linux-time-v"
        readonly RSS_SOURCE="/usr/bin/time -v Maximum resident set size"
        readonly RSS_NATIVE_UNITS="KiB"
        physical_memory_bytes="$(python3 - <<'PY'
with open('/proc/meminfo', encoding='ascii') as source:
    fields = dict(line.split(':', 1) for line in source if ':' in line)
print(int(fields['MemTotal'].split()[0]) * 1024)
PY
)"
        cpu_description="$(python3 - <<'PY'
with open('/proc/cpuinfo', encoding='ascii', errors='replace') as source:
    for line in source:
        if line.lower().startswith('model name'):
            print(line.split(':', 1)[1].strip())
            break
    else:
        print('UNKNOWN')
PY
)"
        filesystem_type="$(stat -f -c '%T' "${EVIDENCE_ROOT}")"
        ;;
    *)
        echo "unsupported measurement OS: ${os_name}" >&2
        exit 1
        ;;
esac

if [[ ! -x /usr/bin/time ]]; then
    echo "/usr/bin/time is required" >&2
    exit 1
fi

revision="$(git rev-parse HEAD)"
if [[ -z "$(git status --porcelain --untracked-files=no)" ]]; then
    revision_status="clean-tracked"
else
    revision_status="dirty-tracked"
fi
read -r filesystem_total_bytes filesystem_free_bytes < <(python3 - "${EVIDENCE_ROOT}" <<'PY'
import shutil
import sys
usage = shutil.disk_usage(sys.argv[1])
print(usage.total, usage.free)
PY
)

cat >"${REPORT_ROOT}/machine.kv" <<EOF
schema=och-v2-evidence-machine-v1
revision=${revision}
revision_status=${revision_status}
os=${os_name}
kernel=$(uname -r)
arch=${arch_name}
cpu_count=$(getconf _NPROCESSORS_ONLN)
cpu_description=${cpu_description}
physical_memory_bytes=${physical_memory_bytes}
filesystem_type=${filesystem_type}
filesystem_total_bytes=${filesystem_total_bytes}
filesystem_free_bytes=${filesystem_free_bytes}
page_size_bytes=$(getconf PAGESIZE)
rustc=$(rustc "+${TOOLCHAIN}" --version)
cargo=$(cargo "+${TOOLCHAIN}" --version)
profile=release
rss_target_bytes=${RSS_TARGET_BYTES}
rss_source=${RSS_SOURCE}
rss_native_units=${RSS_NATIVE_UNITS}
rss_report_units=bytes
rss_unit_verification=platform-specific /usr/bin/time label and documented native unit
process_cache_mode=cold-process-per-sample
warm_process_mode=not-measured
filesystem_cold_mode=UNKNOWN-uncontrolled
repetitions=${repetitions}
cases=${cases}
EOF

printf 'case\toperation\trepetition\telapsed_seconds\tpeak_rss_bytes\tfixture_logical_high_water_bytes\tfixture_allocated_high_water_bytes\tfinal_logical_high_water_bytes\tfinal_allocated_high_water_bytes\texternal_workspace_logical_high_water_bytes\texternal_workspace_allocated_high_water_bytes\n' >"${SAMPLES}"

measure_one() {
    local fixture_case="$1"
    local operation="$2"
    local repetition="$3"
    local time_report="${REPORT_ROOT}/${fixture_case}-${operation}-${repetition}.time.txt"
    local tool_report="${REPORT_ROOT}/${fixture_case}-${operation}-${repetition}.tool.kv"
    local -a command=("${TOOL}" "${operation}" --root "${EVIDENCE_ROOT}" --case "${fixture_case}")
    if [[ "${TIME_MODE}" == "darwin-time-l" ]]; then
        /usr/bin/time -l -o "${time_report}" "${command[@]}" >"${tool_report}"
    else
        /usr/bin/time -v -o "${time_report}" "${command[@]}" >"${tool_report}"
    fi
    python3 - "${TIME_MODE}" "${time_report}" "${tool_report}" "${SAMPLES}" \
        "${fixture_case}" "${operation}" "${repetition}" "${EVIDENCE_ROOT}" <<'PY'
import os
import re
import sys

mode, time_path, tool_path, samples_path, case, operation, repetition, root = sys.argv[1:]
time_text = open(time_path, encoding='utf-8', errors='replace').read()
tool = {}
with open(tool_path, encoding='utf-8', errors='strict') as source:
    for line in source:
        if '=' in line:
            key, value = line.rstrip('\n').split('=', 1)
            tool[key] = value

if mode == 'darwin-time-l':
    elapsed_match = re.search(r'^\s*([0-9.]+)\s+real\b', time_text, re.MULTILINE)
    rss_match = re.search(r'^\s*([0-9]+)\s+maximum resident set size\b', time_text, re.MULTILINE)
    multiplier = 1
else:
    elapsed_match = re.search(r'^\s*Elapsed \(wall clock\) time.*:\s*([0-9:.]+)\s*$', time_text, re.MULTILINE)
    rss_match = re.search(r'^\s*Maximum resident set size \(kbytes\):\s*([0-9]+)\s*$', time_text, re.MULTILINE)
    multiplier = 1024
if not elapsed_match or not rss_match:
    raise SystemExit('unrecognized /usr/bin/time output')

elapsed_text = elapsed_match.group(1)
parts = [float(part) for part in elapsed_text.split(':')]
elapsed = 0.0
for part in parts:
    elapsed = elapsed * 60.0 + part
rss = int(rss_match.group(1)) * multiplier

raw_path = os.path.join(root, 'fixtures', f'{case}.raw-journal-v1-evidence')
segment_path = os.path.join(root, 'artifacts', f'{case}.ochseg01-evidence')
def sizes(path):
    stat = os.stat(path)
    return stat.st_size, getattr(stat, 'st_blocks', 0) * 512
fixture_logical, fixture_allocated = sizes(raw_path)
final_logical, final_allocated = sizes(segment_path)
with open(samples_path, 'a', encoding='utf-8') as output:
    output.write(
        f'{case}\t{operation}\t{repetition}\t{elapsed:.9f}\t{rss}\t'
        f'{fixture_logical}\t{fixture_allocated}\t{final_logical}\t{final_allocated}\t'
        f'{tool.get("external_sort_workspace_bytes", "UNKNOWN")}\t0\n'
    )
PY
}

for fixture_case in "${case_list[@]}"; do
    "${TOOL}" generate --root "${EVIDENCE_ROOT}" --case "${fixture_case}" --seed 1 \
        >"${REPORT_ROOT}/${fixture_case}-generation.kv"
    for ((sample = 1; sample <= repetitions; sample++)); do
        measure_one "${fixture_case}" stream-build "${sample}"
    done
    for ((sample = 1; sample <= repetitions; sample++)); do
        measure_one "${fixture_case}" stream-validate "${sample}"
    done
done

python3 - "${SAMPLES}" "${REPORT_ROOT}/summary.kv" "${RSS_TARGET_BYTES}" "${os_name}" "${arch_name}" <<'PY'
import csv
import math
import statistics
import sys
from collections import defaultdict

samples_path, output_path, target_text, os_name, arch_name = sys.argv[1:]
target = int(target_text)
groups = defaultdict(list)
with open(samples_path, encoding='utf-8', newline='') as source:
    for row in csv.DictReader(source, delimiter='\t'):
        groups[(row['case'], row['operation'])].append(row)

with open(output_path, 'w', encoding='utf-8') as output:
    output.write('schema=och-v2-evidence-summary-v1\n')
    output.write(f'platform={os_name}\n')
    output.write(f'arch={arch_name}\n')
    acceptance = 'LINUX_X86_64_CANDIDATE_ONLY' if os_name == 'Linux' and arch_name == 'x86_64' else 'EXPLORATORY_ONLY'
    output.write(f'acceptance={acceptance}\n')
    output.write(f'rss_target_bytes={target}\n')
    for (case, operation), rows in sorted(groups.items()):
        elapsed = sorted(float(row['elapsed_seconds']) for row in rows)
        rss = sorted(int(row['peak_rss_bytes']) for row in rows)
        p95_index = max(0, math.ceil(len(rows) * 0.95) - 1)
        prefix = f'{case}.{operation}'
        output.write(f'{prefix}.samples={len(rows)}\n')
        output.write(f'{prefix}.elapsed_min_seconds={elapsed[0]:.9f}\n')
        output.write(f'{prefix}.elapsed_median_seconds={statistics.median(elapsed):.9f}\n')
        output.write(f'{prefix}.elapsed_p95_seconds={elapsed[p95_index]:.9f}\n')
        output.write(f'{prefix}.elapsed_max_seconds={elapsed[-1]:.9f}\n')
        output.write(f'{prefix}.rss_min_bytes={rss[0]}\n')
        output.write(f'{prefix}.rss_median_bytes={int(statistics.median(rss))}\n')
        output.write(f'{prefix}.rss_p95_bytes={rss[p95_index]}\n')
        output.write(f'{prefix}.rss_max_bytes={rss[-1]}\n')
        output.write(f'{prefix}.rss_below_target={str(rss[-1] < target).lower()}\n')
PY

echo "wrote sanitized evidence reports under target/v2-evidence/reports"
