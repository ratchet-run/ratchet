#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "$#" -ne 2 ]]; then
  echo "Usage: $0 <runtime-artifact-root> <conformance-docs-root>" >&2
  exit 2
fi

artifact_root="$1"
docs_root="$2"

if [[ ! -d "${artifact_root}" ]]; then
  echo "Runtime artifact root does not exist: ${artifact_root}" >&2
  exit 1
fi

if [[ "${RUNTIME_MATRIX_JSON+x}" == 'x' ]]; then
  matrix_json="${RUNTIME_MATRIX_JSON}"
else
  if ! lane_output="$(GITHUB_OUTPUT='' GITHUB_STEP_SUMMARY='' python3 "${SCRIPT_DIR}/../infra/ci/affected_lanes.py" --full)"; then
    echo 'Failed to read runtime matrix from affected_lanes.py --full' >&2
    exit 1
  fi
  matrix_json=''
  integration_found=0
  while IFS= read -r line; do
    if [[ "${line}" == integration=* ]]; then
      matrix_json="${line#integration=}"
      integration_found=1
      break
    fi
  done <<< "${lane_output}"
  if [[ "${integration_found}" -eq 0 ]]; then
    echo 'Missing integration= output from affected_lanes.py --full' >&2
    exit 1
  fi
fi

matrix_output=''
if ! matrix_output="$(python3 -c '
import json
import sys

try:
    rows = json.load(sys.stdin)
except ValueError as error:
    sys.exit(f"Invalid runtime matrix JSON: {error}")
if not isinstance(rows, list):
    sys.exit("Runtime matrix JSON must be an array")
if not rows:
    sys.exit("Runtime matrix row list is empty")
combinations = set()
for index, row in enumerate(rows, 1):
    for field in ("server", "database"):
        if not isinstance(row, dict) or field not in row:
            sys.exit(f"Runtime matrix row {index} is missing {field}")
        value = row[field]
        if not isinstance(value, str) or not value or any(c in value for c in "\t\r\n:"):
            sys.exit(f"Runtime matrix row {index} has invalid {field}")
    combinations.add((row["server"], row["database"]))
for server, database in sorted(combinations):
    print(f"{server}\t{database}")
' <<< "${matrix_json}")"; then
  exit 1
fi

echo "Read $(printf '%s\n' "${matrix_output}" | wc -l | tr -d ' ') runtime matrix cells"

combinations=()
while IFS=$'\t' read -r server database; do
  if [[ -n "${server}" && -n "${database}" ]]; then
    combinations+=("${server}:${database}")
  fi
done <<< "${matrix_output}"

server_label_for() {
  case "$1" in
    wildfly-managed) echo 'WildFly EE 10' ;;
    wildfly-ee11-managed) echo 'WildFly EE 11' ;;
    payara-managed) echo 'Payara EE 10' ;;
    payara-ee11-managed) echo 'Payara EE 11' ;;
    openliberty-managed) echo 'Open Liberty EE 10' ;;
    openliberty-ee11-managed) echo 'Open Liberty EE 11' ;;
    glassfish-managed) echo 'GlassFish EE 11' ;;
    *)
      echo "No display label for server id '$1'. Add a label for it in scripts/materialize-runtime-conformance-reports.sh." >&2
      return 1
      ;;
  esac
}

database_label_for() {
  case "$1" in
    mysql) echo 'MySQL' ;;
    postgresql) echo 'PostgreSQL' ;;
    oracle) echo 'Oracle' ;;
    sqlserver) echo 'SQL Server' ;;
    mongodb) echo 'MongoDB' ;;
    *)
      echo "No display label for database id '$1'. Add a label for it in scripts/materialize-runtime-conformance-reports.sh." >&2
      return 1
      ;;
  esac
}

TIER_ENTRIES=(
  'api:API'
  'jakarta:Jakarta'
)
PLACEHOLDER_TEXT='This page is generated automatically after each successful CI run'

sources=()
destinations=()
titles=()

for combination in "${combinations[@]}"; do
  server="${combination%%:*}"
  database="${combination#*:}"
  if ! server_label="$(server_label_for "${server}")"; then
    exit 1
  fi
  if ! database_label="$(database_label_for "${database}")"; then
    exit 1
  fi
  artifact_dir="${artifact_root}/tck-conformance-reports-${server}-${database}"

  if [[ ! -d "${artifact_dir}" ]]; then
    echo "Missing runtime artifact directory for ${server} / ${database}: ${artifact_dir}" >&2
    exit 1
  fi

  for tier_entry in "${TIER_ENTRIES[@]}"; do
    tier="${tier_entry%%:*}"
    tier_label="${tier_entry#*:}"
    report_name="tck-${tier}-conformance-report.md"
    matches=()

    while IFS= read -r -d '' match; do
      matches+=("${match}")
    done < <(find "${artifact_dir}" -type f -name "${report_name}" -print0)

    if [[ "${#matches[@]}" -eq 0 ]]; then
      echo "Missing ${tier_label} report for ${server} / ${database} in ${artifact_dir}" >&2
      exit 1
    fi
    if [[ "${#matches[@]}" -gt 1 ]]; then
      echo "Found duplicate ${tier_label} reports for ${server} / ${database} in ${artifact_dir}" >&2
      printf '  %s\n' "${matches[@]}" >&2
      exit 1
    fi
    if [[ ! -s "${matches[0]}" ]]; then
      echo "Found empty ${tier_label} report for ${server} / ${database}: ${matches[0]}" >&2
      exit 1
    fi
    if grep -Fq "${PLACEHOLDER_TEXT}" "${matches[0]}"; then
      echo "Found placeholder ${tier_label} report for ${server} / ${database}: ${matches[0]}" >&2
      exit 1
    fi

    sources+=("${matches[0]}")
    destinations+=("${tier}/${server}-${database}.md")
    titles+=("${server_label} / ${database_label}")
  done
done

expected_combinations="${#combinations[@]}"
expected_reports=$(( expected_combinations * ${#TIER_ENTRIES[@]} ))
if [[ "${#sources[@]}" -ne "${expected_reports}" ]]; then
  echo "Expected ${expected_reports} runtime reports, found ${#sources[@]}" >&2
  exit 1
fi

stage_root="$(mktemp -d "${TMPDIR:-/tmp}/ratchet-runtime-reports.XXXXXX")"
trap 'rm -rf "${stage_root}"' EXIT

for ((index = 0; index < ${#sources[@]}; index++)); do
  staged_file="${stage_root}/${destinations[index]}"
  mkdir -p "$(dirname "${staged_file}")"
  {
    printf -- '---\n'
    printf 'title: "%s"\n' "${titles[index]}"
    printf -- '---\n\n'
    cat "${sources[index]}"
  } > "${staged_file}"
done

staged_count="$(find "${stage_root}" -type f -name '*.md' | wc -l | tr -d ' ')"
if [[ "${staged_count}" -ne "${expected_reports}" ]]; then
  echo "Expected ${expected_reports} staged runtime reports, found ${staged_count}" >&2
  exit 1
fi
if grep -R -Fq "${PLACEHOLDER_TEXT}" "${stage_root}"; then
  echo 'A staged runtime conformance page still contains placeholder content' >&2
  exit 1
fi

mkdir -p "${docs_root}/api" "${docs_root}/jakarta"
for staged_file in "${stage_root}/api/"*.md "${stage_root}/jakarta/"*.md; do
  tier="$(basename "$(dirname "${staged_file}")")"
  cp "${staged_file}" "${docs_root}/${tier}/$(basename "${staged_file}")"
done

for tier_entry in "${TIER_ENTRIES[@]}"; do
  tier="${tier_entry%%:*}"
  for output_file in "${docs_root}/${tier}/"*.md; do
    [[ -e "${output_file}" ]] || continue
    filename="${output_file##*/}"
    [[ "${filename}" != 'index.md' ]] || continue

    keep_page=0
    for destination in "${destinations[@]}"; do
      if [[ "${destination}" == "${tier}/${filename}" ]]; then
        keep_page=1
        break
      fi
    done

    if [[ "${keep_page}" -eq 0 ]]; then
      rm "${output_file}"
      echo "Pruned stale conformance page: ${output_file}"
    fi
  done
done

for destination in "${destinations[@]}"; do
  output_file="${docs_root}/${destination}"
  if [[ ! -s "${output_file}" ]]; then
    echo "Materialized runtime report is missing or empty: ${output_file}" >&2
    exit 1
  fi
  if grep -Fq "${PLACEHOLDER_TEXT}" "${output_file}"; then
    echo "Materialized runtime report is still a placeholder: ${output_file}" >&2
    exit 1
  fi
done

echo "Materialized ${expected_reports} runtime conformance reports for ${expected_combinations} server/database combinations"
