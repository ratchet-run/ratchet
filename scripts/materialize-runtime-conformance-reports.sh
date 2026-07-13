#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CI_WORKFLOW_FILE="${CI_WORKFLOW_FILE:-${SCRIPT_DIR}/../.github/workflows/ci.yml}"

if [[ "$#" -ne 2 ]]; then
  echo "Usage: $0 <runtime-artifact-root> <conformance-docs-root>" >&2
  exit 2
fi

artifact_root="$1"
docs_root="$2"

if [[ ! -f "${CI_WORKFLOW_FILE}" ]]; then
  echo "CI workflow file does not exist: ${CI_WORKFLOW_FILE}" >&2
  exit 1
fi

if [[ ! -d "${artifact_root}" ]]; then
  echo "Runtime artifact root does not exist: ${artifact_root}" >&2
  exit 1
fi

matrix_output=''
if ! matrix_output="$(LC_ALL=C awk -v workflow_file="${CI_WORKFLOW_FILE}" '
  function trim(value) {
    sub(/^[[:space:]]+/, "", value)
    sub(/[[:space:]]+$/, "", value)
    return value
  }

  function indentation(line, leading) {
    leading = line
    sub(/[^ ].*$/, "", leading)
    return length(leading)
  }

  function set_parse_error(message) {
    if (parse_error == "") {
      parse_error = message
    }
  }

  function parse_inline_list(line, kind, body, count, values, list_index, value) {
    body = line
    sub(/^[^[]*\[/, "", body)
    sub(/\][[:space:]]*$/, "", body)
    body = trim(body)
    if (body == "") {
      return
    }

    count = split(body, values, ",")
    for (list_index = 1; list_index <= count; list_index++) {
      value = trim(values[list_index])
      if (value == "") {
        continue
      }
      if (kind == "server") {
        base_server_count++
        base_servers[base_server_count] = value
      } else {
        base_database_count++
        base_databases[base_database_count] = value
      }
    }
  }

  function parse_include_field(field, value) {
    field = trim(field)
    if (field ~ /^server:[[:space:]]*/) {
      value = field
      sub(/^server:[[:space:]]*/, "", value)
      item_server = trim(value)
    } else if (field ~ /^database:[[:space:]]*/) {
      value = field
      sub(/^database:[[:space:]]*/, "", value)
      item_database = trim(value)
    }
  }

  function finish_include_item(missing) {
    if (!include_item_open) {
      return
    }

    missing = ""
    if (item_server == "") {
      missing = "server"
    }
    if (item_database == "") {
      if (missing == "") {
        missing = "database"
      } else {
        missing = missing " and database"
      }
    }

    if (missing != "") {
      set_parse_error("integration-test matrix include item " include_item_number \
        " is missing " missing " in " workflow_file)
    } else {
      include_count++
      include_servers[include_count] = item_server
      include_databases[include_count] = item_database
    }

    include_item_open = 0
    item_server = ""
    item_database = ""
  }

  function add_combination(server, database, key) {
    key = server SUBSEP database
    if (!(key in seen_combinations)) {
      seen_combinations[key] = 1
      combination_count++
      combination_servers[combination_count] = server
      combination_databases[combination_count] = database
    }
  }

  {
    raw_line = $0
    if (raw_line ~ /^[[:space:]]*$/ || raw_line ~ /^[[:space:]]*#/) {
      next
    }

    indent = indentation(raw_line)

    if (!in_job) {
      if (raw_line ~ /^  integration-test:[[:space:]]*$/) {
        in_job = 1
      }
      next
    }

    if (raw_line ~ /^  [[:alnum:]_-]+:[[:space:]]*$/) {
      finish_include_item()
      exit
    }

    if (!in_strategy) {
      if (raw_line ~ /^    strategy:[[:space:]]*$/) {
        in_strategy = 1
      }
      next
    }

    if (!in_matrix) {
      if (raw_line ~ /^      matrix:[[:space:]]*$/) {
        in_matrix = 1
      } else if (indent <= 4) {
        in_strategy = 0
      }
      next
    }

    if (indent <= 6) {
      finish_include_item()
      exit
    }

    line = raw_line
    sub(/[[:space:]]+#.*$/, "", line)

    if (indent == 8 && line ~ /^        server:[[:space:]]*\[[^]]*\][[:space:]]*$/) {
      finish_include_item()
      in_include = 0
      parse_inline_list(line, "server")
      next
    }
    if (indent == 8 && line ~ /^        database:[[:space:]]*\[[^]]*\][[:space:]]*$/) {
      finish_include_item()
      in_include = 0
      parse_inline_list(line, "database")
      next
    }
    if (indent == 8 && line ~ /^        include:[[:space:]]*$/) {
      finish_include_item()
      in_include = 1
      next
    }

    if (in_include && line ~ /^          -[[:space:]]*/) {
      finish_include_item()
      include_item_number++
      include_item_open = 1
      field = line
      sub(/^          -[[:space:]]*/, "", field)
      parse_include_field(field)
      next
    }
    if (in_include && include_item_open && indent > 10) {
      parse_include_field(line)
      next
    }
    if (in_include && indent <= 8) {
      finish_include_item()
      in_include = 0
    }
  }

  END {
    finish_include_item()

    if (parse_error != "") {
      print parse_error > "/dev/stderr"
      exit 1
    }
    if (base_server_count == 0) {
      print "integration-test matrix server list is empty in " workflow_file > "/dev/stderr"
      exit 1
    }
    if (base_database_count == 0) {
      print "integration-test matrix database list is empty in " workflow_file > "/dev/stderr"
      exit 1
    }

    for (server_index = 1; server_index <= base_server_count; server_index++) {
      for (database_index = 1; database_index <= base_database_count; database_index++) {
        add_combination(base_servers[server_index], base_databases[database_index])
      }
    }
    for (include_index = 1; include_index <= include_count; include_index++) {
      add_combination(include_servers[include_index], include_databases[include_index])
    }

    for (left = 1; left <= combination_count; left++) {
      for (right = left + 1; right <= combination_count; right++) {
        left_value = combination_servers[left] "\t" combination_databases[left]
        right_value = combination_servers[right] "\t" combination_databases[right]
        if (right_value < left_value) {
          swap = combination_servers[left]
          combination_servers[left] = combination_servers[right]
          combination_servers[right] = swap
          swap = combination_databases[left]
          combination_databases[left] = combination_databases[right]
          combination_databases[right] = swap
        }
      }
    }

    for (combination_index = 1; combination_index <= combination_count; combination_index++) {
      print combination_servers[combination_index] "\t" combination_databases[combination_index]
    }
  }
' "${CI_WORKFLOW_FILE}")"; then
  exit 1
fi

combinations=()
while IFS=$'\t' read -r server database; do
  if [[ -n "${server}" && -n "${database}" ]]; then
    combinations+=("${server}:${database}")
  fi
done <<< "${matrix_output}"

server_label_for() {
  case "$1" in
    wildfly-managed) echo 'WildFly' ;;
    wildfly-ee11-managed) echo 'WildFly (EE 11)' ;;
    payara-managed) echo 'Payara' ;;
    openliberty-managed) echo 'Open Liberty' ;;
    glassfish-managed) echo 'GlassFish' ;;
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
