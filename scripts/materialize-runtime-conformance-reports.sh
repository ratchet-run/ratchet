#!/usr/bin/env bash

set -euo pipefail

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

SERVER_ENTRIES=(
  'wildfly-managed:WildFly'
  'wildfly-ee11-managed:WildFly (EE 11)'
  'payara-managed:Payara'
  'openliberty-managed:Open Liberty'
  'glassfish-managed:GlassFish'
)
DATABASE_ENTRIES=(
  'mysql:MySQL'
  'postgresql:PostgreSQL'
  'oracle:Oracle'
  'sqlserver:SQL Server'
  'mongodb:MongoDB'
)
TIER_ENTRIES=(
  'api:API'
  'jakarta:Jakarta'
)
PLACEHOLDER_TEXT='This page is generated automatically after each successful CI run'

sources=()
destinations=()
titles=()

for server_entry in "${SERVER_ENTRIES[@]}"; do
  server="${server_entry%%:*}"
  server_label="${server_entry#*:}"

  for database_entry in "${DATABASE_ENTRIES[@]}"; do
    database="${database_entry%%:*}"
    database_label="${database_entry#*:}"
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
done

expected_combinations=$(( ${#SERVER_ENTRIES[@]} * ${#DATABASE_ENTRIES[@]} ))
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
