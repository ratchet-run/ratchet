#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MATERIALIZER="${SCRIPT_DIR}/materialize-runtime-conformance-reports.sh"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/ratchet-runtime-report-test.XXXXXX")"
trap 'rm -rf "${TMP_ROOT}"' EXIT

SERVERS=(
  wildfly-managed
  wildfly-ee11-managed
  payara-managed
  openliberty-managed
  glassfish-managed
)
DATABASES=(mysql postgresql oracle sqlserver mongodb)
TIERS=(api jakarta)
EXPECTED_REPORT_COUNT=$(( ${#SERVERS[@]} * ${#DATABASES[@]} * ${#TIERS[@]} ))

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

server_label() {
  case "$1" in
    wildfly-managed) echo 'WildFly' ;;
    wildfly-ee11-managed) echo 'WildFly (EE 11)' ;;
    payara-managed) echo 'Payara' ;;
    openliberty-managed) echo 'Open Liberty' ;;
    glassfish-managed) echo 'GlassFish' ;;
    *) fail "unknown server: $1" ;;
  esac
}

database_label() {
  case "$1" in
    mysql) echo 'MySQL' ;;
    postgresql) echo 'PostgreSQL' ;;
    oracle) echo 'Oracle' ;;
    sqlserver) echo 'SQL Server' ;;
    mongodb) echo 'MongoDB' ;;
    *) fail "unknown database: $1" ;;
  esac
}

create_fixture() {
  local layout="$1"
  local fixture_root="$2"
  local server database tier artifact_dir report_dir

  mkdir -p "${fixture_root}/runtime" "${fixture_root}/docs/api" "${fixture_root}/docs/jakarta"

  for server in "${SERVERS[@]}"; do
    for database in "${DATABASES[@]}"; do
      artifact_dir="${fixture_root}/runtime/tck-conformance-reports-${server}-${database}"
      case "${layout}" in
        flattened)
          report_dir="${artifact_dir}"
          ;;
        nested)
          report_dir="${artifact_dir}/testing/ratchet-testsuite/target"
          ;;
        *)
          fail "unknown fixture layout: ${layout}"
          ;;
      esac
      mkdir -p "${report_dir}"

      for tier in "${TIERS[@]}"; do
        printf '# %s report for %s / %s\n\nfixture-result: PASS\n' \
          "${tier}" "${server}" "${database}" \
          > "${report_dir}/tck-${tier}-conformance-report.md"
        cat > "${fixture_root}/docs/${tier}/${server}-${database}.md" <<'EOF'
---
title: Placeholder
---

> This page is generated automatically after each successful CI run on `main`.
EOF
      done
    done
  done
}

assert_materialized() {
  local docs_root="$1"
  local actual_count server database tier report_file expected_frontmatter actual_frontmatter
  actual_count="$(find "${docs_root}/api" "${docs_root}/jakarta" -type f -name '*.md' | wc -l | tr -d ' ')"
  [[ "${actual_count}" == "${EXPECTED_REPORT_COUNT}" ]] \
    || fail "expected ${EXPECTED_REPORT_COUNT} materialized reports, found ${actual_count}"

  for server in "${SERVERS[@]}"; do
    for database in "${DATABASES[@]}"; do
      expected_frontmatter="$(printf '%s\n%s\n%s' \
        '---' "title: \"$(server_label "${server}") / $(database_label "${database}")\"" '---')"
      for tier in "${TIERS[@]}"; do
        report_file="${docs_root}/${tier}/${server}-${database}.md"
        actual_frontmatter="$(sed -n '1,3p' "${report_file}")"
        [[ "${actual_frontmatter}" == "${expected_frontmatter}" ]] \
          || fail "incorrect VitePress frontmatter in ${report_file}"
        grep -Fq 'fixture-result: PASS' "${report_file}" \
          || fail "report body was not copied to ${report_file}"
      done
    done
  done

  if grep -R -Fq 'This page is generated automatically after each successful CI run' \
    "${docs_root}/api" "${docs_root}/jakarta"; then
    fail "a placeholder page remained after materialization"
  fi
}

assert_legacy_flattened_regression() {
  local fixture_root="$1"
  local found=0 server database tier legacy_path

  for server in "${SERVERS[@]}"; do
    for database in "${DATABASES[@]}"; do
      for tier in "${TIERS[@]}"; do
        legacy_path="${fixture_root}/runtime/tck-conformance-reports-${server}-${database}/testing/ratchet-testsuite/target/tck-${tier}-conformance-report.md"
        [[ ! -f "${legacy_path}" ]] || found=$((found + 1))
      done
    done
  done

  [[ "${found}" -eq 0 ]] \
    || fail "flattened fixture unexpectedly satisfied ${found} legacy report paths"
  echo "BASELINE: the original nested-path copy logic finds 0 of ${EXPECTED_REPORT_COUNT} flattened reports"
}

expect_failure() {
  local expected_message="$1"
  shift
  local stderr_file="${TMP_ROOT}/failure.stderr"

  if "$@" > /dev/null 2> "${stderr_file}"; then
    fail "command unexpectedly succeeded: $*"
  fi
  grep -Fq "${expected_message}" "${stderr_file}" \
    || fail "failure did not mention '${expected_message}': $(cat "${stderr_file}")"
}

flat_root="${TMP_ROOT}/flattened"
create_fixture flattened "${flat_root}"

# The download action flattens the common upload prefix. Confirm that the
# original inline workflow's nested source path misses every report.
assert_legacy_flattened_regression "${flat_root}"

"${MATERIALIZER}" "${flat_root}/runtime" "${flat_root}/docs"
assert_materialized "${flat_root}/docs"

nested_root="${TMP_ROOT}/nested"
create_fixture nested "${nested_root}"
"${MATERIALIZER}" "${nested_root}/runtime" "${nested_root}/docs"
assert_materialized "${nested_root}/docs"

missing_root="${TMP_ROOT}/missing"
create_fixture flattened "${missing_root}"
rm "${missing_root}/runtime/tck-conformance-reports-payara-managed-oracle/tck-jakarta-conformance-report.md"
expect_failure 'Missing Jakarta report for payara-managed / oracle' \
  "${MATERIALIZER}" "${missing_root}/runtime" "${missing_root}/docs"

duplicate_root="${TMP_ROOT}/duplicate"
create_fixture flattened "${duplicate_root}"
mkdir -p "${duplicate_root}/runtime/tck-conformance-reports-glassfish-managed-mongodb/nested"
cp "${duplicate_root}/runtime/tck-conformance-reports-glassfish-managed-mongodb/tck-api-conformance-report.md" \
  "${duplicate_root}/runtime/tck-conformance-reports-glassfish-managed-mongodb/nested/tck-api-conformance-report.md"
expect_failure 'duplicate API reports for glassfish-managed / mongodb' \
  "${MATERIALIZER}" "${duplicate_root}/runtime" "${duplicate_root}/docs"

empty_root="${TMP_ROOT}/empty"
create_fixture flattened "${empty_root}"
: > "${empty_root}/runtime/tck-conformance-reports-wildfly-managed-postgresql/tck-api-conformance-report.md"
expect_failure 'empty API report for wildfly-managed / postgresql' \
  "${MATERIALIZER}" "${empty_root}/runtime" "${empty_root}/docs"

placeholder_root="${TMP_ROOT}/placeholder"
create_fixture flattened "${placeholder_root}"
cat > "${placeholder_root}/runtime/tck-conformance-reports-openliberty-managed-sqlserver/tck-jakarta-conformance-report.md" <<'EOF'
> This page is generated automatically after each successful CI run on `main`.
EOF
expect_failure 'placeholder Jakarta report for openliberty-managed / sqlserver' \
  "${MATERIALIZER}" "${placeholder_root}/runtime" "${placeholder_root}/docs"

echo 'PASS: runtime conformance reports materialize from flattened and nested artifacts'
echo 'PASS: missing, duplicate, empty, and placeholder reports fail closed'
