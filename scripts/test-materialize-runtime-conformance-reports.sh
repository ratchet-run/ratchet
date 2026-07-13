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
DATABASES=(mysql postgresql mongodb oracle sqlserver)
TIERS=(api jakarta)
EXPECTED_REPORT_COUNT=$(( ${#SERVERS[@]} * ${#DATABASES[@]} * ${#TIERS[@]} ))
API_INDEX_CONTENT='fixture API conformance index'
JAKARTA_INDEX_CONTENT='fixture Jakarta conformance index'

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

print_inline_list() {
  local key="$1"
  shift
  local value separator=''

  printf '        %s: [' "${key}"
  for value in "$@"; do
    printf '%s%s' "${separator}" "${value}"
    separator=', '
  done
  printf ']\n'
}

write_ci_fixture() {
  local fixture_root="$1"
  local omitted_include="${2:-}"
  local server database
  local base_servers=()

  for server in "${SERVERS[@]}"; do
    if [[ "${server}" != 'glassfish-managed' ]]; then
      base_servers+=("${server}")
    fi
  done

  {
    printf 'name: Fixture CI\n\n'
    printf 'jobs:\n'
    printf '  integration-test:\n'
    printf '    runs-on: ubuntu-latest\n'
    printf '    strategy:\n'
    printf '      fail-fast: false\n'
    printf '      matrix:\n'
    print_inline_list server "${base_servers[@]}"
    printf '        # Fixture comment proves comments between matrix lists are ignored.\n'
    print_inline_list database "${DATABASES[@]}"
    printf '        include:\n'
    for database in "${DATABASES[@]}"; do
      if [[ "glassfish-managed:${database}" == "${omitted_include}" ]]; then
        continue
      fi
      printf '          - server: glassfish-managed\n'
      printf '            database: %s\n' "${database}"
    done
    printf '  showcase-smoke:\n'
    printf '    runs-on: ubuntu-latest\n'
  } > "${fixture_root}/ci.yml"
}

write_empty_server_ci_fixture() {
  local fixture_root="$1"

  {
    printf 'jobs:\n'
    printf '  integration-test:\n'
    printf '    strategy:\n'
    printf '      fail-fast: false\n'
    printf '      matrix:\n'
    print_inline_list server
    printf '        # Empty-server failure fixture comment.\n'
    print_inline_list database "${DATABASES[@]}"
    printf '  showcase-smoke:\n'
  } > "${fixture_root}/ci.yml"
}

write_unknown_label_ci_fixture() {
  local fixture_root="$1"

  {
    printf 'jobs:\n'
    printf '  integration-test:\n'
    printf '    strategy:\n'
    printf '      fail-fast: false\n'
    printf '      matrix:\n'
    print_inline_list server weblogic-managed
    printf '        # Unknown-label failure fixture comment.\n'
    print_inline_list database mysql
    printf '  showcase-smoke:\n'
  } > "${fixture_root}/ci.yml"
}

create_fixture() {
  local layout="$1"
  local fixture_root="$2"
  local server database tier artifact_dir report_dir

  mkdir -p "${fixture_root}/runtime" "${fixture_root}/docs/api" "${fixture_root}/docs/jakarta"
  write_ci_fixture "${fixture_root}"
  printf '%s' "${API_INDEX_CONTENT}" > "${fixture_root}/docs/api/index.md"
  printf '%s' "${JAKARTA_INDEX_CONTENT}" > "${fixture_root}/docs/jakarta/index.md"

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

assert_indexes_preserved() {
  local docs_root="$1"
  local api_content jakarta_content

  [[ -f "${docs_root}/api/index.md" ]] || fail "API index.md was removed"
  [[ -f "${docs_root}/jakarta/index.md" ]] || fail "Jakarta index.md was removed"
  api_content="$(< "${docs_root}/api/index.md")"
  jakarta_content="$(< "${docs_root}/jakarta/index.md")"
  [[ "${api_content}" == "${API_INDEX_CONTENT}" ]] || fail "API index.md was modified"
  [[ "${jakarta_content}" == "${JAKARTA_INDEX_CONTENT}" ]] || fail "Jakarta index.md was modified"
}

assert_materialized() {
  local docs_root="$1"
  local omitted_server="${2:-}"
  local omitted_database="${3:-}"
  local expected_report_count="${EXPECTED_REPORT_COUNT}"
  local actual_count server database tier report_file expected_frontmatter actual_frontmatter

  if [[ -n "${omitted_server}" && -n "${omitted_database}" ]]; then
    expected_report_count=$(( expected_report_count - ${#TIERS[@]} ))
  fi

  actual_count="$(find "${docs_root}/api" "${docs_root}/jakarta" -type f -name '*.md' ! -name 'index.md' | wc -l | tr -d ' ')"
  [[ "${actual_count}" == "${expected_report_count}" ]] \
    || fail "expected ${expected_report_count} materialized reports, found ${actual_count}"

  for server in "${SERVERS[@]}"; do
    for database in "${DATABASES[@]}"; do
      if [[ "${server}" == "${omitted_server}" && "${database}" == "${omitted_database}" ]]; then
        for tier in "${TIERS[@]}"; do
          report_file="${docs_root}/${tier}/${server}-${database}.md"
          [[ ! -e "${report_file}" ]] || fail "removed matrix combination was materialized: ${report_file}"
        done
        continue
      fi

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

  assert_indexes_preserved "${docs_root}"
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

run_materializer() {
  local fixture_root="$1"
  CI_WORKFLOW_FILE="${fixture_root}/ci.yml" \
    "${MATERIALIZER}" "${fixture_root}/runtime" "${fixture_root}/docs"
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
printf 'retired conformance page' > "${flat_root}/docs/api/retired-server-mysql.md"

# The download action flattens the common upload prefix. Confirm that the
# original inline workflow's nested source path misses every report.
assert_legacy_flattened_regression "${flat_root}"

flat_output="$(run_materializer "${flat_root}")"
grep -Fq "Pruned stale conformance page: ${flat_root}/docs/api/retired-server-mysql.md" <<< "${flat_output}" \
  || fail "materializer did not report pruning the retired page"
[[ ! -e "${flat_root}/docs/api/retired-server-mysql.md" ]] \
  || fail "retired conformance page was not pruned"
assert_materialized "${flat_root}/docs"

nested_root="${TMP_ROOT}/nested"
create_fixture nested "${nested_root}"
run_materializer "${nested_root}"
assert_materialized "${nested_root}/docs"

missing_root="${TMP_ROOT}/missing"
create_fixture flattened "${missing_root}"
rm "${missing_root}/runtime/tck-conformance-reports-payara-managed-oracle/tck-jakarta-conformance-report.md"
expect_failure 'Missing Jakarta report for payara-managed / oracle' \
  run_materializer "${missing_root}"

duplicate_root="${TMP_ROOT}/duplicate"
create_fixture flattened "${duplicate_root}"
mkdir -p "${duplicate_root}/runtime/tck-conformance-reports-glassfish-managed-mongodb/nested"
cp "${duplicate_root}/runtime/tck-conformance-reports-glassfish-managed-mongodb/tck-api-conformance-report.md" \
  "${duplicate_root}/runtime/tck-conformance-reports-glassfish-managed-mongodb/nested/tck-api-conformance-report.md"
expect_failure 'duplicate API reports for glassfish-managed / mongodb' \
  run_materializer "${duplicate_root}"

empty_root="${TMP_ROOT}/empty"
create_fixture flattened "${empty_root}"
: > "${empty_root}/runtime/tck-conformance-reports-wildfly-managed-postgresql/tck-api-conformance-report.md"
expect_failure 'empty API report for wildfly-managed / postgresql' \
  run_materializer "${empty_root}"

placeholder_root="${TMP_ROOT}/placeholder"
create_fixture flattened "${placeholder_root}"
cat > "${placeholder_root}/runtime/tck-conformance-reports-openliberty-managed-sqlserver/tck-jakarta-conformance-report.md" <<'EOF'
> This page is generated automatically after each successful CI run on `main`.
EOF
expect_failure 'placeholder Jakarta report for openliberty-managed / sqlserver' \
  run_materializer "${placeholder_root}"

combo_root="${TMP_ROOT}/combo-removal"
create_fixture flattened "${combo_root}"
write_ci_fixture "${combo_root}" 'glassfish-managed:sqlserver'
[[ -d "${combo_root}/runtime/tck-conformance-reports-glassfish-managed-sqlserver" ]] \
  || fail "removed combination artifact directory was not present before materialization"
[[ -f "${combo_root}/docs/api/glassfish-managed-sqlserver.md" ]] \
  || fail "removed API combination page was not pre-seeded"
[[ -f "${combo_root}/docs/jakarta/glassfish-managed-sqlserver.md" ]] \
  || fail "removed Jakarta combination page was not pre-seeded"
combo_output="$(run_materializer "${combo_root}")"
grep -Fq 'Materialized 48 runtime conformance reports for 24 server/database combinations' <<< "${combo_output}" \
  || fail "reduced matrix summary did not report 48 reports for 24 combinations"
[[ -d "${combo_root}/runtime/tck-conformance-reports-glassfish-managed-sqlserver" ]] \
  || fail "materializer touched the removed combination artifact directory"
assert_materialized "${combo_root}/docs" glassfish-managed sqlserver

parse_root="${TMP_ROOT}/parse-failure"
mkdir -p "${parse_root}/runtime"
write_empty_server_ci_fixture "${parse_root}"
expect_failure 'integration-test matrix server list is empty' \
  run_materializer "${parse_root}"

unknown_label_root="${TMP_ROOT}/unknown-label"
mkdir -p "${unknown_label_root}/runtime"
write_unknown_label_ci_fixture "${unknown_label_root}"
expect_failure 'weblogic-managed' \
  run_materializer "${unknown_label_root}"

echo 'PASS: runtime conformance reports materialize from flattened and nested artifacts'
echo 'PASS: missing, duplicate, empty, and placeholder reports fail closed'
echo 'PASS: stale conformance pages are pruned while index pages are preserved'
echo 'PASS: ci.yml matrix removal materializes 48 reports for 24 combinations'
echo 'PASS: empty matrices and unknown display labels fail closed'
