#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECKER_RELATIVE="integrations/ratchet-spring-boot/integration-tests/scripts/check-runtime-dependency-graph.py"
FIXTURES="$(mktemp -d "${TMPDIR:-/tmp}/ratchet-runtime-dependency-test.XXXXXX")"
trap 'rm -rf "${FIXTURES}"' EXIT

fail() {
  echo "runtime dependency graph script test: $*" >&2
  exit 1
}

make_fixture() {
  local name="$1"
  local mode="$2"
  local fixture="${FIXTURES}/${name}"
  local scripts_dir="${fixture}/integrations/ratchet-spring-boot/integration-tests/scripts"
  local target_dir="${fixture}/integrations/ratchet-spring-boot/integration-tests/target"
  local boot3_coordinate="org.example:runtime:jar:1.0"

  mkdir -p "${scripts_dir}" "${target_dir}"
  cp "${ROOT}/${CHECKER_RELATIVE}" "${scripts_dir}/"

  case "${mode}" in
    valid)
      boot3_coordinate="jakarta.enterprise:jakarta.enterprise.cdi-api:jar:4.1.0"
      ;;
    denied)
      boot3_coordinate="org.springframework:spring-jdbc:jar:6.2.0"
      ;;
    missing-lane)
      ;;
    *)
      fail "unknown fixture mode: ${mode}"
      ;;
  esac

  cat > "${fixture}/integrations/ratchet-spring-boot/integration-tests/scenario-manifest.json" <<'JSON'
{
  "schemaVersion": 1,
  "scenarios": {
    "mongodb-runtime": {
      "qualification": {
        "runtimeDependencyFlavor": "mongodb"
      },
      "dependencyPolicy": {
        "deniedPatterns": [
          "^org\\.springframework:spring-jdbc$",
          "^jakarta\\.enterprise:"
        ],
        "allowedExceptions": [
          "jakarta.enterprise:jakarta.enterprise.cdi-api"
        ]
      }
    }
  }
}
JSON

  jq -n \
    --arg boot3_coordinate "${boot3_coordinate}" \
    --arg mode "${mode}" '
      {
        schemaVersion: 1,
        runtimeDependencies: (
          [
            {
              lane: "boot-3.5",
              flavor: "mongodb",
              treeSha256: ("a" * 64),
              dependencies: [
                {
                  coordinate: $boot3_coordinate,
                  sha256: ("b" * 64),
                  scopes: ["compile"]
                }
              ]
            }
          ]
          + (
            if $mode == "missing-lane" then []
            else [
              {
                lane: "boot-4.1",
                flavor: "mongodb",
                treeSha256: ("c" * 64),
                dependencies: [
                  {
                    coordinate: "org.example:runtime:jar:2.0",
                    sha256: ("d" * 64),
                    scopes: ["runtime"]
                  }
                ]
              }
            ]
            end
          )
        )
      }
    ' > "${target_dir}/jvm-matrix-hashes.json"

  printf '%s\n' "${fixture}"
}

assert_success() {
  local fixture="$1"
  local output

  output="$(python3 "${fixture}/${CHECKER_RELATIVE}" mongodb)" \
    || fail "expected checker success"
  grep -Fq 'boot-3.5=1, boot-4.1=1 coordinates' <<<"${output}" \
    || fail "success summary omitted per-lane coordinate counts"
}

assert_failure() {
  local fixture="$1"
  local expected="$2"
  local output

  if output="$(python3 "${fixture}/${CHECKER_RELATIVE}" mongodb 2>&1)"; then
    fail "expected checker failure containing: ${expected}"
  fi
  grep -Fq "${expected}" <<<"${output}" \
    || fail "checker failure did not contain: ${expected}"
}

valid_fixture="$(make_fixture valid valid)"
assert_success "${valid_fixture}"

denied_fixture="$(make_fixture denied denied)"
assert_failure "${denied_fixture}" 'org.springframework:spring-jdbc:jar:6.2.0'

missing_lane_fixture="$(make_fixture missing-lane missing-lane)"
assert_failure "${missing_lane_fixture}" 'missing lanes: boot-4.1'

echo "Runtime dependency graph script self-tests passed."
