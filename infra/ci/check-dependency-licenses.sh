#!/usr/bin/env bash
# Resolve compile/runtime metadata, then enforce the checked-in license policy.
# Prerequisite: mvn -DskipTests -Dspotbugs.skip=true install
# Optional --offline uses only Maven's local repository (including plugins).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
maven_args=()
if [[ "${1:-}" == --offline && $# == 1 ]]; then
  maven_args+=(--offline)
elif [[ $# != 0 ]]; then
  echo "usage: $0 [--offline]" >&2
  exit 2
fi

REPORT="target/generated-sources/license/dependency-licenses.json"
mkdir -p target
# A failed or incomplete generation must never certify an earlier report.
rm -f "$REPORT"
echo "Resolving compile/runtime dependency license metadata..."
if ! mvn "${maven_args[@]}" -B -ntp license:aggregate-add-third-party \
  -Dlicense.force=true -Dlicense.failOnMissing=true \
  -Dlicense.excludedScopes=test,provided,system \
  -Dlicense.thirdPartyFilename=dependency-licenses.json \
  "-Dlicense.fileTemplate=$ROOT/infra/ci/dependency-licenses.ftl" \
  > target/dependency-licenses-maven.log 2>&1; then
  cat target/dependency-licenses-maven.log >&2
  exit 2
fi

if grep -q "could not be resolved at this point of the build but seem to be part of the reactor" \
  target/dependency-licenses-maven.log; then
  echo "ERROR: reactor not fully resolved — run 'mvn -DskipTests install' first." >&2
  exit 2
fi

python3 infra/ci/check_dependency_licenses.py "$REPORT"
