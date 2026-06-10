#!/usr/bin/env bash
#
# Copyleft dependency-license gate.
#
# Fails if any compile- or runtime-scope dependency of a shipped Ratchet
# artifact carries a strong-copyleft license (GPL / LGPL / AGPL / SSPL) that
# would be incompatible with the Apache-2.0 distribution.
#
# This is the deterministic companion to the Eclipse Dash check: Dash is broad
# and SPDX-aware but depends on an external API, whereas this gate runs offline
# (beyond Maven's own resolution), is narrowly scoped to the copyleft risk, and
# is runnable locally before pushing.
#
# Not flagged, by design:
#   - test / provided / system scope — never redistributed, so their licenses
#     do not bind the release.
#   - Licenses with a linking carve-out — GPL "Classpath Exception" and the
#     MySQL "Universal FOSS Exception" — which are not copyleft for a consumer
#     that merely depends on the artifact.
#   - EPL / MPL / CDDL / EDL — weak, file-level copyleft, lawful to depend on.
#
# Prerequisite: a fully installed reactor so every module resolves, e.g.
#   mvn -DskipTests install
# Run from the repository root.
#
set -euo pipefail

REPORT="target/generated-sources/license/THIRD-PARTY.txt"

# Strong-copyleft license families, matched case-insensitively against the
# generated license names.
COPYLEFT='\b(GNU (General|Lesser General|Library General) Public License|GPL|LGPL|Affero|AGPL|Server Side Public License|SSPL)\b'

# Carve-outs that neutralize copyleft for a linking consumer. Matched against
# the same license names; a hit here clears an otherwise-flagged line.
EXCEPTIONS='classpath[ -]exception|foss[ -]exception'

echo "Generating third-party license report (compile + runtime scope)..."
# force=true defeats the plugin's up-to-date short-circuit, which would
# otherwise skip regeneration and quietly certify a stale report.
out=$(mvn -B -ntp license:aggregate-add-third-party \
  -Dlicense.force=true \
  -Dlicense.excludedScopes=test,provided,system 2>&1) || { echo "$out"; exit 2; }

# Fail safe: an un-installed reactor silently drops modules from the aggregate,
# turning the gate into a false pass. Refuse to certify an incomplete run.
if grep -q "could not be resolved at this point of the build but seem to be part of the reactor" <<<"$out"; then
  echo "ERROR: reactor not fully resolved — run 'mvn -DskipTests install' first." >&2
  exit 2
fi

[ -f "$REPORT" ] || { echo "ERROR: $REPORT was not generated." >&2; exit 2; }

hits=$(grep -Ei "$COPYLEFT" "$REPORT" | grep -viE "$EXCEPTIONS" || true)
if [ -n "$hits" ]; then
  echo "FAIL: strong-copyleft license(s) in shipped (compile/runtime) scope:" >&2
  echo "$hits" >&2
  echo >&2
  echo "Remove the dependency, move it to test/provided scope, or — if it carries" >&2
  echo "a linking exception — extend EXCEPTIONS in this script." >&2
  exit 1
fi

echo "OK: no GPL/LGPL/AGPL/SSPL in compile/runtime scope ($(grep -c ' - ' "$REPORT") dependencies checked)."
