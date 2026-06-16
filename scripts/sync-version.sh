#!/usr/bin/env bash
#
# sync-version.sh — set every non-pom reference to the Ratchet artifact version.
#
# The Maven poms carry the canonical version (managed by `mvn versions:set`).
# A scattering of docs, the loadtest Dockerfile, and the bug-report template
# repeat that version by hand, so they drift the moment a release bumps the
# poms. This script rewrites those hand-written copies to whatever version you
# pass in, so the two can never disagree again.
#
# Usage:
#   scripts/sync-version.sh <version>
#   scripts/sync-version.sh 0.1.1-SNAPSHOT
#
# It is idempotent: it SETS each reference to <version> regardless of the value
# already there, so you never need to know the old version. Running it twice
# with the same argument produces no diff.
#
# It only touches the Ratchet artifact version. Replacements are anchored to
# Ratchet context (a run.ratchet dependency block, a ratchet-* jar/war
# filename, or a specific sentence/placeholder), so unrelated versions — Java
# 17, MySQL 8, Jakarta EE 10/11, MongoDB 6, etc. — are left alone.

set -euo pipefail

if [[ $# -ne 1 || -z "${1:-}" ]]; then
  echo "usage: $0 <version>   (e.g. 0.1.1-SNAPSHOT)" >&2
  exit 2
fi

VERSION="$1"

# Resolve repo root so the script works from any cwd.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# A Ratchet version is a SemVer core with an optional -SNAPSHOT (or other
# qualifier). This matches whatever is currently written so we can overwrite it
# without knowing it. It deliberately does NOT match bare "8" / "17" style
# numbers used for Java/MySQL/etc.
VER_RE='[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)?'

changed_files=()

# Run a perl in-place substitution against one file, anchored to Ratchet
# context. Echoes the file + a label when it actually changes something.
#   apply <file> <label> <perl-substitution>
apply() {
  local file="$1" label="$2" subst="$3"
  [[ -f "$file" ]] || { echo "skip (missing): $file" >&2; return; }
  local before after
  before="$(cat "$file")"
  VERSION="$VERSION" VER_RE="$VER_RE" perl -0777 -i -pe "$subst" "$file"
  after="$(cat "$file")"
  if [[ "$before" != "$after" ]]; then
    echo "  changed [$label]: $file"
    changed_files+=("$file")
  fi
}

# 1. Maven dependency snippets: a <version>…</version> line whose immediately
#    preceding artifactId is a Ratchet artifact under groupId run.ratchet.
#    Anchored on "run.ratchet" so non-Ratchet dependency blocks are untouched.
#    Files: README, getting-started + deployment install/quickstart/overview,
#    plus the per-module snippets (store-mongodb, micrometer).
MAVEN_FILES=(
  "README.md"                                 # ratchet-bom quick-start snippet
  "website/docs/getting-started/installation.md"  # ratchet-bom
  "website/docs/getting-started/quickstart.md"     # ratchet-bom
  "website/docs/deployment/installation.md"        # ratchet-bom
  "website/docs/deployment/overview.md"            # ratchet-bom
  "website/docs/deployment/mongodb.md"             # ratchet-store-mongodb
  "website/docs/deployment/monitoring.md"          # ratchet-micrometer
)
for f in "${MAVEN_FILES[@]}"; do
  apply "$f" "maven-version" \
    's{(<groupId>run\.ratchet</groupId>\s*<artifactId>ratchet[A-Za-z0-9-]*</artifactId>\s*<version>)$ENV{VER_RE}(</version>)}{$1$ENV{VERSION}$2}g'
done

# 2. ratchet-* jar/war filenames (extract-the-DDL snippets + the loadtest
#    Dockerfile artifact path). Anchored on the "ratchet-<artifact>-" prefix.
JAR_FILES=(
  "website/docs/deployment/database-setup.md"  # jar xf ratchet-store-*-VER.jar (x2)
  "website/docs/deployment/docker.md"          # jar xf ratchet-store-postgresql-VER.jar
  "infra/loadtest/Dockerfile"                  # ratchet-loadtest-VER.war COPY path
)
for f in "${JAR_FILES[@]}"; do
  apply "$f" "jar-war-filename" \
    's{(ratchet-[A-Za-z0-9-]*?-)$ENV{VER_RE}(\.(?:jar|war))}{$1$ENV{VERSION}$2}g'
done

# 3. Prose "version" sentences (bold **VER**). Anchored on the leading phrase so
#    only the project-status sentence is touched, never some other bold number.
apply "README.md" "prose-status" \
  's{(Ratchet is in \*\*)$ENV{VER_RE}(\*\*)}{$1$ENV{VERSION}$2}g'
apply "website/docs/getting-started/introduction.md" "prose-status" \
  's{(Ratchet is currently at version \*\*)$ENV{VER_RE}(\*\*)}{$1$ENV{VERSION}$2}g'

# 4. Bug-report template placeholder. Anchored on the trailing " or a commit SHA".
apply ".github/ISSUE_TEMPLATE/bug_report.yml" "issue-placeholder" \
  's{(placeholder: )$ENV{VER_RE}( or a commit SHA)}{$1$ENV{VERSION}$2}g'

echo ""
if [[ ${#changed_files[@]} -eq 0 ]]; then
  echo "sync-version: all references already at $VERSION (no changes)."
else
  echo "sync-version: set ${#changed_files[@]} file(s) to $VERSION."
fi
