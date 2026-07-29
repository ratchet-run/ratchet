#!/usr/bin/env bash
#
# sync-version.sh — sync project and published Ratchet version references.
#
# The Maven poms carry the canonical version (managed by `mvn versions:set`).
# Development references (project-status prose, verified-against notes, the
# loadtest Dockerfile, and the bug-report template) follow that version. Public
# dependency and JAR snippets follow the latest published release instead: a
# -SNAPSHOT coordinate is not copy-pasteable from Maven Central.
#
# Usage:
#   scripts/sync-version.sh <version>
#   scripts/sync-version.sh 0.1.2-SNAPSHOT
#   RELEASE_VERSION=0.1.1 scripts/sync-version.sh 0.1.2-SNAPSHOT
#
# A non-SNAPSHOT argument is both the project and public version. For a
# -SNAPSHOT argument, public references change only when RELEASE_VERSION names
# the release that was just published. The release workflow already exports
# that variable before it creates the next-development bump PR. Without it,
# public references stay unchanged.
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

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?$ ]]; then
  echo "invalid Ratchet version: $VERSION" >&2
  exit 2
fi

PUBLIC_VERSION=""
if [[ "$VERSION" != *-SNAPSHOT ]]; then
  PUBLIC_VERSION="$VERSION"
elif [[ -n "${RELEASE_VERSION:-}" ]]; then
  PUBLIC_VERSION="$RELEASE_VERSION"
fi

if [[ -n "$PUBLIC_VERSION" ]]; then
  if [[ ! "$PUBLIC_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?$ ]]; then
    echo "invalid published Ratchet version: $PUBLIC_VERSION" >&2
    exit 2
  fi
  if [[ "$PUBLIC_VERSION" == *-SNAPSHOT ]]; then
    echo "published Ratchet version must not be a SNAPSHOT: $PUBLIC_VERSION" >&2
    exit 2
  fi
fi

# Resolve repo root so the script works from any cwd.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# A Ratchet version is a SemVer core with an optional -SNAPSHOT (or other
# qualifier). This matches whatever is currently written so we can overwrite it
# without knowing it. It deliberately does NOT match bare "8" / "17" style
# numbers used for Java/MySQL/etc.
VER_RE='[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)?'
PUBLIC_REF_RE='(?:[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)?|\$\{ratchet\.version\})'

changed_files=()

# Run a perl in-place substitution against one file, anchored to Ratchet
# context. Echoes the file + a label when it actually changes something.
#   apply <file> <label> <perl-substitution>
apply() {
  local file="$1" label="$2" subst="$3"
  [[ -f "$file" ]] || { echo "skip (missing): $file" >&2; return; }
  local before after
  before="$(cat "$file")"
  VERSION="$VERSION" PUBLIC_VERSION="$PUBLIC_VERSION" VER_RE="$VER_RE" \
    PUBLIC_REF_RE="$PUBLIC_REF_RE" perl -0777 -i -pe "$subst" "$file"
  after="$(cat "$file")"
  if [[ "$before" != "$after" ]]; then
    echo "  changed [$label]: $file"
    changed_files+=("$file")
  fi
}

# 1. Public Maven dependency snippets: a <version>…</version> line whose immediately
#    preceding artifactId is a Ratchet artifact under groupId run.ratchet.
#    Anchored on "run.ratchet" so non-Ratchet dependency blocks are untouched.
#    These coordinates must resolve from Maven Central, so a development bump
#    leaves them alone unless RELEASE_VERSION identifies the release just cut.
PUBLIC_MAVEN_FILES=(
  "README.md"                                 # ratchet-bom quick-start snippet
  "website/docs/getting-started/installation.md"  # ratchet-bom
  "website/docs/getting-started/quickstart.md"     # ratchet-bom
  "website/docs/deployment/installation.md"        # ratchet-bom
  "website/docs/deployment/overview.md"            # ratchet-bom
  "website/docs/deployment/mongodb.md"             # ratchet-store-mongodb
  "website/docs/deployment/monitoring.md"          # ratchet-micrometer
  "website/docs/deployment/performance-tuning.md"  # ratchet-micrometer
  "website/docs/use-cases/durable-llm-workflows.md"   # ratchet + ratchet-store-postgresql
  "website/docs/advanced/metrics-collection.md"     # ratchet-micrometer
  "website/docs/advanced/spi-implementation.md"     # ratchet-tck-store
  "website/docs/concepts/overview.md"               # ratchet-bom
  "website/docs/conformance/adopting-the-tck.md"    # ratchet-bom
)
if [[ -n "$PUBLIC_VERSION" ]]; then
  for f in "${PUBLIC_MAVEN_FILES[@]}"; do
    apply "$f" "published-maven-version" \
      's{(<groupId>run\.ratchet</groupId>\s*<artifactId>ratchet[A-Za-z0-9-]*</artifactId>\s*<version>)$ENV{PUBLIC_REF_RE}(</version>)}{$1$ENV{PUBLIC_VERSION}$2}g'
  done
fi

# 1b. Source-built Maven dependency snippets: same anchoring as section 1, but
#     for artifacts that are NOT published to Maven Central. Readers install
#     these from a source checkout, so the snippets must show the project
#     version (SNAPSHOT included) rather than the last published release —
#     the opposite rule from section 1. Move a file up to PUBLIC_MAVEN_FILES
#     once its artifact actually ships.
PROJECT_MAVEN_FILES=(
  "website/docs/deployment/quarkus.md"   # ratchet-quarkus (unpublished; built from source)
)
for f in "${PROJECT_MAVEN_FILES[@]}"; do
  apply "$f" "project-maven-version" \
    's{(<groupId>run\.ratchet</groupId>\s*<artifactId>ratchet[A-Za-z0-9-]*</artifactId>\s*<version>)$ENV{PUBLIC_REF_RE}(</version>)}{$1$ENV{VERSION}$2}g'
done

# 2. Published ratchet-* JAR filenames used by extract-the-DDL snippets.
#    Oracle and SQL Server are included so future release references cannot
#    drift when their bundled-JAR instructions are present.
PUBLIC_JAR_FILES=(
  "website/docs/deployment/database-setup.md"
  "website/docs/deployment/docker.md"          # jar xf ratchet-store-postgresql-VER.jar
  "website/docs/deployment/oracle.md"
  "website/docs/deployment/sqlserver.md"
)
if [[ -n "$PUBLIC_VERSION" ]]; then
  for f in "${PUBLIC_JAR_FILES[@]}"; do
    apply "$f" "published-jar-filename" \
      's{(ratchet-[A-Za-z0-9-]*?-)$ENV{VER_RE}(\.jar)}{$1$ENV{PUBLIC_VERSION}$2}g'
  done
fi

# 2b. The loadtest WAR is built from this checkout, so it follows the project
#     version even during next-development bumps.
apply "infra/loadtest/Dockerfile" "project-war-filename" \
  's{(ratchet-loadtest-)$ENV{VER_RE}(\.war)}{$1$ENV{VERSION}$2}g'

# 3. Prose "version" sentences (bold **VER**). Anchored on the leading phrase so
#    only the project-status sentence is touched, never some other bold number.
apply "README.md" "prose-status" \
  's{(Ratchet is in \*\*)$ENV{VER_RE}(\*\*)}{$1$ENV{VERSION}$2}g'
apply "website/docs/getting-started/introduction.md" "prose-status" \
  's{(Ratchet is currently at version \*\*)$ENV{VER_RE}(\*\*)}{$1$ENV{VERSION}$2}g'

# 3b. Use-case "Verified" tip blocks: the inline prose `ratchet-api` `VER` claim.
#     Anchored on the backtick-wrapped artifact name so other code-span numbers
#     (langchain4j versions, Java 17) are untouched.
VERIFIED_TIP_FILES=(
  "website/docs/use-cases/durable-llm-workflows.md"
  "website/docs/use-cases/scheduled-recurring-jobs.md"
  "website/docs/use-cases/offload-after-request.md"
  "website/docs/use-cases/resilient-integrations.md"
  "website/docs/use-cases/bulk-batch-jobs.md"
  "website/docs/use-cases/human-in-the-loop.md"
)
for f in "${VERIFIED_TIP_FILES[@]}"; do
  apply "$f" "verified-tip" \
    's{(`ratchet-api` `)$ENV{VER_RE}(`)}{$1$ENV{VERSION}$2}g'
done

# 4. Bug-report template placeholder. Anchored on the trailing " or a commit SHA".
apply ".github/ISSUE_TEMPLATE/bug_report.yml" "issue-placeholder" \
  's{(placeholder: )$ENV{VER_RE}( or a commit SHA)}{$1$ENV{VERSION}$2}g'

echo ""
if [[ ${#changed_files[@]} -eq 0 ]]; then
  echo "sync-version: all selected references already match (no changes)."
else
  echo "sync-version: updated ${#changed_files[@]} file(s)."
fi
echo "  project version: $VERSION"
if [[ -n "$PUBLIC_VERSION" ]]; then
  echo "  published version: $PUBLIC_VERSION"
else
  echo "  published version: unchanged (RELEASE_VERSION not set)"
fi
