#!/usr/bin/env bash
#
# verify-native-postgresql.sh — run the Boot 4.1 JVM control and native
# PostgreSQL scheduler against artifacts installed by the same verify.sh run.

set -euo pipefail

if [[ $# -ne 1 || "${1:-}" != -Dmaven.repo.local=* ]]; then
  echo "usage: $0 -Dmaven.repo.local=/absolute/isolated/repository" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
MATRIX="$ROOT/integrations/ratchet-spring-boot/integration-tests/compatibility-matrix.json"
POM="$ROOT/integrations/ratchet-spring-boot/integration-tests/native-postgresql/pom.xml"
TOOLCHAIN="$ROOT/integrations/ratchet-spring-boot/integration-tests/native-postgresql/native-toolchain.properties"
EVIDENCE_DIR="$ROOT/integrations/ratchet-spring-boot/integration-tests/native-postgresql/target/spring-boot-native-postgresql-evidence"
BUILD_LOG="$EVIDENCE_DIR/native-build.log"
RUN_LOG="$EVIDENCE_DIR/native-run.log"
RUN_STDERR_LOG="$EVIDENCE_DIR/native-run.stderr.log"
EVIDENCE_JSON="$EVIDENCE_DIR/native-evidence.json"
IMAGE_NAME="ratchet-native-postgresql:verify-$$"
NETWORK_NAME="ratchet-native-pg-$$"
POSTGRES_CONTAINER="ratchet-native-pg-db-$$"
NATIVE_CONTAINER="ratchet-native-pg-app-$$"
POSTGRES_IMAGE="postgres:16"
POSTGRES_ALIAS="postgresql"
POSTGRES_DATABASE="ratchet_spring_boot"
POSTGRES_USERNAME="ratchet"
POSTGRES_PASSWORD="ratchet"

for command in docker java mvn python3 sleep tee; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

MAVEN_REPO_ARGUMENT="${1#-Dmaven.repo.local=}"
MAVEN_REPO_ENVIRONMENT="${MAVEN_REPO_LOCAL:-}"
TRUSTED_INSTALL_REPO="${RATCHET_SPRING_BOOT_REACTOR_INSTALLED_REPO:-}"

MAVEN_REPO="$({
  python3 - \
    "$MAVEN_REPO_ARGUMENT" \
    "$MAVEN_REPO_ENVIRONMENT" \
    "$TRUSTED_INSTALL_REPO" <<'PY'
import os
import pathlib
import sys

labels = (
    "maven.repo.local argument",
    "MAVEN_REPO_LOCAL",
    "RATCHET_SPRING_BOOT_REACTOR_INSTALLED_REPO",
)
resolved = []
for label, raw_path in zip(labels, sys.argv[1:]):
    if not raw_path:
        raise SystemExit(f"{label} must be set")
    candidate = pathlib.Path(raw_path)
    if not candidate.is_absolute():
        raise SystemExit(f"{label} must be an absolute path: {candidate}")
    if not candidate.is_dir():
        raise SystemExit(f"{label} does not exist: {candidate}")
    resolved.append(candidate.resolve())

if len(set(resolved)) != 1:
    raise SystemExit(
        "isolated Maven repository inputs disagree:\n"
        + "\n".join(
            f"  {label}: {path}" for label, path in zip(labels, resolved)
        )
    )

shared = pathlib.Path(os.path.expanduser("~/.m2/repository")).resolve()
if resolved[0] == shared:
    raise SystemExit(f"refusing to use the shared Maven repository: {shared}")

print(resolved[0])
PY
} 2>&1)" || {
  echo "verify-native-postgresql.sh: $MAVEN_REPO" >&2
  exit 1
}
REPO_PROPERTY="-Dmaven.repo.local=$MAVEN_REPO"

for required_file in "$MATRIX" "$POM" "$TOOLCHAIN"; do
  if [[ ! -f "$required_file" ]]; then
    echo "required native PostgreSQL input is missing: $required_file" >&2
    exit 1
  fi
done

properties="$({
  python3 - "$TOOLCHAIN" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
required = {
    "builder.image.digest",
    "run.image.digest",
    "nik.version",
    "nik.assert.pattern",
}
values = {}
for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
    line = raw_line.strip()
    if not line or line.startswith("#"):
        continue
    key, separator, value = line.partition("=")
    key = key.strip()
    value = value.strip()
    if not separator or not key or not value:
        raise SystemExit(f"invalid property at {path}:{line_number}")
    if key in values:
        raise SystemExit(f"duplicate property at {path}:{line_number}: {key}")
    values[key] = value

missing = sorted(required - values.keys())
unexpected = sorted(values.keys() - required)
if missing or unexpected:
    parts = []
    if missing:
        parts.append("missing " + ", ".join(missing))
    if unexpected:
        parts.append("unexpected " + ", ".join(unexpected))
    raise SystemExit(f"invalid native toolchain properties: {'; '.join(parts)}")

image_pattern = re.compile(
    r"^[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}$"
)
for key in ("builder.image.digest", "run.image.digest"):
    if not image_pattern.fullmatch(values[key]):
        raise SystemExit(f"{key} must be a full digest-only image reference")
if not re.fullmatch(r"25[.]\d+[.]\d+", values["nik.version"]):
    raise SystemExit("nik.version must pin an exact Liberica NIK 25.x version")
try:
    re.compile(values["nik.assert.pattern"])
except re.error as exc:
    raise SystemExit(f"nik.assert.pattern is not a valid regular expression: {exc}")

for key in sorted(required):
    print(f"{key}\t{values[key]}")
PY
} 2>&1)" || {
  echo "verify-native-postgresql.sh: $properties" >&2
  exit 1
}

BUILDER_IMAGE=""
RUN_IMAGE=""
NIK_VERSION=""
NIK_ASSERT_PATTERN=""
while IFS=$'\t' read -r key value; do
  case "$key" in
    builder.image.digest) BUILDER_IMAGE="$value" ;;
    run.image.digest) RUN_IMAGE="$value" ;;
    nik.version) NIK_VERSION="$value" ;;
    nik.assert.pattern) NIK_ASSERT_PATTERN="$value" ;;
  esac
done <<< "$properties"

if [[ -z "$BUILDER_IMAGE" || -z "$RUN_IMAGE" || -z "$NIK_VERSION" \
      || -z "$NIK_ASSERT_PATTERN" ]]; then
  echo "native toolchain properties did not produce all required values" >&2
  exit 1
fi

CONSUMER_JAVA_HOME="${RATCHET_MATRIX_JAVA_HOME:-}"
if [[ -n "$CONSUMER_JAVA_HOME" ]]; then
  CONSUMER_JAVA_HOME="$({
    python3 - "$CONSUMER_JAVA_HOME" <<'PY'
import pathlib
import sys

candidate = pathlib.Path(sys.argv[1])
if not candidate.is_absolute():
    raise SystemExit(
        f"RATCHET_MATRIX_JAVA_HOME must be an absolute path: {candidate}"
    )
resolved = candidate.resolve()
if not (resolved / "bin" / "java").is_file():
    raise SystemExit(
        f"RATCHET_MATRIX_JAVA_HOME does not contain a java executable: {resolved}"
    )
print(resolved)
PY
  } 2>&1)" || {
    echo "verify-native-postgresql.sh: $CONSUMER_JAVA_HOME" >&2
    exit 1
  }
  CONSUMER_JAVA="$CONSUMER_JAVA_HOME/bin/java"
else
  CONSUMER_JAVA="java"
fi

python3 - "$MATRIX" "$CONSUMER_JAVA" "$CONSUMER_JAVA_HOME" <<'PY'
import json
import pathlib
import re
import subprocess
import sys

matrix_path = pathlib.Path(sys.argv[1])
java_binary = sys.argv[2]
java_home = sys.argv[3]
try:
    matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(f"cannot parse compatibility matrix {matrix_path}: {exc}")
expected = matrix.get("consumerJavaRuntime")
if not isinstance(expected, int) or isinstance(expected, bool):
    raise SystemExit("compatibility matrix consumerJavaRuntime must be an integer")

result = subprocess.run(
    [java_binary, "-XshowSettings:properties", "-version"],
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
match = re.search(
    r"^\s*java[.]specification[.]version\s*=\s*(\d+)\s*$",
    f"{result.stdout}\n{result.stderr}",
    re.MULTILINE,
)
if result.returncode != 0 or match is None:
    raise SystemExit("cannot determine the JVM-control Java runtime")
actual = int(match.group(1))
if actual != expected:
    source = f"RATCHET_MATRIX_JAVA_HOME {java_home}" if java_home else "ambient Java"
    raise SystemExit(
        f"{source} runs Java {actual}, but the compatibility matrix requires "
        f"the JVM control to run Java {expected}"
    )
PY

echo "Running Boot 4.1 native PostgreSQL JVM control on Java from ${CONSUMER_JAVA_HOME:-PATH}..."
if [[ -n "$CONSUMER_JAVA_HOME" ]]; then
  JAVA_HOME="$CONSUMER_JAVA_HOME" \
    PATH="$CONSUMER_JAVA_HOME/bin:$PATH" \
    mvn -B -ntp "$REPO_PROPERTY" -f "$POM" -Pboot-4.1 clean test
else
  mvn -B -ntp "$REPO_PROPERTY" -f "$POM" -Pboot-4.1 clean test
fi

mkdir -p "$EVIDENCE_DIR"

cleanup() {
  docker rm -f "$NATIVE_CONTAINER" >/dev/null 2>&1 || true
  docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
  docker network rm "$NETWORK_NAME" >/dev/null 2>&1 || true
  docker image rm "$IMAGE_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Building Boot 4.1 native PostgreSQL image with pinned Paketo references..."
set +e
mvn -B -ntp "$REPO_PROPERTY" -f "$POM" -Pboot-4.1,native \
  -DskipTests \
  "-Dspring-boot.build-image.imageName=$IMAGE_NAME" \
  "-Dspring-boot.build-image.builder=$BUILDER_IMAGE" \
  "-Dspring-boot.build-image.runImage=$RUN_IMAGE" \
  spring-boot:build-image 2>&1 | tee "$BUILD_LOG"
build_status=${PIPESTATUS[0]}
set -e
if [[ $build_status -ne 0 ]]; then
  echo "native PostgreSQL image build failed with exit code $build_status" >&2
  exit "$build_status"
fi

python3 - \
  "$BUILD_LOG" \
  "$BUILDER_IMAGE" \
  "$RUN_IMAGE" \
  "$NIK_VERSION" \
  "$NIK_ASSERT_PATTERN" <<'PY'
import pathlib
import re
import sys

log_path = pathlib.Path(sys.argv[1])
builder_image, run_image, nik_version, nik_pattern = sys.argv[2:]
log = log_path.read_text(encoding="utf-8", errors="replace")
for label, image in (("builder", builder_image), ("run", run_image)):
    if image not in log:
        raise SystemExit(
            f"native build output does not contain pinned {label} image digest: {image}"
        )
match = re.search(nik_pattern, log)
if match is None:
    raise SystemExit(
        f"native build output does not match nik.assert.pattern: {nik_pattern}"
    )
if nik_version not in match.group(0):
    raise SystemExit(
        "nik.assert.pattern matched output that does not contain the pinned "
        f"nik.version {nik_version}: {match.group(0)}"
    )
if not re.search(r"Executing native-image\s+--no-fallback(?:\s|$)", log):
    raise SystemExit("native build output does not prove a no-fallback native-image build")
PY

echo "Starting PostgreSQL $POSTGRES_IMAGE on Docker network $NETWORK_NAME..."
docker network create "$NETWORK_NAME" >/dev/null
docker run -d \
  --name "$POSTGRES_CONTAINER" \
  --network "$NETWORK_NAME" \
  --network-alias "$POSTGRES_ALIAS" \
  -e "POSTGRES_DB=$POSTGRES_DATABASE" \
  -e "POSTGRES_USER=$POSTGRES_USERNAME" \
  -e "POSTGRES_PASSWORD=$POSTGRES_PASSWORD" \
  "$POSTGRES_IMAGE" >/dev/null

postgres_ready=false
for ((attempt = 1; attempt <= 60; attempt++)); do
  if docker exec "$POSTGRES_CONTAINER" \
    pg_isready -U "$POSTGRES_USERNAME" -d "$POSTGRES_DATABASE" >/dev/null 2>&1; then
    postgres_ready=true
    break
  fi
  sleep 1
done
if [[ "$postgres_ready" != true ]]; then
  echo "PostgreSQL did not become ready within 60 seconds" >&2
  docker logs "$POSTGRES_CONTAINER" >&2 || true
  exit 1
fi

echo "Running native PostgreSQL image..."
set +e
docker run --rm \
  --name "$NATIVE_CONTAINER" \
  --network "$NETWORK_NAME" \
  -e "SPRING_DATASOURCE_URL=jdbc:postgresql://$POSTGRES_ALIAS:5432/$POSTGRES_DATABASE" \
  -e "SPRING_DATASOURCE_USERNAME=$POSTGRES_USERNAME" \
  -e "SPRING_DATASOURCE_PASSWORD=$POSTGRES_PASSWORD" \
  "$IMAGE_NAME" \
  2> >(tee "$RUN_STDERR_LOG" >&2) | tee "$RUN_LOG"
native_status=${PIPESTATUS[0]}
set -e

python3 - \
  "$RUN_LOG" \
  "$EVIDENCE_JSON" \
  "$native_status" \
  "$BUILDER_IMAGE" \
  "$RUN_IMAGE" \
  "$NIK_VERSION" <<'PY'
import json
import os
import pathlib
import sys

run_log = pathlib.Path(sys.argv[1])
evidence_path = pathlib.Path(sys.argv[2])
native_exit_code = int(sys.argv[3])
builder_image, run_image, nik_version = sys.argv[4:]
prefix = "SPRING_BOOT_NATIVE_EVIDENCE="
required = [
    "schema-migration",
    "node-registration",
    "direct-submission",
    "method-reference-submission",
    "wrapper-submission",
    "persistence-claim",
    "retry-history",
    "recurring-execution",
    "jsonb-round-trip",
    "class-policy-creation-denial",
    "class-policy-invocation-denial",
    "clean-shutdown",
]
evidence = {}
errors = []

for line_number, line in enumerate(
    run_log.read_text(encoding="utf-8", errors="replace").splitlines(), 1
):
    if not line.startswith(prefix):
        continue
    raw_json = line[len(prefix) :]
    try:
        value = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        errors.append(f"line {line_number} has malformed evidence JSON: {exc}")
        continue
    if not isinstance(value, dict):
        errors.append(f"line {line_number} evidence must be a JSON object")
        continue
    scenario = value.get("scenario")
    if scenario not in required:
        errors.append(f"line {line_number} has unknown scenario: {scenario!r}")
        continue
    if scenario in evidence:
        errors.append(f"line {line_number} duplicates scenario: {scenario}")
        continue
    if not isinstance(value.get("passed"), bool):
        errors.append(f"line {line_number} scenario {scenario} has non-boolean passed")
        continue
    if not isinstance(value.get("detail"), str) or not value["detail"]:
        errors.append(f"line {line_number} scenario {scenario} has no detail")
        continue
    evidence[scenario] = value

missing = [scenario for scenario in required if scenario not in evidence]
if missing:
    errors.append("missing scenarios: " + ", ".join(missing))
failed = [
    scenario
    for scenario in required
    if scenario in evidence and evidence[scenario]["passed"] is not True
]
if failed:
    errors.append("failed scenarios: " + ", ".join(failed))
if native_exit_code != 0:
    errors.append(f"native image exited with code {native_exit_code}")

document = {
    "schemaVersion": 1,
    "toolchain": {
        "builderImage": builder_image,
        "runImage": run_image,
        "nikVersion": nik_version,
    },
    "scenarios": [evidence[scenario] for scenario in required if scenario in evidence],
}
evidence_path.parent.mkdir(parents=True, exist_ok=True)
temporary = evidence_path.with_name(f".{evidence_path.name}.{os.getpid()}.tmp")
with temporary.open("x", encoding="utf-8") as stream:
    json.dump(document, stream, indent=2, sort_keys=True)
    stream.write("\n")
    stream.flush()
    os.fsync(stream.fileno())
os.replace(temporary, evidence_path)

if errors:
    raise SystemExit(
        "native PostgreSQL evidence failed validation: " + "; ".join(errors)
    )
print(f"Validated twelve native PostgreSQL scenarios: {evidence_path}")
PY

echo "Spring Boot native PostgreSQL scheduler passed."
