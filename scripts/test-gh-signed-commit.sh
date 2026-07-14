#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SIGNED_COMMIT_SCRIPT="${ROOT}/scripts/gh-signed-commit.sh"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/ratchet-gh-signed-commit-test.XXXXXX")"
trap 'rm -rf "${TMP_ROOT}"' EXIT

FIXTURE_REPO="${TMP_ROOT}/repo"
SHIM_DIR="${TMP_ROOT}/bin"
CAPTURED_REQUEST="${TMP_ROOT}/request.json"
STDERR_FILE="${TMP_ROOT}/stderr"
BODY_FILE="${TMP_ROOT}/body.txt"
DECODED_FILE="${TMP_ROOT}/decoded.txt"
ADDITION_COUNT=60
DELETION_COUNT=3

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

mkdir -p "${FIXTURE_REPO}/tracked" "${FIXTURE_REPO}/deleted" "${SHIM_DIR}"
git -C "${FIXTURE_REPO}" init -q

# Sixty 40 KiB files become a request larger than typical ARG_MAX limits once
# base64-encoded. This is deliberately release-sized so the former --argjson
# handoff fails before the gh shim can be reached.
dd if=/dev/zero bs=1024 count=40 2>/dev/null \
  | LC_ALL=C tr '\000' 'a' > "${TMP_ROOT}/original-40k.txt"
dd if=/dev/zero bs=1024 count=40 2>/dev/null \
  | LC_ALL=C tr '\000' 'b' > "${TMP_ROOT}/modified-40k.txt"

for ((i = 1; i <= ADDITION_COUNT; i++)); do
  path="$(printf 'tracked/file-%03d.txt' "${i}")"
  cp "${TMP_ROOT}/original-40k.txt" "${FIXTURE_REPO}/${path}"
done

for ((i = 1; i <= DELETION_COUNT; i++)); do
  path="$(printf 'deleted/file-%03d.txt' "${i}")"
  printf 'delete fixture %03d\n' "${i}" > "${FIXTURE_REPO}/${path}"
done

git -C "${FIXTURE_REPO}" add tracked deleted
git -C "${FIXTURE_REPO}" \
  -c user.name='Ratchet Test' \
  -c user.email='ratchet-test@example.invalid' \
  -c commit.gpgsign=false \
  -c core.hooksPath=/dev/null \
  commit -qm 'fixture baseline'

for ((i = 1; i <= ADDITION_COUNT; i++)); do
  path="$(printf 'tracked/file-%03d.txt' "${i}")"
  cp "${TMP_ROOT}/modified-40k.txt" "${FIXTURE_REPO}/${path}"
done
rm "${FIXTURE_REPO}"/deleted/*.txt

actual_changed_count="$(git -C "${FIXTURE_REPO}" diff --name-only HEAD | wc -l | tr -d ' ')"
expected_changed_count=$(( ADDITION_COUNT + DELETION_COUNT ))
[[ "${actual_changed_count}" -eq "${expected_changed_count}" ]] \
  || fail "expected ${expected_changed_count} changed paths, found ${actual_changed_count}"

cat > "${SHIM_DIR}/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 4 || "$1" != 'api' || "$2" != 'graphql' || "$3" != '--input' ]]; then
  echo "unexpected gh invocation: $*" >&2
  exit 2
fi
if [[ "$4" == '-' ]]; then
  echo 'expected gh --input to name a request file' >&2
  exit 2
fi

cp "$4" "${GH_CAPTURE_FILE}"
printf '%s\n' '{"data":{"createCommitOnBranch":{"commit":{"oid":"deadbeef"}}}}'
EOF
chmod +x "${SHIM_DIR}/gh"

# The production runner uses GNU base64. Make the same `base64 -w0` interface
# available when this test is run on macOS, whose system base64 is BSD-based.
REAL_BASE64="$(command -v base64)"
export REAL_BASE64
cat > "${SHIM_DIR}/base64" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == '-w0' ]]; then
  shift
  "${REAL_BASE64}" "$@" | tr -d '\n'
else
  exec "${REAL_BASE64}" "$@"
fi
EOF
chmod +x "${SHIM_DIR}/base64"

printf '%s\n' 'Fixture body' 'Signed-off-by: Ratchet Test <ratchet-test@example.invalid>' \
  > "${BODY_FILE}"
expected_head_oid="$(git -C "${FIXTURE_REPO}" rev-parse HEAD)"

if stdout="$(
  cd "${FIXTURE_REPO}"
  GH_CAPTURE_FILE="${CAPTURED_REQUEST}" \
    PATH="${SHIM_DIR}:${PATH}" \
    "${SIGNED_COMMIT_SCRIPT}" \
      'ratchet-run/ratchet' \
      'release-fixture' \
      "${expected_head_oid}" \
      'Exercise release-sized signed commit' \
      "${BODY_FILE}"
)" 2> "${STDERR_FILE}"; then
  :
else
  status=$?
  cat "${STDERR_FILE}" >&2
  fail "gh-signed-commit.sh exited ${status}"
fi

[[ "${stdout}" == 'deadbeef' ]] \
  || fail "expected stdout to be deadbeef, got: ${stdout}"
[[ -s "${CAPTURED_REQUEST}" ]] || fail 'gh shim did not capture a request'
jq empty "${CAPTURED_REQUEST}" || fail 'captured request is not valid JSON'

actual_additions="$(jq -r '.variables.input.fileChanges.additions | length' "${CAPTURED_REQUEST}")"
actual_deletions="$(jq -r '.variables.input.fileChanges.deletions | length' "${CAPTURED_REQUEST}")"
[[ "${actual_additions}" -eq "${ADDITION_COUNT}" ]] \
  || fail "expected ${ADDITION_COUNT} additions, found ${actual_additions}"
[[ "${actual_deletions}" -eq "${DELETION_COUNT}" ]] \
  || fail "expected ${DELETION_COUNT} deletions, found ${actual_deletions}"

round_trip_path='tracked/file-037.txt'
jq -j --arg path "${round_trip_path}" \
  '.variables.input.fileChanges.additions[]
   | select(.path == $path)
   | .contents
   | @base64d' \
  "${CAPTURED_REQUEST}" > "${DECODED_FILE}"
cmp "${FIXTURE_REPO}/${round_trip_path}" "${DECODED_FILE}" \
  || fail "base64 contents did not round-trip for ${round_trip_path}"

for ((i = 1; i <= DELETION_COUNT; i++)); do
  path="$(printf 'deleted/file-%03d.txt' "${i}")"
  jq -e --arg path "${path}" \
    '.variables.input.fileChanges.deletions | any(.path == $path)' \
    "${CAPTURED_REQUEST}" > /dev/null \
    || fail "captured request is missing deletion ${path}"
done

payload_bytes="$(wc -c < "${CAPTURED_REQUEST}" | tr -d ' ')"
[[ "${payload_bytes}" -gt 3000000 ]] \
  || fail "expected a regression payload larger than 3 MB, found ${payload_bytes} bytes"

echo "PASS: signed commit request contains ${ADDITION_COUNT} additions and ${DELETION_COUNT} deletions"
echo "PASS: ${payload_bytes}-byte request parses and file contents round-trip"
