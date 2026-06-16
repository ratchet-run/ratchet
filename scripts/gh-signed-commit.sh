#!/usr/bin/env bash
#
# gh-signed-commit.sh — create a Verified commit via the GitHub GraphQL
# `createCommitOnBranch` mutation, so the release workflow never has to manage
# a signing key. GitHub signs commits made through this API with its own key,
# and they show up as "Verified".
#
# It takes the changes already present in the working tree (staged or not),
# turns them into a fileChanges payload, and commits them onto an existing
# branch ref at a known head oid.
#
# Usage:
#   gh-signed-commit.sh <owner/repo> <branch> <expected-head-oid> <headline> <body-file>
#
# Arguments:
#   owner/repo         e.g. ratchet-run/ratchet
#   branch             the branch ref to commit onto (must already exist)
#   expected-head-oid  the commit oid the branch is expected to point at; the
#                      mutation fails if the branch moved, which is the
#                      optimistic-lock that keeps reruns honest
#   headline           the commit message headline
#   body-file          path to a file holding the commit message body (already
#                      including the Signed-off-by trailer); pass /dev/null for
#                      no body
#
# Requires: gh (authenticated), jq, git. Reads the changed files from
# `git diff --name-only HEAD` plus `git diff --name-only --cached`.
#
# Prints the new commit oid on stdout.

set -euo pipefail

REPO="$1"
BRANCH="$2"
EXPECTED_HEAD_OID="$3"
HEADLINE="$4"
BODY_FILE="${5:-/dev/null}"

# Collect every TRACKED path that differs from HEAD — modifications, staged
# changes, and deletions (`git diff HEAD` plus `--cached`). Untracked files are
# deliberately NOT swept in: the release/bump callers only ever edit tracked
# files (poms + the synced docs), and ignoring untracked paths means stray
# build output left in the tree can never leak into a release commit. The union
# is sorted and de-duplicated. A portable while-read loop (not mapfile) keeps
# this working on bash 3.x too.
CHANGED_LIST="$(
  {
    git diff --name-only HEAD
    git diff --name-only --cached
  } | sort -u
)"

if [[ -z "$CHANGED_LIST" ]]; then
  echo "gh-signed-commit: no changes to commit" >&2
  exit 1
fi

# Build the additions/deletions arrays in one pass. A file still present in the
# working tree is an addition carrying its base64 contents (base64 -w0 keeps
# each blob on one line); a path that has vanished is a deletion.
ADDITIONS='[]'
DELETIONS='[]'
while IFS= read -r path; do
  [[ -n "$path" ]] || continue
  if [[ -f "$path" ]]; then
    b64="$(base64 -w0 < "$path")"
    ADDITIONS="$(jq -c --arg p "$path" --arg c "$b64" \
      '. + [{path: $p, contents: $c}]' <<<"$ADDITIONS")"
  else
    DELETIONS="$(jq -c --arg p "$path" '. + [{path: $p}]' <<<"$DELETIONS")"
  fi
done <<<"$CHANGED_LIST"

BODY="$(cat "$BODY_FILE")"

MUTATION='mutation($input: CreateCommitOnBranchInput!) {
  createCommitOnBranch(input: $input) {
    commit { oid }
  }
}'

# Assemble the COMPLETE GraphQL request body ({query, variables}) with jq so
# nothing is shell-interpolated into the payload. `gh api graphql --input -`
# reads the whole body from stdin, so query and variables both ride along
# rather than being passed as flat field flags (which can't express a nested
# input object). The message is {headline, body}; an empty body is allowed.
REQUEST="$(jq -n \
  --arg query "$MUTATION" \
  --arg repo "$REPO" \
  --arg branch "$BRANCH" \
  --arg oid "$EXPECTED_HEAD_OID" \
  --arg headline "$HEADLINE" \
  --arg body "$BODY" \
  --argjson additions "$ADDITIONS" \
  --argjson deletions "$DELETIONS" \
  '{
    query: $query,
    variables: {
      input: {
        branch: { repositoryNameWithOwner: $repo, branchName: $branch },
        expectedHeadOid: $oid,
        message: { headline: $headline, body: $body },
        fileChanges: { additions: $additions, deletions: $deletions }
      }
    }
  }')"

# Capture the new commit oid for the caller.
NEW_OID="$(printf '%s' "$REQUEST" \
  | gh api graphql --input - \
  | jq -r '.data.createCommitOnBranch.commit.oid')"

if [[ -z "$NEW_OID" || "$NEW_OID" == "null" ]]; then
  echo "gh-signed-commit: createCommitOnBranch returned no commit oid" >&2
  exit 1
fi

printf '%s\n' "$NEW_OID"
