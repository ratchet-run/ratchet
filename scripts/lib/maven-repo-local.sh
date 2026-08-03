#!/usr/bin/env bash
# Shared -Dmaven.repo.local=... argument parsing for Spring Boot verification scripts.
#
# Sourced, not executed. Requires `set -euo pipefail` and a `fail()` function to already be
# defined by the caller.

# Parses "$@" for a single -Dmaven.repo.local=... argument, cross-checks it against the
# MAVEN_REPO_LOCAL env var, and refuses the shared ${HOME}/.m2/repository. Sets
# MAVEN_REPO_ARGUMENT to the resolved path (or leaves it empty if neither was provided).
resolve_maven_repo_local() {
  MAVEN_REPO_ARGUMENT=""
  local argument candidate
  for argument in "$@"; do
    case "$argument" in
      -Dmaven.repo.local=*)
        candidate="${argument#-Dmaven.repo.local=}"
        [[ -n "$candidate" ]] || fail "maven.repo.local must not be empty"
        [[ "$candidate" == /* ]] \
          || fail "maven.repo.local must be an absolute path: $candidate"
        if [[ -n "$MAVEN_REPO_ARGUMENT" && "$MAVEN_REPO_ARGUMENT" != "$candidate" ]]; then
          fail "conflicting maven.repo.local arguments"
        fi
        MAVEN_REPO_ARGUMENT="$candidate"
        ;;
      *)
        fail "unexpected argument: $argument"
        ;;
    esac
  done

  if [[ -n "${MAVEN_REPO_LOCAL:-}" ]]; then
    [[ "$MAVEN_REPO_LOCAL" == /* ]] \
      || fail "MAVEN_REPO_LOCAL must be an absolute path: $MAVEN_REPO_LOCAL"
    if [[ -n "$MAVEN_REPO_ARGUMENT" && "$MAVEN_REPO_ARGUMENT" != "$MAVEN_REPO_LOCAL" ]]; then
      fail "MAVEN_REPO_LOCAL does not match the maven.repo.local argument"
    fi
    MAVEN_REPO_ARGUMENT="$MAVEN_REPO_LOCAL"
  fi

  if [[ -n "$MAVEN_REPO_ARGUMENT" \
        && -n "${HOME:-}" \
        && "$MAVEN_REPO_ARGUMENT" == "${HOME}/.m2/repository" ]]; then
    fail "refusing to use the shared Maven repository: $MAVEN_REPO_ARGUMENT"
  fi
}
