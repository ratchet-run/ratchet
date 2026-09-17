#!/usr/bin/env bash
# Build the reactor before aggregating Javadoc, without a nested compile lifecycle.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Separate invocations ensure every module has compiled before the root report
# runs. aggregate-no-fork is not a Maven aggregator goal for task ordering.
mvn -B -ntp clean compile
# Repeat the compile phase in this session so Maven can resolve sibling modules
# from target/classes when visiting child projects, even without installed JARs.
# Do not clean here: the root report needs the complete reactor's compiled output.
mvn -B -ntp compile javadoc:aggregate-no-fork@aggregate-javadoc -P javadoc-aggregate

# Verify the public API and SPI pages as well as the landing page before copying
# this directory into the website. A successful process alone is insufficient.
for page in index.html run/ratchet/api/JobSchedulerService.html run/ratchet/store/spi/JobStore.html; do
  if [[ ! -s "target/reports/apidocs/$page" ]]; then
    echo "Missing generated Javadoc: $page" >&2
    exit 1
  fi
done
