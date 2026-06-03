#!/bin/sh
# Pool-vs-lock triage: JVM-side corroboration via JDK Flight Recorder on ONE node.
#
# JFR is in-JVM and low overhead, so it runs on a single live node without contaminating the
# cluster-wide throughput number. The 'profile' configuration captures method sampling, lock
# events (jdk.JavaMonitorEnter / jdk.ThreadPark), allocation, and socket I/O. For the triage the
# signal is which the hot threads are parked on: a IronJacamar pool semaphore (pool wait) versus
# a MySQL driver socket read (database latency).
#
# Run on the load-test host with the cluster already up, alongside mysql-capture.sh + the k6 run:
#   DURATION=120 sh infra/loadtest/profiling/jfr-capture.sh
#
# Analyze the pulled recording offline:
#   jfr summary triage-*.jfr
#   jfr print --events jdk.ThreadPark,jdk.JavaMonitorEnter,jdk.SocketRead triage-*.jfr
set -eu

STORE="${STORE:-mysql}"
DURATION="${DURATION:-120}"
RUN_ID="${RUN_ID:-$(date +%s)}"

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

OUT="${OUT:-profiling-out/${STORE}-${RUN_ID}}"
mkdir -p "$OUT"

COMPOSE="docker compose -f compose.yml -f compose.${STORE}.yml"

# Pick a single node container from the scaled service.
CID="$($COMPOSE ps -q ratchet-node | head -1)"
if [ -z "$CID" ]; then
  echo "[jfr] no running ratchet-node container found; is the cluster up?" >&2
  exit 1
fi

# The WildFly JVM runs jboss-modules; resolve its PID inside the container. The image has no
# pgrep, so use the JVM-native listing (jcmd -l), which is more reliable anyway.
JVM_PID="$(docker exec "$CID" sh -c 'jcmd -l 2>/dev/null | grep -i jboss-modules | head -1 | cut -d" " -f1')"
if [ -z "$JVM_PID" ]; then
  echo "[jfr] could not find the WildFly JVM pid in $CID" >&2
  exit 1
fi

REMOTE_FILE="/tmp/triage-${RUN_ID}.jfr"
echo "[jfr] node=$CID pid=$JVM_PID duration=${DURATION}s -> $REMOTE_FILE"

docker exec "$CID" jcmd "$JVM_PID" \
  JFR.start name="triage-${RUN_ID}" settings=profile duration="${DURATION}s" filename="$REMOTE_FILE"

# JFR writes the file when the timed recording ends; wait it out plus a small margin.
sleep "$((DURATION + 10))"

docker cp "$CID:$REMOTE_FILE" "$OUT/triage-${RUN_ID}.jfr"
echo "[jfr] done -> $OUT/triage-${RUN_ID}.jfr"
