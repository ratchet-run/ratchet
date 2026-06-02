# Load-Test Profiling — Pool-vs-Lock Triage

Before profiling the scheduler code in depth, answer one question: is the throughput ceiling
**connection-pool starvation** (WildFly app threads blocked in `getConnection()`) or **MySQL
hot-table contention** (lock/index churn on enqueue + claim + complete)? They look identical from
the outside — `iter/s` plateaus, `p95` climbs — but the fixes are opposite (raise pool size vs.
change the data model). This pass settles it with cheap, always-on instrumentation plus one JFR
recording.

## What was added

- **`DataSourcePoolMetricsBinder`** (in `ratchet-loadtest`) publishes the WildFly IronJacamar pool
  statistics for `RatchetDS` as `ratchet_ds_pool_*` gauges on the existing `:8080/metrics` scrape —
  the client side of the pool that nothing previously measured.
- **`statistics-enabled=true`** on the datasource in `wildfly/mysql.cli` and `wildfly/postgresql.cli`
  (the pool MBean reports zeros without it). These are cheap counters, so the headline throughput
  number stays comparable to the prior `~1570 iter/s` baseline — no separate "clean" build needed.
- **`mysql-capture.sh`** — resets `performance_schema`, samples live lock state during the run, then
  snapshots the top statement digests and wait events (server side).
- **`jfr-capture.sh`** — a single-node JDK Flight Recording (JVM side, low overhead).
- **`prom-dump.sh`** — pulls the decisive pool + MySQL series out of Prometheus as JSON.

## Where this runs

On the load-test host (`deep-thought`) over Tailscale/SSH. Workstation tunnels are typically:
gateway `localhost:18083`, Prometheus `localhost:19093`, Grafana `localhost:13003`. The capture
scripts talk to the local Docker socket, so run them **on deep-thought**, not through the tunnel.

## Run the triage

From the repo root on deep-thought:

```sh
# 1. Bring up the cluster (this --builds, so it picks up the new CLI + binder).
sh infra/loadtest/run.sh mysql 10

# 2. Warm up: 60s at target rate, discarded. Buffer-pool/JIT warmth is a known source of drift —
#    do this identically before every comparison run.
LOAD_RATE=2000 LOAD_DURATION=1m JOB_WORKLOAD=noop \
  sh infra/loadtest/run-k6-enqueue.sh mysql 10

# 3. Measured run. In three terminals, start the captures, then the load, so all three windows
#    overlap. RUN_ID ties the artifacts together.
RUN_ID=triage-$(date +%s)
DURATION=130 RUN_ID=$RUN_ID sh infra/loadtest/profiling/mysql-capture.sh   # terminal A
DURATION=130 RUN_ID=$RUN_ID sh infra/loadtest/profiling/jfr-capture.sh     # terminal B
LOAD_RATE=2000 LOAD_DURATION=2m JOB_WORKLOAD=noop \
  sh infra/loadtest/run-k6-enqueue.sh mysql 10                            # terminal C

# 4. After the run, pull the Prometheus series for the same window.
RUN_ID=$RUN_ID DURATION=140 sh infra/loadtest/profiling/prom-dump.sh
```

All artifacts land under `infra/loadtest/profiling-out/mysql-<run-id>/`.

## Reading the verdict

| Signal | Pool starvation | MySQL contention |
| --- | --- | --- |
| `ratchet_ds_pool_in_use_count` vs `max_used_count` | pinned at max pool (20) | below max |
| `ratchet_ds_pool_wait_count` / `average_blocking_time` | rising / non-zero | ~0 |
| `ratchet_ds_pool_blocking_failure_count` / `timed_out` | non-zero | ~0 |
| MySQL `threads_running` | low (≈ pool size × nodes) | high |
| `innodb_row_lock_current_waits`, `row_lock_time` rate | ~0 | rising |
| `post-statement-digests.tsv` `avg_ms` on claim/enqueue/update | low | high |
| `post-statement-digests.tsv` `lock_s` | low | significant |
| JFR hot parked frames | IronJacamar pool semaphore | MySQL driver `SocketRead` / lock |

Pool starvation → raise `DB_MAX_POOL_SIZE` (and MySQL `max-connections`) and re-measure. MySQL
contention → the bottleneck is in the schema/claim path, and the deeper JFR/`perf_schema` profiling
pass is warranted.

## What to hand back for analysis

The whole `profiling-out/mysql-<run-id>/` directory plus the k6 summary line (`iter/s`, `p95`,
teardown SUCCEEDED/PENDING/RUNNING). The `.jfr` is analyzed offline with `jfr summary` and
`jfr print --events jdk.ThreadPark,jdk.JavaMonitorEnter,jdk.SocketRead`.
