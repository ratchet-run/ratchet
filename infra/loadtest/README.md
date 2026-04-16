# Ratchet Docker Compose Load Testing

This stack runs a scalable Ratchet cluster for throughput and resiliency testing. It supports
PostgreSQL, MySQL, and MongoDB backends, Prometheus/Grafana observability, and an optional chaos
monkey that stops and restarts Ratchet node containers.

## Start a Cluster

Run from the repository root:

```bash
sh infra/loadtest/run.sh postgresql 5
```

Equivalent direct Compose command:

```bash
cd infra/loadtest
docker compose -f compose.yml -f compose.postgresql.yml up --build --scale ratchet-node=5
```

Use `mysql` or `mongodb` as the first argument to switch stores:

```bash
sh infra/loadtest/run.sh mysql 5
sh infra/loadtest/run.sh mongodb 5
```

The gateway is exposed at `http://localhost:8080`, Prometheus at `http://localhost:9090`, and
Grafana at `http://localhost:3000` with `admin` / `ratchet` by default.

## Enable Chaos

The chaos monkey uses the Docker socket to randomly disrupt containers whose Compose service is
`ratchet-node`. It can vary the attack interval, target count per cycle, and disruption mode
(`stop`, `kill`, `pause`, `restart`) while preserving a minimum number of running nodes.

```bash
CHAOS_INTERVAL_SECONDS_MIN=10 CHAOS_INTERVAL_SECONDS_MAX=35 \
CHAOS_TARGETS_PER_CYCLE_MIN=1 CHAOS_TARGETS_PER_CYCLE_MAX=3 \
CHAOS_ACTIONS=stop,kill,pause,restart \
CHAOS_DOWN_SECONDS_MIN=5 CHAOS_DOWN_SECONDS_MAX=15 \
  sh infra/loadtest/run.sh postgresql 5 chaos
```

Direct Compose form:

```bash
cd infra/loadtest
docker compose -f compose.yml -f compose.postgresql.yml --profile chaos \
  up --build --scale ratchet-node=5
```

Do not run the chaos profile against production Docker hosts. The service can control containers via
`/var/run/docker.sock`.

## Submit Workloads

Start a run through the gateway:

```bash
curl -sS -X POST http://localhost:8080/api/runs \
  -H 'Content-Type: application/json' \
  -d '{"workload":"sleep","jobs":10000,"sleepMs":5,"sleepJitterMs":25,"maxRetries":0}' | jq
```

Or use the helper:

```bash
sh infra/loadtest/submit-run.sh sleep 10000 5 0.0 25 0.02 250
```

This mode sends one API request to whichever node receives `/api/runs`; that node enqueues the
whole run. It is useful for scheduler throughput and execution safety testing, but it intentionally
does not exercise multi-node HTTP writes into the queue.

## Continuous HTTP Enqueue Load

Use the `k6` profile when the test needs each job enqueue to enter through the gateway as its own
HTTP request. This exercises client-facing enqueue latency, nginx balancing, and concurrent writes
from all Ratchet nodes into the shared queue.

Start the cluster first:

```bash
sh infra/loadtest/run.sh postgresql 10
```

In another terminal, run a fixed-rate enqueue load. The second argument is the expected number of
Ratchet nodes; the k6 run fails if the gateway probe or the actual enqueued job metadata sees fewer
enqueue writer nodes than this value.

```bash
LOAD_RATE=500 LOAD_DURATION=5m JOB_WORKLOAD=sleep JOB_SLEEP_MS=10 JOB_SLEEP_JITTER_MS=50 \
  sh infra/loadtest/run-k6-enqueue.sh postgresql 10
```

`LOAD_MODE=spread` is the default and forces short-lived client connections so node distribution is
easy to prove. Use `LOAD_MODE=throughput` for sustained capacity testing; this keeps client
connections alive and uses nginx upstream keepalive while still validating that accepted jobs reached
the expected number of nodes.

```bash
LOAD_MODE=throughput LOAD_RATE=500 LOAD_DURATION=10m \
  sh infra/loadtest/run-k6-enqueue.sh postgresql 10
```

Useful k6 environment variables:

| Variable | Default | Meaning |
| --- | ---: | --- |
| `LOAD_MODE` | `spread` | `spread` validates distribution with short-lived connections; `throughput` uses keep-alive |
| `LOAD_RATE` | `100` | Constant enqueue request arrival rate per second |
| `LOAD_DURATION` | `1m` | Duration for the enqueue scenario |
| `MIN_ACCEPT_NODES` | script argument | Minimum nodes that must accept enqueue writes |
| `NODE_PROBE_REQUESTS` | `200` | Gateway probe requests before load starts |
| `RUN_ID` | generated | Run ID attached to every enqueued job |
| `LOAD_TEARDOWN_TIMEOUT` | `3m` | k6 teardown timeout for large backlog summary calls |
| `LOAD_ENQUEUE_RETRIES` | `3` | Retry count for transient enqueue HTTP failures |
| `LOAD_ENQUEUE_RETRY_DELAY_MS` | `100` | Initial retry delay for transient enqueue failures |
| `LOAD_ENQUEUE_RETRY_BACKOFF` | `2.0` | Retry delay multiplier |
| `JOB_WORKLOAD` | `noop` | Job workload submitted by each HTTP request |
| `JOB_SLEEP_MS` | `0` | Base per-job sleep time |
| `JOB_SLEEP_JITTER_MS` | `0` | Deterministic per-job extra sleep |
| `JOB_SLEEP_SPIKE_RATE` | `0.0` | Fraction of jobs that get a long-tail sleep spike |
| `JOB_SLEEP_SPIKE_MS` | `0` | Additional sleep for spike jobs |
| `JOB_FAILURE_RATE` | `0.0` | Deterministic injected failure rate |
| `JOB_PAYLOAD_BYTES` | `0` | Bytes added to each job payload argument |

Each `POST /api/jobs` response includes `acceptedNodeId` and the `X-Ratchet-Node-Id` header. Run
status includes `enqueueNodeCounts` for nodes that accepted queue writes and `executionNodeCounts`
for nodes that claimed/executed jobs.

Supported workloads:

| Workload | Behavior |
| --- | --- |
| `noop` | Minimal no-op job body |
| `sleep` | Sleeps for `sleepMs` per job |
| `probabilistic-failure` | Fails each job deterministically by `failureRate`, then sleeps |
| `mixed` | Rotates through no-op, sleep, and probabilistic failure jobs |

Useful request fields:

| Field | Default | Meaning |
| --- | ---: | --- |
| `jobs` | `1000` | Number of jobs to enqueue |
| `sleepMs` | `5` | Base per-job sleep time for sleep/failure workloads |
| `sleepJitterMs` | `0` | Deterministic per-job extra sleep in the range `0..sleepJitterMs` |
| `sleepSpikeRate` | `0.0` | Fraction of jobs that get an additional long-tail sleep spike |
| `sleepSpikeMs` | `0` | Additional sleep added when a spike is selected |
| `failureRate` | `0.0` | Value from `0.0` to `1.0` |
| `payloadBytes` | `0` | Bytes added to each job payload argument |
| `maxRetries` | `0` | Ratchet retry count for injected failures |
| `priority` | `NORMAL` | Ratchet job priority |
| `timeoutSeconds` | `60` | Per-job timeout |

## Observe Runs

Check cluster status:

```bash
curl -sS http://localhost:8080/api/cluster | jq
```

Check a run:

```bash
curl -sS http://localhost:8080/api/runs/<run-id> | jq
```

Reset known load-test runs on the node that receives the request:

```bash
curl -sS -X POST http://localhost:8080/api/reset -H 'Content-Type: application/json' -d '{}'
```

Reset a specific run by ID:

```bash
curl -sS -X POST http://localhost:8080/api/reset \
  -H 'Content-Type: application/json' \
  -d '{"runId":"<run-id>"}'
```

Prometheus scrapes all `ratchet-node` containers using Docker service discovery. The PostgreSQL
overlay also runs `postgres-exporter` and scrapes connection, wait, and lock metrics. Grafana
provisions the `Ratchet Load Test` dashboard automatically, including a `DB Pressure` panel for
active database connections, waiting backends, waiting locks, and pending queue depth.

## Tuning

The main knobs are exposed as environment variables before running Compose:

```bash
RATCHET_THREAD_POOL_SIZE_SINGLE=64 \
RATCHET_POLLER_BATCH_SIZE=64 \
RATCHET_POLLER_MIN_DELAY_MS=100 \
DB_MAX_POOL_SIZE=50 \
POSTGRES_MAX_CONNECTIONS=800 \
sh infra/loadtest/run.sh postgresql 10
```

For better node balance, keep `RATCHET_POLLER_BATCH_SIZE` near the per-node executor capacity for
the dominant job type. Very large batches let whichever node polls first reserve too much work.

For PostgreSQL, tune `POSTGRES_MAX_CONNECTIONS` and `DB_MAX_POOL_SIZE` together. The default
`DB_MAX_POOL_SIZE=50` and `POSTGRES_MAX_CONNECTIONS=800` support a 10-node load-test cluster while
leaving headroom for the exporter, manual sessions, and restart churn. A practical upper bound is
`nodes * DB_MAX_POOL_SIZE + 100`, but raising connection counts too far can make Postgres spend more
time scheduling connections than draining queue work.

The PostgreSQL overlay sets `POSTGRES_SHM_SIZE=1gb` by default via Compose `shm_size`; raise it for
large runs if status or analytics queries report dynamic shared-memory allocation errors.

For MySQL, the Compose overlay sets `READ-COMMITTED` transaction isolation and raises
`max-connections` to support larger node counts.
