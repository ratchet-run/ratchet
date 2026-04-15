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

The chaos monkey uses the Docker socket to randomly stop and later restart containers whose Compose
service is `ratchet-node`.

```bash
CHAOS_INTERVAL_SECONDS=20 CHAOS_DOWN_SECONDS_MIN=5 CHAOS_DOWN_SECONDS_MAX=15 \
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

Prometheus scrapes all `ratchet-node` containers using Docker service discovery. Grafana provisions
the `Ratchet Load Test` dashboard automatically.

## Tuning

The main knobs are exposed as environment variables before running Compose:

```bash
RATCHET_THREAD_POOL_SIZE_SINGLE=64 \
RATCHET_POLLER_BATCH_SIZE=64 \
RATCHET_POLLER_MIN_DELAY_MS=100 \
DB_MAX_POOL_SIZE=100 \
POSTGRES_MAX_CONNECTIONS=1200 \
sh infra/loadtest/run.sh postgresql 10
```

For better node balance, keep `RATCHET_POLLER_BATCH_SIZE` near the per-node executor capacity for
the dominant job type. Very large batches let whichever node polls first reserve too much work.

For PostgreSQL, set `POSTGRES_MAX_CONNECTIONS` high enough for the cluster size and datasource
pools. A practical starting point is `nodes * DB_MAX_POOL_SIZE + 100`.

For MySQL, the Compose overlay sets `READ-COMMITTED` transaction isolation and raises
`max-connections` to support larger node counts.
