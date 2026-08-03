---
title: Configuration reference
description: Complete property and environment-variable reference generated from Ratchet's typed configuration catalog
---

# Configuration reference

This page lists every fixed key read by `RatchetOptionsFactory.fromEnvironment()`. The factory checks caller-supplied `RatchetConfigSource` instances first, then MicroProfile Config, then environment variables, and finally the compiled default shown below. If you build `RatchetOptions` programmatically, these names are not read.

The table is checked against `RatchetConfigKeys` during the website build. Adding, removing, or renaming a fixed key fails the documentation check until this reference is regenerated with `npm run docs:sync-config-reference`.

This build exposes **59 fixed keys**.

<!-- CONFIG_REFERENCE_START -->

| Property | Environment variable | Default | Catalog key |
|---|---|---|---|
| `ratchet.poller.batch-size` | `RATCHET_POLLER_BATCH_SIZE` | `50` | `POLLER_BATCH_SIZE` |
| `ratchet.poller.burst-delay-ms` | `RATCHET_POLLER_BURST_DELAY_MS` | `500` | `POLLER_BURST_DELAY_MS` |
| `ratchet.poller.min-delay-ms` | `RATCHET_POLLER_MIN_DELAY_MS` | `2000` | `POLLER_MIN_DELAY_MS` |
| `ratchet.poller.max-delay-ms` | `RATCHET_POLLER_MAX_DELAY_MS` | `10000` | `POLLER_MAX_DELAY_MS` |
| `ratchet.poller.deep-idle-delay-ms` | `RATCHET_POLLER_DEEP_IDLE_DELAY_MS` | `30000` | `POLLER_DEEP_IDLE_DELAY_MS` |
| `ratchet.poller.deep-idle-threshold-ms` | `RATCHET_POLLER_DEEP_IDLE_THRESHOLD_MS` | `60000` | `POLLER_DEEP_IDLE_THRESHOLD_MS` |
| `ratchet.poller.idle-threshold` | `RATCHET_POLLER_IDLE_THRESHOLD` | `3` | `POLLER_IDLE_THRESHOLD` |
| `ratchet.poller.claim-headroom-factor` | `RATCHET_POLLER_CLAIM_HEADROOM_FACTOR` | `0` | `POLLER_CLAIM_HEADROOM_FACTOR` |
| `ratchet.worker.default-threading-mode` | `RATCHET_WORKER_DEFAULT_THREADING_MODE` | `PLATFORM` | `WORKER_DEFAULT_THREADING_MODE` |
| `ratchet.worker.job-executor-jndi` | `RATCHET_WORKER_JOB_EXECUTOR_JNDI` | `java:comp/DefaultManagedExecutorService` | `WORKER_JOB_EXECUTOR_JNDI` |
| `ratchet.worker.scheduled-executor-jndi` | `RATCHET_WORKER_SCHEDULED_EXECUTOR_JNDI` | `java:comp/DefaultManagedScheduledExecutorService` | `WORKER_SCHEDULED_EXECUTOR_JNDI` |
| `ratchet.coordinator.thread-factory-jndi` | `RATCHET_COORDINATOR_THREAD_FACTORY_JNDI` | `java:comp/DefaultManagedThreadFactory` | `COORDINATOR_THREAD_FACTORY_JNDI` |
| `ratchet.worker.virtual-executor-jndi` | `RATCHET_WORKER_VIRTUAL_EXECUTOR_JNDI` | `(empty string)` | `WORKER_VIRTUAL_EXECUTOR_JNDI` |
| `ratchet.worker.virtual-counter-accounting` | `RATCHET_WORKER_VIRTUAL_COUNTER_ACCOUNTING` | `false` | `WORKER_VIRTUAL_COUNTER_ACCOUNTING` |
| `ratchet.thread-pool.queue-size` | `RATCHET_THREAD_POOL_QUEUE_SIZE` | `100` | `THREAD_POOL_QUEUE_SIZE` |
| `ratchet.thread-pool.size.single` | `RATCHET_THREAD_POOL_SIZE_SINGLE` | `20` | `THREAD_POOL_SIZE_SINGLE` |
| `ratchet.thread-pool.size.recurring` | `RATCHET_THREAD_POOL_SIZE_RECURRING` | `5` | `THREAD_POOL_SIZE_RECURRING` |
| `ratchet.thread-pool.size.batch-child` | `RATCHET_THREAD_POOL_SIZE_BATCH_CHILD` | `30` | `THREAD_POOL_SIZE_BATCH_CHILD` |
| `ratchet.thread-pool.size.batch-parent` | `RATCHET_THREAD_POOL_SIZE_BATCH_PARENT` | `2` | `THREAD_POOL_SIZE_BATCH_PARENT` |
| `ratchet.thread-pool.size.chain-step` | `RATCHET_THREAD_POOL_SIZE_CHAIN` | `10` | `THREAD_POOL_SIZE_CHAIN` |
| `ratchet.thread-pool.size.workflow-branch` | `RATCHET_THREAD_POOL_SIZE_WORKFLOW_BRANCH` | `10` | `THREAD_POOL_SIZE_WORKFLOW_BRANCH` |
| `ratchet.thread-pool.size.workflow-join` | `RATCHET_THREAD_POOL_SIZE_WORKFLOW_JOIN` | `10` | `THREAD_POOL_SIZE_WORKFLOW_JOIN` |
| `ratchet.node.id` | `RATCHET_NODE_ID` | `(empty string)` | `NODE_ID` |
| `ratchet.node.heartbeat-interval-seconds` | `RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS` | `10` | `NODE_HEARTBEAT_INTERVAL_SECONDS` |
| `ratchet.node.orphan-grace-seconds` | `RATCHET_NODE_ORPHAN_GRACE_SECONDS` | `60` | `NODE_ORPHAN_GRACE_SECONDS` |
| `ratchet.node.orphan-scan-interval-minutes` | `RATCHET_NODE_ORPHAN_SCAN_INTERVAL_MINUTES` | `5` | `ORPHAN_SCAN_INTERVAL_MINUTES` |
| `ratchet.node.dynamic-heartbeat-enabled` | `RATCHET_DYNAMIC_HEARTBEAT_ENABLED` | `true` | `DYNAMIC_HEARTBEAT_ENABLED` |
| `ratchet.recurring.batch-limit` | `RATCHET_RECURRING_BATCH_LIMIT` | `20` | `RECURRING_BATCH_LIMIT` |
| `ratchet.recurring.poll-ms` | `RATCHET_RECURRING_POLL_MS` | `1000` | `RECURRING_POLL_MS` |
| `ratchet.recurring.max-poll-ms` | `RATCHET_RECURRING_MAX_POLL_MS` | `60000` | `RECURRING_MAX_POLL_MS` |
| `ratchet.recurring.startup-grace-seconds` | `RATCHET_RECURRING_STARTUP_GRACE_SECONDS` | `60` | `RECURRING_STARTUP_GRACE_SECONDS` |
| `ratchet.recurring.convergence-window-seconds` | `RATCHET_RECURRING_CONVERGENCE_WINDOW_SECONDS` | `0` | `RECURRING_CONVERGENCE_WINDOW_SECONDS` |
| `ratchet.retry-buffer.drain-interval-ms` | `RATCHET_RETRY_BUFFER_DRAIN_INTERVAL_MS` | `1000` | `RETRY_BUFFER_DRAIN_INTERVAL_MS` |
| `ratchet.timeout.soft-timeout-percent` | `RATCHET_SOFT_TIMEOUT_PERCENT` | `80` | `SOFT_TIMEOUT_PERCENT` |
| `ratchet.timeout.default-sla-seconds` | `RATCHET_WORKER_DEFAULT_SLA` | `1800` | `WORKER_DEFAULT_SLA` |
| `ratchet.timeout.signal-timeout-batch-size` | `RATCHET_SIGNAL_TIMEOUT_BATCH_SIZE` | `500` | `SIGNAL_TIMEOUT_BATCH_SIZE` |
| `ratchet.dlq.purge-enabled` | `RATCHET_DLQ_PURGE_ENABLED` | `true` | `DLQ_PURGE_ENABLED` |
| `ratchet.dlq.purge-cron` | `RATCHET_DLQ_PURGE_CRON` | `0 0 2 * * ?` | `DLQ_PURGE_CRON` |
| `ratchet.dlq.purge-days` | `RATCHET_DLQ_PURGE_DAYS` | `90` | `DLQ_PURGE_DAYS` |
| `ratchet.jobs.archive-enabled` | `RATCHET_JOB_ARCHIVE_ENABLED` | `true` | `JOB_ARCHIVE_ENABLED` |
| `ratchet.jobs.archive-cron` | `RATCHET_JOB_ARCHIVE_CRON` | `0 0 1 * * ?` | `JOB_ARCHIVE_CRON` |
| `ratchet.jobs.retention-days` | `RATCHET_JOB_RETENTION_DAYS` | `90` | `JOB_RETENTION_DAYS` |
| `ratchet.jobs.archive-batch-size` | `RATCHET_JOB_ARCHIVE_BATCH_SIZE` | `1000` | `JOB_ARCHIVE_BATCH_SIZE` |
| `ratchet.logs.purge-enabled` | `RATCHET_LOG_PURGE_ENABLED` | `true` | `LOG_PURGE_ENABLED` |
| `ratchet.logs.purge-cron` | `RATCHET_LOG_PURGE_CRON` | `0 30 2 * * ?` | `LOG_PURGE_CRON` |
| `ratchet.logs.retention-days` | `RATCHET_LOG_RETENTION_DAYS` | `30` | `LOG_RETENTION_DAYS` |
| `ratchet.schema.auto-migrate` | `RATCHET_SCHEMA_AUTO_MIGRATE` | `false` | `SCHEMA_AUTO_MIGRATE` |
| `ratchet.schema.migration-dialect` | `RATCHET_SCHEMA_MIGRATION_DIALECT` | `(empty string)` | `SCHEMA_MIGRATION_DIALECT` |
| `ratchet.schema.migration-prefix` | `RATCHET_SCHEMA_MIGRATION_PREFIX` | `ddl/migrations` | `SCHEMA_MIGRATION_PREFIX` |
| `ratchet.payload.max-payload-kb` | `RATCHET_MAX_PAYLOAD_KB` | `100` | `MAX_PAYLOAD_KB` |
| `ratchet.jobs.max-result-bytes` | `RATCHET_JOB_RESULT_MAX_BYTES` | `65536` | `MAX_RESULT_BYTES` |
| `ratchet.allow-empty-class-policy` | `RATCHET_ALLOW_EMPTY_CLASS_POLICY` | `false` | `ALLOW_EMPTY_CLASS_POLICY` |
| `ratchet.class-policy.allowed-packages` | `RATCHET_CLASS_POLICY_ALLOWED_PACKAGES` | `(empty string)` | `CLASS_POLICY_ALLOWED_PACKAGES` |
| `ratchet.class-policy.allowed-result-type-packages` | `RATCHET_CLASS_POLICY_ALLOWED_RESULT_TYPE_PACKAGES` | `(empty string)` | `CLASS_POLICY_ALLOWED_RESULT_TYPE_PACKAGES` |
| `ratchet.security.redact-emails` | `RATCHET_REDACT_EMAILS` | `true` | `REDACT_EMAILS` |
| `ratchet.security.mask-payloads` | `RATCHET_MASK_PAYLOADS` | `false` | `MASK_PAYLOADS` |
| `ratchet.isolation-check` | `RATCHET_ISOLATION_CHECK_MODE` | `FAIL` | `ISOLATION_CHECK_MODE` |
| `ratchet.priority-boost-interval-minutes` | `RATCHET_PRIORITY_BOOST_INTERVAL_MINUTES` | `15` | `PRIORITY_BOOST_INTERVAL_MINUTES` |
| `ratchet.circuit-breaker.enabled` | `RATCHET_CIRCUIT_BREAKER_ENABLED` | `true` | `CIRCUIT_BREAKER_ENABLED` |

<!-- CONFIG_REFERENCE_END -->

## Dynamic key families

Three families are created from an execution type or circuit-breaker profile rather than declared as fixed catalog fields:

| Purpose | Property pattern | Environment-variable pattern |
|---|---|---|
| Virtual-thread backpressure | `ratchet.virtual-thread-limit.<execution-type>` | `RATCHET_VIRTUAL_THREAD_LIMIT_<EXECUTION_TYPE>` |
| Per-minute rate limit | `ratchet.rate-limit-per-minute.<execution-type>` | `RATCHET_RATE_LIMIT_PER_MINUTE_<EXECUTION_TYPE>` |
| Circuit-breaker profiles | `ratchet.circuit-breaker.<profile>.<setting>` | `RATCHET_CB_<PROFILE>_<SETTING>` |

Circuit-breaker settings are `failure-rate`, `window-size`, `wait-ms`, `half-open-calls`, and `minimum-calls`. Their defaults come from the selected profile rather than the fixed catalog.

## Invalid values

Values are trimmed and parsed through the typed catalog. An invalid value emits one warning and falls back to the default instead of being silently accepted. The [configuration guide](/getting-started/configuration) describes the corresponding builder options and CDI setup.
