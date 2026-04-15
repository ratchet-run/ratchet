---
title: Configuration
---

# Configuration

Tuning Ratchet for your deployment.

This page is the deployment-focused companion to the more exhaustive [Getting Started configuration guide](/docs/getting-started/configuration). The short version:

- Operational settings come from environment variables first, then system properties.
- SPI customizations come from CDI `@Alternative` beans.
- The defaults are intentionally conservative and security-biased.

## Runtime Settings

`RatchetConfiguration` reads environment variables with the `RATCHET_` prefix and falls back to matching `-D` system properties. Examples:

```bash
RATCHET_POLLER_BATCH_SIZE=100
RATCHET_POLLER_MIN_DELAY_MS=500
RATCHET_THREAD_POOL_SIZE_SINGLE=32
RATCHET_WORKER_USE_VIRTUAL_THREADS=true
RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS=10
```

### Common Deployment Knobs

| Variable | Default | Purpose |
|----------|---------|---------|
| `RATCHET_POLLER_BATCH_SIZE` | `50` | Jobs claimed per poll cycle |
| `RATCHET_POLLER_MIN_DELAY_MS` | `2000` | Minimum poll interval |
| `RATCHET_POLLER_MAX_DELAY_MS` | `10000` | Maximum adaptive poll interval |
| `RATCHET_THREAD_POOL_SIZE_SINGLE` | `20` | Worker threads for one-off jobs |
| `RATCHET_THREAD_POOL_SIZE_BATCH_CHILD` | `30` | Worker threads for batch children |
| `RATCHET_WORKER_USE_VIRTUAL_THREADS` | `false` | Switch to Java 21 virtual threads |
| `RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS` | `10` | Node heartbeat interval |
| `RATCHET_NODE_ORPHAN_GRACE_SECONDS` | `60` | Grace period before reclaiming orphaned work |
| `RATCHET_JOB_RETENTION_DAYS` | `90` | Completed-job retention before archiving |
| `RATCHET_LOG_RETENTION_DAYS` | `30` | Per-job log retention |

For the full matrix of poller, thread-pool, retention, DLQ, archiving, and circuit-breaker settings, see [Getting Started configuration](/docs/getting-started/configuration).

## SPI Overrides

Security and extension-point behavior are not configured with class names in properties. They are CDI beans.

### Payload and Result Customization

Default payload resolution is handled by `JobInvocationResolver`; default result persistence is handled by `ResultPersistenceStrategy`.

This controls how Ratchet turns submitted callbacks into persisted target-method metadata and how completed job return values are stored. The default RI uses ASM callback analysis and JSON result metadata.

### ClassPolicy

Default: `PackagePrefixClassPolicy` with an empty allowlist

This is an intentional security boundary. By default, `RatchetProducer` refuses to start until you provide an `@Alternative @Priority(APPLICATION)` `ClassPolicy` bean naming the application package prefixes that may be invoked.

### ErrorSanitizer

Default: `DefaultErrorSanitizer`

The default sanitizer redacts common credential patterns, JDBC URLs, and email-like data before errors are persisted.

### MetricsCollector

Default: `NoOpMetricsCollector`

If you want metrics, add `ratchet-micrometer` or provide your own CDI alternative.

## Example: Production Environment

```bash
RATCHET_POLLER_BATCH_SIZE=100
RATCHET_POLLER_MIN_DELAY_MS=500
RATCHET_THREAD_POOL_SIZE_SINGLE=32
RATCHET_THREAD_POOL_SIZE_BATCH_CHILD=64
RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS=10
RATCHET_NODE_ORPHAN_GRACE_SECONDS=90
RATCHET_JOB_RETENTION_DAYS=30
RATCHET_LOG_RETENTION_DAYS=14
```

## See Also

- [Getting Started configuration](/docs/getting-started/configuration)
- [Installation](/docs/deployment/installation)
- [Troubleshooting](/docs/troubleshooting/common-issues)
