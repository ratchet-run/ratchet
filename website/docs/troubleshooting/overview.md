---
sidebar_position: 1
title: Troubleshooting Overview
description: How to diagnose issues with Ratchet, configure logging, and use the event system for debugging.
---

# Troubleshooting Overview

When something goes wrong with your Ratchet jobs, there are several layers of observability available to help you pinpoint the issue. This guide covers the diagnostic tools at your disposal and how to use them effectively.

## Diagnostic Approach

Follow this general strategy when troubleshooting Ratchet issues:

1. **Check the job status in the database** -- most issues are visible in the `scheduler_job` table
2. **Review the logs** -- Ratchet uses `java.util.logging` (JUL) with detailed lifecycle messages
3. **Listen to events** -- the event system provides real-time visibility into job state transitions
4. **Inspect execution history** -- the `scheduler_job_execution` table records every attempt

### Quick Health Check

Run this query to get a snapshot of your scheduler's current state:

```sql
SELECT status, COUNT(*) as count
FROM scheduler_job
GROUP BY status
ORDER BY count DESC;
```

A healthy system typically shows mostly `SUCCEEDED` jobs with a small number of `PENDING` and `RUNNING` jobs. Red flags include:

- **Many RUNNING jobs with old `picked_at` timestamps** -- jobs may be stuck (orphaned)
- **Growing PENDING count** -- the poller may not be running or the thread pool is exhausted
- **Many FAILED jobs** -- check `last_error` for patterns

## Event System for Debugging

Ratchet fires events at every major lifecycle transition. You can observe these through two mechanisms: programmatic listeners and CDI observers.

### Programmatic Event Listeners

Register a listener via the `JobSchedulerService` API to receive all events in a single callback:

```java
@Inject
JobSchedulerService scheduler;

public void enableDiagnostics() {
    scheduler.addEventListener(event -> {
        if (event instanceof JobFailedEvent failed) {
            log.error("Job {} failed: {}", failed.getJobId(), failed.getErrorMessage());
        } else if (event instanceof JobRetryingEvent retrying) {
            log.warn("Job {} retrying (attempt {}), next at: {}",
                retrying.getJobId(), retrying.getAttempt(), retrying.getNextScheduledTime());
        } else if (event instanceof JobDlqEvent dlq) {
            log.error("Job {} moved to DLQ after {} attempts: {}",
                dlq.getJobId(), dlq.getAttempt(), dlq.getErrorMessage());
        }
    });
}
```

### CDI Event Observers

For type-safe event observation in a CDI environment, use `@Observes`:

```java
@ApplicationScoped
public class JobDiagnosticObserver {

    private static final Logger log = Logger.getLogger(JobDiagnosticObserver.class.getName());

    public void onJobStarted(@Observes JobStartedEvent event) {
        log.info("Job " + event.getJobId() + " started on node " + event.getNodeId());
    }

    public void onJobCompleted(@Observes JobCompletedEvent event) {
        log.info("Job " + event.getJobId() + " completed in " + event.getExecutionTimeMs() + " ms");
    }

    public void onJobFailed(@Observes JobDlqEvent event) {
        log.severe("Job " + event.getJobId() + " sent to DLQ: " + event.getErrorMessage());
    }
}
```

### Available Event Types

| Event | When Fired |
|---|---|
| `JobStartedEvent` | Job begins execution on a worker thread |
| `JobCompletedEvent` | Job finishes successfully |
| `JobRetryingEvent` | Job failed but will be retried (includes next scheduled time) |
| `JobDlqEvent` | Job exhausted retries and moved to dead letter queue |
| `JobCancellingEvent` | Cancel request received for a job |
| `JobCancelledEvent` | Job successfully canceled |
| `JobPausedEvent` | Job paused via `pauseJob()` |
| `JobResumedEvent` | Job resumed via `resumeJob()` |
| `BatchCompletingEvent` | Last child of a batch completed |
| `BatchCompletedEvent` | Batch fully finalized |
| `ChainStartedEvent` | First step of a chain begins |
| `ChainCompletedEvent` | All chain steps completed successfully |
| `ChainFailedEvent` | A chain step failed permanently |
| `WorkflowBranchTriggeredEvent` | A conditional workflow branch was activated |
| `PerformanceMetricsEvent` | Periodic metrics snapshot |

## Logging Configuration

Ratchet uses `java.util.logging` (JUL) under the package `run.ratchet`. Most Jakarta EE runtimes bridge JUL to their logging subsystem.

### WildFly / JBoss EAP

Add a logger category in your `standalone.xml`:

```xml
<subsystem xmlns="urn:jboss:domain:logging:8.0">
    <logger category="run.ratchet">
        <level name="DEBUG"/>
    </logger>
    <!-- For detailed poller and thread pool diagnostics -->
    <logger category="run.ratchet.ri.core.Poller">
        <level name="FINE"/>
    </logger>
    <logger category="run.ratchet.ri.core.ThreadPoolManager">
        <level name="FINE"/>
    </logger>
</subsystem>
```

### Payara / GlassFish

Use the `asadmin` CLI:

```bash
asadmin set-log-levels run.ratchet=FINE
```

### Open Liberty

Add to `server.xml`:

```xml
<logging traceSpecification="run.ratchet.*=fine"/>
```

### Key Logger Categories

| Logger | What It Logs |
|---|---|
| `run.ratchet.ri.core.JobTask` | Job execution lifecycle, payload resolution, retry decisions |
| `run.ratchet.ri.core.Poller` | Poll cycle results, claim counts, adaptive delay changes |
| `run.ratchet.ri.core.OrphanRecoveryTimer` | Orphan detection and recovery actions |
| `run.ratchet.ri.core.JobTimeoutHandler` | Soft and hard timeout warnings |
| `run.ratchet.ri.resilience.CircuitBreaker` | Circuit breaker state transitions |
| `run.ratchet.ri.security.JobSecurityValidator` | Security validation results and rejections |
| `run.ratchet.ri.security.PackagePrefixClassPolicy` | Class policy allow/deny decisions |

### MDC Context

Ratchet automatically sets MDC (Mapped Diagnostic Context) values during job execution:

- `jobId` -- the job's unique identifier
- `nodeId` -- the cluster node executing the job
- `jobCreator` -- the user who created the job (if set)

These MDC values are available in your log format patterns for correlation:

```
# Example log4j2 pattern
%d{ISO8601} [%X{jobId}] [%X{nodeId}] %-5p %c - %m%n
```

## Configuration Reference

Ratchet reads configuration from environment variables (checked first) and system properties (checked second). All variables support both `RATCHET_*` and legacy `SCHEDULER_*` prefixes.

Key diagnostic-related settings:

| Variable | Default | Purpose |
|---|---|---|
| `RATCHET_POLLER_MIN_DELAY_MS` | `2000` | Minimum time between poll cycles |
| `RATCHET_POLLER_MAX_DELAY_MS` | `10000` | Maximum time between poll cycles (idle) |
| `RATCHET_POLLER_BATCH_SIZE` | `50` | Jobs claimed per poll cycle |
| `RATCHET_NODE_ORPHAN_GRACE_SECONDS` | `60` | Time before a stale node's jobs are recovered |
| `RATCHET_ORPHAN_SCAN_INTERVAL_MINUTES` | `5` | How often to scan for orphaned jobs |
| `RATCHET_SOFT_TIMEOUT_PERCENT` | `80` | Percentage of timeout at which warning fires |
| `RATCHET_WORKER_DEFAULT_SLA` | `1800` | Default job timeout in seconds (30 min) |
| `RATCHET_CIRCUIT_BREAKER_ENABLED` | `true` | Enable/disable the built-in circuit breaker |

## Getting Help

If you cannot resolve an issue using these guides:

1. **Search existing issues** on the [Ratchet GitHub repository](https://github.com/jcputney/ratchet/issues)
2. **Open a new issue** with:
   - Ratchet version and Jakarta EE runtime (WildFly, Payara, etc.)
   - Database vendor and version
   - Relevant log output (with `run.ratchet` set to `FINE`)
   - The SQL output of `SELECT status, COUNT(*) FROM scheduler_job GROUP BY status`
   - Steps to reproduce the issue
3. **Check the [Common Issues](./common-issues) page** for known problems and their solutions
