---
sidebar_position: 3
title: Debugging Guide
description: How to debug Ratchet jobs using event listeners, database queries, CDI observers, and optional per-job logging.
---

# Debugging Guide

This guide covers practical techniques for understanding what your Ratchet jobs are doing at runtime, from event tracing to database-level inspection. `JobContext.logger()` exists on every job and is backed by JBoss Logging by default; database persistence of those log lines is optional.

## Using JobContext.logger()

Every job has access to a `JobContext` that provides a job-scoped logger. The default logger writes to the runtime logging backend and publishes `JobLogLine` events. If your application observes and persists those events, the entries can be stored in `scheduler_job_log`, giving you a structured execution trace tied to each job ID.

```java
@ApplicationScoped
public class DataImportService {

    public void importData() {
        JobContext ctx = JobContext.current();
        JobLogger log = ctx.logger();

        log.info("Starting import for job " + ctx.jobId());

        String batchSize = ctx.param("batchSize", "100");
        log.info("Using batch size: " + batchSize);

        try {
            int rows = processRecords(Integer.parseInt(batchSize));
            log.info("Imported " + rows + " rows successfully");
        } catch (Exception e) {
            log.error("Import failed: " + e.getMessage());
            throw e;
        }
    }
}
```

### Querying Job Logs

If your application persists `JobLogLine` events, you can retrieve them after a job runs:

```sql
-- Get all log entries for a specific job, ordered chronologically
SELECT ts, level, message
FROM scheduler_job_log
WHERE job_id = '01902c4e-c4f3-7b8a-9d3e-fedcba987654'
ORDER BY ts ASC;
```

```sql
-- Find jobs that logged errors in the last hour. A job may already have terminated
-- (its scheduler_job_queue row deleted), so LEFT JOIN the queue and fall back to the
-- cold scheduler_job terminal columns for status/error.
SELECT DISTINCT jl.job_id,
       COALESCE(q.status, j.terminal_status) AS status,
       COALESCE(q.last_error, j.terminal_error) AS last_error,
       jl.message
FROM scheduler_job_log jl
JOIN scheduler_job j ON j.job_id = jl.job_id
LEFT JOIN scheduler_job_queue q ON q.job_id = jl.job_id
WHERE jl.level = 'ERROR'
  AND jl.ts >= NOW() - INTERVAL '1 hour'
ORDER BY jl.ts DESC;
```

### Using Job Parameters for Debug Context

Attach parameters at submission time to carry context through to execution:

```java
scheduler.enqueue(importService::importData)
    .withParam("batchSize", "500")
    .withParam("source", "quarterly-report")
    .withParam("requestedBy", currentUser.getName())
    .submit();
```

These parameters are accessible via `JobContext.current().param("key")` and stored in the `params` column of `scheduler_job`:

```sql
-- Find jobs by parameter values. params lives on the cold scheduler_job row;
-- status is live state on scheduler_job_queue (NULL once the job terminates), so
-- LEFT JOIN and fall back to terminal_status.
SELECT j.job_id, COALESCE(q.status, j.terminal_status) AS status,
       j.params::jsonb ->> 'source' as source
FROM scheduler_job j
LEFT JOIN scheduler_job_queue q ON q.job_id = j.job_id
WHERE j.params::jsonb ->> 'source' = 'quarterly-report';
```

## Event Listeners for Tracing Job Lifecycle

For real-time debugging, register an event listener that logs all lifecycle transitions:

```java
@ApplicationScoped
public class LifecycleTracer {

    private static final Logger log = Logger.getLogger(LifecycleTracer.class.getName());

    @Inject
    JobSchedulerService scheduler;

    private Consumer<Object> listener;

    public void startTracing() {
        listener = event -> {
            switch (event) {
                case JobStartedEvent e ->
                    log.info("[TRACE] Job " + e.getJobId() + " STARTED on " + e.getNodeId()
                        + " [type=" + e.getJobType() + ", priority=" + e.getPriority() + "]");

                case JobCompletedEvent e ->
                    log.info("[TRACE] Job " + e.getJobId() + " COMPLETED in "
                        + e.getExecutionTimeMs() + " ms");

                case JobRetryingEvent e ->
                    log.warning("[TRACE] Job " + e.getJobId() + " RETRYING (attempt "
                        + e.getRetryAttempt() + "), next at " + e.getScheduledTime()
                        + ", error: " + e.getErrorMessage());

                case JobDlqEvent e ->
                    log.severe("[TRACE] Job " + e.getJobId() + " -> DLQ after "
                        + e.getRetryAttempt() + " attempts: " + e.getErrorMessage());

                case JobCancelledEvent e ->
                    log.info("[TRACE] Job " + e.getJobId() + " CANCELLED from "
                        + e.getPreviousStatus() + " after " + e.getExecutionTimeMs() + " ms");

                case BatchCompletedEvent e ->
                    log.info("[TRACE] Batch " + e.getJobId() + " COMPLETED");

                case ChainFailedEvent e ->
                    log.severe("[TRACE] Chain " + e.getJobId() + " FAILED");

                default -> log.fine("[TRACE] " + event.getClass().getSimpleName()
                    + " for job " + ((AbstractJobSchedulerEvent) event).getJobId());
            }
        };
        scheduler.addEventListener(listener);
    }

    public void stopTracing() {
        if (listener != null) {
            scheduler.removeEventListener(listener);
        }
    }
}
```

## CDI @Observes for Event Monitoring

CDI observers provide type-safe event handling. Use specific event types to narrow what you observe:

```java
@ApplicationScoped
public class SchedulerMonitor {

    private static final Logger log = Logger.getLogger(SchedulerMonitor.class.getName());

    /**
     * Track slow jobs. Fires after every successful completion.
     */
    public void onJobCompleted(@Observes JobCompletedEvent event) {
        if (event.getExecutionTimeMs() > 60_000) {
            log.warning("Slow job detected: " + event.getJobId()
                + " took " + event.getExecutionTimeMs() + " ms"
                + " [type=" + event.getJobType() + "]");
        }
    }

    /**
     * Alert on DLQ entries. Fires when a job exhausts all retries.
     */
    public void onDlq(@Observes JobDlqEvent event) {
        log.severe("DLQ ALERT: Job " + event.getJobId()
            + " failed permanently after " + event.getRetryAttempt() + " attempts"
            + " - " + event.getErrorMessage());
        // Send alert to monitoring system, Slack, etc.
    }

    /**
     * Monitor retry patterns. Fires each time a job is rescheduled for retry.
     */
    public void onRetry(@Observes JobRetryingEvent event) {
        log.info("Retry: Job " + event.getJobId()
            + " attempt " + event.getRetryAttempt()
            + " scheduled for " + event.getScheduledTime());
    }
}
```

## Database Queries to Inspect Job State

### Find a Specific Job

```sql
-- Full job details by ID.
-- Live state (status, attempts, scheduled_time, picked_by, picked_at, last_error)
-- lives on scheduler_job_queue while the job is live; the queue row is deleted at
-- the terminal transition. The cold scheduler_job row keeps immutable shape plus
-- terminal_status/terminal_error/total_attempts. LEFT JOIN so a terminated job
-- (no queue row) still returns its cold details.
SELECT c.job_id, q.status, c.terminal_status, c.job_type, c.priority,
       q.attempts, c.total_attempts, c.max_retries, c.backoff_policy,
       q.scheduled_time, q.picked_by, q.picked_at,
       c.execution_start_time, c.execution_end_time, c.execution_duration_ms,
       q.last_error, c.terminal_error, c.business_key, c.resource_name,
       c.payload::jsonb ->> 'target' as target_class,
       c.payload::jsonb ->> 'method' as method_name
FROM scheduler_job c
LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
WHERE c.job_id = '01902c4e-c4f3-7b8a-9d3e-fedcba987654';
```

### Find Failed Jobs by Error Pattern

```sql
-- Find jobs that failed with a specific exception type.
-- A FAILED job is terminal: its hot queue row has been deleted and lifecycle has
-- copied last_error -> terminal_error on the cold scheduler_job row, so query
-- terminal_status / terminal_error here (not the queue).
SELECT job_id, terminal_status, total_attempts, max_retries, terminal_error,
       payload::jsonb ->> 'target' as target_class,
       payload::jsonb ->> 'method' as method_name,
       created_at, terminated_at
FROM scheduler_job
WHERE terminal_status = 'FAILED'
  AND terminal_error LIKE '%ConnectionTimeout%'
ORDER BY terminated_at DESC
LIMIT 20;
```

### View Execution History for a Job

Each job execution attempt is recorded in `scheduler_job_execution`:

```sql
-- See all attempts for a job
SELECT id, attempt, node_id, started_at, ended_at, status,
       duration_ms, error_message, error_class
FROM scheduler_job_execution
WHERE job_id = '01902c4e-c4f3-7b8a-9d3e-fedcba987654'
ORDER BY attempt ASC;
```

### Monitor Job Throughput

```sql
-- Jobs completed per hour over the last 24 hours
SELECT date_trunc('hour', execution_end_time) as hour,
       COUNT(*) as completed,
       AVG(execution_duration_ms) as avg_ms,
       MAX(execution_duration_ms) as max_ms
FROM scheduler_job
WHERE terminal_status = 'SUCCEEDED'
  AND execution_end_time >= NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;
```

### Check Node Health

```sql
-- See which nodes are active and when they last checked in
SELECT node_id, heartbeat_ts, started_at,
       EXTRACT(EPOCH FROM (NOW() - heartbeat_ts)) as seconds_since_heartbeat
FROM scheduler_node
ORDER BY heartbeat_ts DESC;
```

A `seconds_since_heartbeat` greater than the orphan grace period (default 60s) indicates a dead or unresponsive node. The `OrphanRecoveryTimer` will recover jobs from stale nodes automatically.

### Inspect Batch Progress

```sql
-- Batch completion status. The parent's live status is on scheduler_job_queue;
-- joining the queue also restricts the result to still-live batches (the queue
-- row is deleted once the parent terminates).
SELECT b.batch_id, q.status as parent_status, j.business_key,
       b.total_items, b.completed_items, b.failed_items,
       b.completion_processed,
       ROUND(100.0 * (b.completed_items + b.failed_items) / GREATEST(b.total_items, 1), 1) as pct_done
FROM scheduler_batch b
JOIN scheduler_job j ON j.job_id = b.batch_id
JOIN scheduler_job_queue q ON q.job_id = b.batch_id
WHERE q.status IN ('PENDING', 'RUNNING')
ORDER BY b.batch_id DESC;
```

### Find Orphaned Jobs

```sql
-- Jobs stuck in RUNNING on nodes that have gone stale. RUNNING is live state, so
-- status / picked_by / picked_at are read from scheduler_job_queue (this matches
-- the idx_queue_orphan scan the OrphanRecoveryTimer uses).
SELECT q.job_id, q.picked_by, q.picked_at,
       EXTRACT(EPOCH FROM (NOW() - q.picked_at)) / 60 as stuck_minutes,
       n.heartbeat_ts
FROM scheduler_job_queue q
LEFT JOIN scheduler_node n ON n.node_id = q.picked_by
WHERE q.status = 'RUNNING'
  AND (n.node_id IS NULL OR n.heartbeat_ts < NOW() - INTERVAL '60 seconds')
ORDER BY q.picked_at ASC;
```

## Enabling Debug Logging

### By Component

Set fine-grained log levels to focus on the subsystem you are debugging:

| What you are debugging | Logger to set to FINE/DEBUG |
|---|---|
| Job not being picked up | `run.ratchet.ri.core.Poller` |
| Job execution failures | `run.ratchet.ri.core.JobTask` |
| Retry/backoff decisions | `run.ratchet.ri.core.JobTask` |
| Security/ClassPolicy rejections | `run.ratchet.ri.security` |
| Circuit breaker behavior | `run.ratchet.ri.resilience` |
| Thread pool saturation | `run.ratchet.ri.core.ThreadPoolManager` |
| Orphan recovery | `run.ratchet.ri.core.OrphanRecoveryTimer` |
| Timeout enforcement | `run.ratchet.ri.core.JobTimeoutHandler` |
| CDI bean resolution | `run.ratchet.ri.cdi` |
| Recurring job scheduling | `run.ratchet.ri.core.RecurringScheduler` |
| Batch orchestration | `run.ratchet.ri.core.BatchService` |

### Full Debug Mode

To enable debug logging for all of Ratchet (generates significant output):

**WildFly CLI:**
```bash
/subsystem=logging/logger=run.ratchet:add(level=FINE)
```

**Payara asadmin:**
```bash
asadmin set-log-levels run.ratchet=FINE
```

**Open Liberty `server.xml`:**
```xml
<logging traceSpecification="run.ratchet.*=fine"/>
```

### Reading the Log Output

Ratchet logs follow a consistent pattern. Here is what a healthy job execution looks like:

```
INFO  JobTask - Job 12345 starting execution [type=SINGLE, priority=NORMAL, attempt=1/4, payload=MyService.processData]
INFO  JobTask - Job 12345 resolving target: com.myapp.MyService.processData (static=false)
INFO  JobTask - Job 12345 succeeded in 234 ms
INFO  JobTask - Job 12345 execution complete - cleaning up context
```

A failed job with retry:

```
SEVERE JobTask - Job 12345 failed with java.net.ConnectException: Connection refused
WARNING JobTask - Job 12345 retrying in 2000 ms (attempt 1/3) due to: java.net.ConnectException: Connection refused
```

A job that exhausted retries:

```
SEVERE JobTask - Job 12345 failed with java.net.ConnectException: Connection refused
SEVERE JobTask - Job 12345 moved to DLQ after 4 attempts
```

## Debugging Checklist

When a job is not behaving as expected, work through this checklist:

1. **Check the job status:** `SELECT COALESCE(q.status, c.terminal_status) AS status, COALESCE(q.last_error, c.terminal_error) AS last_error FROM scheduler_job c LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id WHERE c.job_id = ?`
2. **Check execution history:** `SELECT * FROM scheduler_job_execution WHERE job_id = ? ORDER BY attempt`
3. **Check per-job logs (if enabled):** `SELECT * FROM scheduler_job_log WHERE job_id = ? ORDER BY ts`
4. **Check if the node is alive:** `SELECT * FROM scheduler_node WHERE node_id = ?`
5. **Check the payload:** `SELECT payload FROM scheduler_job WHERE job_id = ?` -- verify the target class and method are correct
6. **Enable debug logging** for the relevant subsystem (see table above)
7. **Register an event listener** to watch lifecycle events in real time
8. **Check for resource contention:** `SELECT * FROM scheduler_resource_permit WHERE job_id = ?`
