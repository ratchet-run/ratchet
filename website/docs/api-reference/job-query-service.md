---
sidebar_position: 3
title: JobQueryService Reference
description: Read-only query API for dashboards, admin tools, job detail pages, history, and queue health.
---

# JobQueryService

`JobQueryService` is the read-only companion to [`JobSchedulerService`](./job-scheduler-service). Inject it in dashboards, CLIs, support tools, and health checks that need to inspect jobs without scheduling or mutating them.

```java
@Inject
JobQueryService jobs;
```

**Package:** `run.ratchet.api`
**Type:** Interface
**Stability:** `@Incubating`

## When to use it

Use `JobQueryService` when you need:

- A pageable job list for an admin screen
- A detail view for a single job, including parameters and execution history
- Batch child listings
- Active recurring master listings
- A point-in-time queue health snapshot

For lifecycle operations such as cancel, pause, resume, retry, and signal delivery, use [`JobSchedulerService`](./job-scheduler-service).

## Methods

### findJobs

```java
JobPage<JobSummary> findJobs(JobFilter filter, int limit, int offset)
```

Returns a page of lightweight job summaries matching the supplied filter.

```java
JobFilter filter = JobFilter.builder()
    .statuses(JobStatus.PENDING, JobStatus.RUNNING, JobStatus.FAILED)
    .types(JobType.SINGLE, JobType.CHAIN)
    .tags("billing")
    .createdAfter(Instant.now().minus(7, ChronoUnit.DAYS))
    .sortField(JobQuerySortField.CREATED_AT)
    .sortAscending(false)
    .build();

JobPage<JobSummary> page = jobs.findJobs(filter, 50, 0);

for (JobSummary job : page.items()) {
    log.info("{} {} {}", job.id(), job.status(), job.methodName());
}
```

`JobFilter` fields are optional. A `null` field means "do not constrain this dimension." Vararg filters such as `statuses()` and `types()` ignore empty argument lists, so a UI can safely pass zero selected statuses to mean "all statuses."

When the store advertises the `JobExtensionStore` capability, jobs can also be filtered by their indexed extension properties:

```java
JobFilter filter = JobFilter.builder()
    .propertyEquals("ratchet-blocks.block_name", "invoice.send")
    .propertyIn("ratchet-blocks.block_version", Set.of("1", "2"))
    .build();
```

`propertyEquals(key, value)` requires an exact key/value match; `propertyIn(key, values)` requires the key to hold one of the given values. Constraints on different keys combine with AND; a repeated call for the same key replaces the earlier constraint.

### getJobDetail

```java
Optional<JobDetail> getJobDetail(UUID jobId)
```

Returns the full read-only job view, including the summary, job parameters, trace context, result metadata, timing, execution history, and direct dependant job IDs.

```java
JobDetail detail = jobs.getJobDetail(jobId)
    .orElseThrow(() -> new NotFoundException("job not found"));

JobSummary summary = detail.summary();
Map<String, String> params = detail.params();
List<ExecutionHistorySummary> history = detail.executionHistory();
```

The result is empty when the job does not exist or the caller is not allowed to read it.

### getExecutionHistory

```java
List<ExecutionHistorySummary> getExecutionHistory(UUID jobId)
JobPage<ExecutionHistorySummary> getExecutionHistory(UUID jobId, int limit, int offset)
```

Returns execution attempts for a job ordered by attempt number. The list-returning convenience
method returns the first default page.

```java
JobPage<ExecutionHistorySummary> attempts = jobs.getExecutionHistory(jobId, 100, 0);
long failures = attempts.items().stream()
    .filter(attempt -> !attempt.succeeded())
    .count();
```

### getQueueHealth

```java
QueueHealthSnapshot getQueueHealth()
```

Returns best-effort aggregate counts from the backing store.

```java
QueueHealthSnapshot health = jobs.getQueueHealth();
log.info("pending={} running={} failed={}",
    health.pendingCount(),
    health.runningCount(),
    health.failedCount());
```

This snapshot is intentionally not transactionally consistent across every field. Treat it as dashboard telemetry, not as a decision-making lock.

### getDependants

```java
List<JobSummary> getDependants(UUID jobId)
JobPage<JobSummary> getDependants(UUID jobId, int limit, int offset)
```

Returns direct dependant jobs whose `dependsOn` field points at the supplied parent job. The
list-returning convenience method returns the first default page.

### getBatchChildren

```java
JobPage<JobSummary> getBatchChildren(UUID batchParentId)
JobPage<JobSummary> getBatchChildren(UUID batchParentId, int limit, int offset)
```

Returns child jobs for a batch parent.

```java
JobPage<JobSummary> children = jobs.getBatchChildren(batchId, 100, 0);
int completed = (int) children.items().stream()
    .filter(child -> child.status() == JobStatus.SUCCEEDED)
    .count();
```

### getRecurringMasters

```java
JobPage<JobSummary> getRecurringMasters()
JobPage<JobSummary> getRecurringMasters(int limit, int offset)
```

Returns active recurring master records. Recurring masters store the recurring schedule (cron expression or fixed interval) and spawn child jobs for each firing.

## Pagination

`JobPage<T>` contains:

| Field | Meaning |
|---|---|
| `items()` | Results on this page |
| `totalCount()` | Total matches, or `-1` when counting was skipped |
| `limit()` | Requested page size |
| `offset()` | Zero-based offset |
| `hasMore()` | Whether another page is available |
| `nextCursor()` | Cursor token for cursor-aware stores, or null |

For large datasets, prefer filters that set `skipCount(true)` and use `nextCursor()` when your store returns one:

```java
JobFilter filter = JobFilter.builder()
    .statuses(JobStatus.FAILED)
    .skipCount(true)
    .cursor(previousCursor)
    .build();

JobPage<JobSummary> failedJobs = jobs.findJobs(filter, 100, 0);
String nextCursor = failedJobs.nextCursor();
```

## Authorization

The reference implementation applies the configured `JobAuthorizationPolicy` to single-job reads. List queries are scoped by `JobAuthorizationPolicy.filterForPrincipal()` before they reach the store.

```java
public JobFilter filterForPrincipal(JobFilter filter, String principal) {
    return filter.toBuilder()
        .callerPrincipal(principal)
        .build();
}
```

Use `filter.toBuilder()` when adding authorization constraints so you do not discard the caller's original filters.

## See also

- [JobSchedulerService Reference](./job-scheduler-service)
- [JobBuilder Reference](./job-builder)
- [Job Lifecycle](../concepts/job-lifecycle)
- [Persistence](../concepts/persistence)
