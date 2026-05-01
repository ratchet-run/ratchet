---
sidebar_position: 3
title: Basic Concepts
description: Core Ratchet terminology — jobs, schedules, stores, builders, contexts, and the CDI programming model
---

# Basic Concepts

Before diving into the API, it helps to understand the core building blocks. Ratchet has a small set of concepts that compose together to handle everything from one-off background tasks to multi-step workflows.

## Jobs

A **job** is a unit of work that Ratchet persists and executes. At its simplest, a job is a serialized lambda — a method reference that gets stored in the database and invoked later by a worker thread.

```java
// This lambda IS the job
scheduler.enqueueNow(() -> sendWelcomeEmail(userId));
```

Every job has:

| Property | Description |
|----------|-------------|
| **ID** | Auto-assigned numeric identifier |
| **Status** | Current lifecycle state (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`, `PAUSED`) |
| **Priority** | Execution ordering — `LOWEST`, `LOW`, `NORMAL`, `HIGH`, `CRITICAL` |
| **Payload** | The serialized lambda or method reference |
| **Parameters** | Key-value pairs available at execution time via `JobContext` |
| **Tags** | Labels for grouping and bulk operations |

Jobs progress through a [lifecycle](../concepts/job-lifecycle.md) from creation to completion (or failure).

## Job Types

Ratchet supports several execution patterns:

- **Single** — Execute once, right now or after a delay
- **Recurring** — Execute on a cron schedule, creating child instances each time
- **Batch parent** — Coordinate parallel execution of child jobs
- **Batch child** — Individual items within a batch
- **Chained** — Execute after another job completes (success, failure, or conditional)

See [Job Types](../concepts/job-types.md) for details on each pattern.

## Schedules

A **schedule** determines _when_ a job runs:

```java
// Immediate
scheduler.enqueueNow(() -> doWork());

// Delayed
scheduler.schedule(Duration.ofMinutes(30), () -> doWork());

// Cron (recurring)
scheduler.scheduleRecurring("0 0 2 * * ?", ZoneId.of("UTC"), () -> doWork());
```

Recurring jobs use Quartz-compatible cron expressions. The schedule is evaluated by the poller, which checks for due jobs on a configurable interval.

## Builders

Ratchet uses the **fluent builder** pattern to configure jobs before submission. Every `enqueue`, `schedule`, and `recurring` call returns a builder:

```java
scheduler.enqueue(() -> processInvoice(invoiceId))
    .withPriority(JobPriority.HIGH)    // Execution priority
    .withMaxRetries(3)                 // Retry on failure
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(5))
    .withTimeout(Duration.ofMinutes(10))
    .withTags("billing", "invoices")
    .withParam("invoiceId", String.valueOf(invoiceId))
    .withBusinessKey("invoice-" + invoiceId)
    .submit();                         // Persists and returns a JobHandle
```

The builder is not submitted until you call `.submit()` (or `.immediate()` for cluster-wide wakeup). See the [JobBuilder API](../api-reference/job-builder.md) for all options.

## JobHandle

A `JobHandle` is returned when you submit a job. It gives you the job's ID for tracking:

```java
JobHandle handle = scheduler.enqueueNow(() -> doWork());
UUID jobId = handle.id();
```

## JobContext

The **JobContext** is the runtime context available inside a running job. It provides access to the job's ID, parameters, and a structured logger:

```java
scheduler.enqueue(() -> {
    JobContext ctx = JobContext.current();
    String orderId = ctx.param("orderId");
    ctx.logger().info("Processing order " + orderId);
    processOrder(orderId);
}).withParam("orderId", "12345")
  .submit();
```

For annotation-based jobs, you can accept the context as a parameter:

```java
@Recurring(cron = "0 0 * * * ?")
public void hourlySync(JobContext ctx) {
    ctx.logger().info("Starting sync for job " + ctx.jobId());
}
```

## JobResult

A **JobResult** represents the outcome of a job execution. Jobs that return void produce a success result automatically. For richer outcomes, your job can return a `JobResult` explicitly:

```java
// Implicit success (void return)
scheduler.enqueueNow(() -> sendEmail(to, body));

// Explicit result for workflow branching
JobResult.success(invoiceTotal);
JobResult.failure("Payment declined", exception);
```

Results drive [workflow branching](../concepts/workflows.md) — downstream jobs can inspect the parent result and conditionally execute.

## Stores

A **store** is the persistence backend that holds jobs, execution history, locks, and node registrations. Ratchet separates the store into fine-grained SPI interfaces:

| Store SPI | Responsibility |
|-----------|---------------|
| `JobCrudStore` | CRUD operations on jobs |
| `JobClaimStore` | Claiming due jobs for execution (`SKIP LOCKED` on SQL stores, atomic updates on MongoDB) |
| `ExecutionStore` | Recording execution history |
| `JobLogStore` | Optional structured storage for persisted `JobLogLine` events |
| `ArchiveStore` | Moving completed jobs to archive |
| `NodeStore` | Cluster node heartbeats |
| `LockStore` | Distributed advisory locks |

You choose a store implementation as a Maven dependency:

```xml
<!-- Pick one -->
<artifactId>ratchet-store-postgresql</artifactId>
<artifactId>ratchet-store-mysql</artifactId>
<artifactId>ratchet-store-mongodb</artifactId>
```

See [Persistence](../concepts/persistence.md) for how stores work internally.

## CDI Integration

Ratchet is **CDI-native**. The core entry point, `JobSchedulerService`, is a CDI bean that you inject:

```java
@Inject
JobSchedulerService scheduler;
```

No factory classes, no static initializers, no XML configuration. CDI discovery finds the Ratchet beans automatically when the modules are on the classpath.

### Annotations

The `@Recurring` annotation lets you declare cron-scheduled jobs on CDI bean methods:

```java
@ApplicationScoped
public class MaintenanceJobs {

    @Recurring(cron = "0 0 3 * * ?", id = "nightly-cleanup")
    public void nightlyCleanup() {
        // Runs every night at 3 AM
    }
}
```

### SPI Overrides

Many Ratchet behaviors are pluggable via CDI alternatives. To replace the default retry policy, for example, produce a CDI bean that implements `RetryPolicy`:

```java
@ApplicationScoped
public class MyRetryPolicy implements RetryPolicy {
    // Your custom logic
}
```

CDI selects your implementation over the default. See [SPI Interfaces](../api-reference/spi-interfaces.md) for the full list of extension points.

## Events

Ratchet publishes **CDI events** for every lifecycle transition. You can observe them with standard CDI:

```java
public void onJobCompleted(@Observes JobCompletedEvent event) {
    log.info("Job " + event.getJobId() + " completed in " + event.getExecutionTimeMs() + "ms");
}
```

Or register a programmatic listener:

```java
scheduler.addEventListener(event -> {
    if (event instanceof JobFailedEvent failed) {
        alertOps(failed.getJobId(), failed.getErrorMessage());
    }
});
```

See the [Event System](../api-reference/event-system.md) for all event types.

## What's Next

Now that you know the vocabulary, the best way to learn is to build something:

- **[Your First Job](./first-job.md)** — Build a complete job with retries, callbacks, and monitoring
- **[Configuration](./configuration.md)** — Tune polling intervals, thread pools, and timeouts
- **[Job Lifecycle](../concepts/job-lifecycle.md)** — Understand every state transition in detail
