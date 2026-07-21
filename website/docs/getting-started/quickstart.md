---
sidebar_position: 3
title: Quick Start
description: Schedule your first background job with Ratchet in under 5 minutes
---

# Quick Start

This guide gets you from zero to a running background job in the shortest path possible. Inject `JobSchedulerService`, enqueue a job, and verify it runs. No retries, no callbacks, no configuration -- just the fire-and-forget pattern.

## Prerequisites

Before starting, make sure you have:

1. A Jakarta EE 10/11 project with a running application server (WildFly, Open Liberty, Payara, GlassFish 8, etc.)
2. A database (PostgreSQL, MySQL, Oracle, SQL Server, or MongoDB) accessible from your application
3. Ratchet dependencies added to your `pom.xml` (see [Installation](./installation.md))
4. The Ratchet schema applied to your database
5. A `@Produces RatchetOptions` CDI bean (required; no automatic fallback)
6. A `ClassPolicy` CDI alternative that allows your application's packages

If you haven't done steps 3 and 4 yet, here's the minimum `pom.xml` setup:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>run.ratchet</groupId>
      <artifactId>ratchet-bom</artifactId>
      <version>0.2.1</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-api</artifactId>
  </dependency>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet</artifactId>
  </dependency>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-store-postgresql</artifactId>
  </dependency>
</dependencies>
```

And apply the schema:

```bash
psql -d mydb -f stores/ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql
```

And produce a `RatchetOptions` bean so the scheduler can start. The smallest viable producer reads `RATCHET_*` environment variables:

```java
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class RatchetConfig {

    @Produces
    @ApplicationScoped
    RatchetOptions ratchetOptions() {
        return RatchetOptionsFactory.fromEnvironment();
    }
}
```

Without this bean, CDI fails deployment with `UnsatisfiedResolutionException` and the scheduler never starts. See [Configuration](./configuration.md) for the builder-based alternative and custom sources.

And install the required `ClassPolicy` override before you boot the app:

```java
@Alternative
@Priority(jakarta.interceptor.Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class AppClassPolicy implements ClassPolicy {

    @Override
    public boolean isAllowed(String className) {
        return className.startsWith("com.example.");
    }
}
```

Ratchet fails fast at startup if you leave the default allowlist empty. The `RatchetOptions.builder().security(s -> s.allowEmptyClassPolicy(true)).build()` escape hatch is only for demos and tests.

## Step 1: Create a CDI bean

Create a simple `@ApplicationScoped` bean that injects `JobSchedulerService`:

```java
package com.example.app;

import run.ratchet.api.JobSchedulerService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.logging.Logger;

@ApplicationScoped
public class OrderService {

    private static final Logger log = Logger.getLogger(OrderService.class.getName());

    @Inject
    JobSchedulerService scheduler;

    public void placeOrder(long orderId) {
        // Schedule the job for immediate execution
        scheduler.enqueueNow(() -> processOrder(orderId));

        log.info("Order " + orderId + " enqueued for background processing");
    }

    public void processOrder(long orderId) {
        log.info("Processing order " + orderId + " in background...");

        // Your business logic here
        // This runs asynchronously on a worker thread
    }
}
```

That's the entire integration. Here's what happens when `placeOrder` is called.

## Step 2: Understand the flow

When you call `scheduler.enqueueNow(() -> processOrder(orderId))`, Ratchet:

1. **Serializes the lambda** -- The method reference `processOrder(orderId)` is analyzed via ASM bytecode analysis, capturing the target class, method name, and argument values.
2. **Persists the job** -- A new row is inserted into the `scheduler_job` table with the serialized payload and an auto-generated idempotency key, alongside a matching `scheduler_job_queue` row that holds the live status `PENDING`. (Live queue state lives on `scheduler_job_queue`; `scheduler_job` keeps the cold metadata and terminal fields.)
3. **Returns immediately** -- `enqueueNow` returns a `JobHandle` with the job's UUIDv7 database ID. Your calling code doesn't block.
4. **Poller picks it up** -- The Ratchet poller (started automatically at application startup by `RatchetLifecycle`) claims the job and submits it to a worker thread.
5. **Worker executes** -- The worker thread deserializes the payload, resolves `OrderService` via CDI, and calls `processOrder(orderId)`.

The key insight: **the job survives server restarts**. Because the payload is persisted to your database, if the server crashes between step 2 and step 5, the job will be picked up after the server comes back.

## Step 3: Verify it runs

### Check the logs

When the application starts, you should see:

```
INFO [run.ratchet.ri.cdi.RatchetLifecycle] Ratchet starting
INFO [run.ratchet.ri.core.internal.DefaultNodeIdentityProvider] Scheduler nodeId=...
INFO [run.ratchet.ri.core.internal.Poller] Poller initialized (batch=50)
INFO [run.ratchet.ri.cdi.RatchetLifecycle] Ratchet started
```

When a job is enqueued and processed:

```
INFO [com.example.app.OrderService] Order 42 enqueued for background processing
INFO [com.example.app.OrderService] Processing order 42 in background...
```

### Check the database

You can also verify by querying the tables directly. While a job is live, its
status sits on `scheduler_job_queue`:

```sql
SELECT job_id, status, scheduled_time
FROM scheduler_job_queue
ORDER BY job_id DESC
LIMIT 5;
```

When a job reaches a terminal state, the queue row is removed and the outcome is
recorded on `scheduler_job` via `terminal_status` and the timing columns:

```sql
SELECT job_id, terminal_status, created_at, execution_start_time, execution_end_time
FROM scheduler_job
ORDER BY job_id DESC
LIMIT 5;
```

A completed job will show `terminal_status` `SUCCEEDED` with populated timing columns.

## The fire-and-forget pattern

The `enqueueNow` method is the simplest entry point. It takes a `SerializableCheckedRunnable` -- a lambda or method reference that can throw checked exceptions -- and returns a `JobHandle`:

```java
JobHandle handle = scheduler.enqueueNow(() -> doSomething());
UUID jobId = handle.id();
```

The `JobHandle` is a lightweight receipt containing just the job's UUIDv7 database ID. You can use it to:

- Log the job ID for correlation
- Cancel the job later with `scheduler.cancelJob(jobId)`
- Retry a failed job with `scheduler.retryJob(jobId)`

### What about method arguments?

The lambda captures values at submission time. These values are serialized and stored in the database as part of the job payload. When the job executes, the values are deserialized and passed to your method:

```java
// The orderId value (42) is captured and serialized at enqueue time
scheduler.enqueueNow(() -> processOrder(42L));

// You can also capture local variables
long orderId = order.getId();
String customerName = order.getCustomerName();
scheduler.enqueueNow(() -> processOrder(orderId));
```

:::caution Keep payloads small
Ratchet persists the target method metadata and captured argument values extracted by `JobInvocationResolver`. Pass IDs and simple serializable values, not large object graphs. A job that needs a complex object should accept an ID and look up the object from the database during execution. Job return values are stored separately through `ResultPersistenceStrategy`.
:::

## Delayed execution

If you want to schedule a job for later instead of immediately, use `schedule`:

```java
import java.time.Duration;

// Execute 30 minutes from now
scheduler.schedule(Duration.ofMinutes(30), () -> sendReminder(orderId))
    .submit();
```

Note the difference: `schedule` returns a `JobBuilder` (not a `JobHandle`) so you can configure the job further before calling `.submit()`. The `enqueueNow` shorthand skips the builder and submits immediately.

## Adding an event observer

Even in this minimal setup, you can observe job events using CDI's `@Observes`:

```java
package com.example.app;

import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobFailedEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.logging.Logger;

@ApplicationScoped
public class JobMonitor {

    private static final Logger log = Logger.getLogger(JobMonitor.class.getName());

    public void onJobCompleted(@Observes JobCompletedEvent event) {
        log.info("Job " + event.getJobId() + " completed successfully");
    }

    public void onJobFailed(@Observes JobFailedEvent event) {
        log.warning("Job " + event.getJobId() + " failed: " + event.getErrorMessage());
    }
}
```

These observers fire for **all** jobs, not just the ones you enqueued from `OrderService`. This is intentional -- it gives you a single place to add logging, alerting, or metrics for your entire job pipeline.

## Common issues

### "No bean found for JobSchedulerService"

This usually means `ratchet` is not on the classpath, or your `beans.xml` is configured to prevent bean discovery. Ratchet's CDI beans use `@ApplicationScoped` and are discovered through annotated bean discovery. Make sure your `beans.xml` uses `bean-discovery-mode="annotated"` (the default in Jakarta CDI 4.0) or `bean-discovery-mode="all"`.

### "No JobStore implementation found"

You need a store module (`ratchet-store-postgresql`, `ratchet-store-mysql`, `ratchet-store-oracle`, or `ratchet-store-mongodb`) on the classpath, and it needs a configured `DataSource` or connection. Check that your application server's data source JNDI name matches what the store expects.

### Startup fails with `ClassPolicy allowedPackages is empty`

Ratchet refuses to boot until you provide a `ClassPolicy` override. Install the `@Alternative @Priority(APPLICATION)` bean shown above, or pass `s -> s.allowEmptyClassPolicy(true)` to the `security(...)` method on the `RatchetOptions` builder only in demos and tests.

### Jobs are enqueued but never execute

Check that `RatchetLifecycle` logged `Ratchet started` and that `Poller` logged `Poller initialized (...)`. If you don't see those messages, the lifecycle bean isn't being activated. This can happen if CDI bean discovery is misconfigured, if your application didn't produce a `RatchetOptions` bean (deployment fails with `UnsatisfiedResolutionException`), or if the `ratchet` module isn't deployed.

## What's next

This quick start covered the simplest possible pattern: fire-and-forget. In practice, you'll want retries, backoff, callbacks, parameters, and monitoring. The next guide walks through all of these:

- [Your First Job](./first-job.md) -- Complete walkthrough with retry, backoff, parameters, callbacks, and event monitoring
- [Configuration](./configuration.md) -- CDI producer setup, SPI defaults, and runtime tuning
