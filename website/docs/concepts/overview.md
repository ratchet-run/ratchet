---
sidebar_position: 1
title: Architecture Overview
description: High-level architecture of Ratchet and how it fits into a Jakarta EE 10/11 application
---

# Architecture Overview

Ratchet is a portable, CDI-based job scheduler for Jakarta EE 10/11 applications. It provides persistent, cluster-safe background job scheduling with a fluent API -- covering batching, chaining, workflows, and transactional enqueueing out of the box.

## Where Ratchet Fits

In a typical Jakarta EE application, Ratchet sits between your business logic and the database. You inject `JobSchedulerService`, enqueue work using lambda expressions, and Ratchet handles persistence, polling, execution, retries, and lifecycle events.

<div class="ratchet-fit-diagram" role="group" aria-label="Ratchet architecture: application code submits jobs through JobSchedulerService into the Ratchet engine, which persists through the JobStore SPI backed by MySQL, PostgreSQL, Oracle, SQL Server, or MongoDB.">
  <div class="fit-layer fit-layer-app">
    <div>
      <span class="fit-kicker">Your Jakarta EE Application</span>
      <strong>Business services submit durable work</strong>

```java
scheduler.enqueue(() -> orderService.process(id)).withMaxRetries(3).submit();
```

</div>
  </div>

  <div class="fit-connector">
    <span>JobSchedulerService</span>
  </div>

  <div class="fit-layer fit-layer-engine">
    <div class="fit-layer-header">
      <span class="fit-kicker">Ratchet Engine</span>
      <strong>Persist, claim, execute, retry, observe</strong>
    </div>
    <div class="fit-engine-grid">
      <div class="fit-node">
        <span>Poller</span>
        <small>Adaptive polling</small>
      </div>
      <div class="fit-node">
        <span>JobTask</span>
        <small>Execution + retry</small>
      </div>
      <div class="fit-node">
        <span>Workflow</span>
        <small>Chains + conditions</small>
      </div>
      <div class="fit-node">
        <span>Batch</span>
        <small>Parent + child jobs</small>
      </div>
      <div class="fit-node">
        <span>Events</span>
        <small>CDI + listeners</small>
      </div>
    </div>
  </div>

  <div class="fit-connector">
    <span>JobStore SPI</span>
  </div>

  <div class="fit-store-grid">
    <div class="fit-store">
      <span>MySQL Store</span>
      <small>SKIP LOCKED claiming</small>
    </div>
    <div class="fit-store">
      <span>PostgreSQL Store</span>
      <small>SKIP LOCKED claiming</small>
    </div>
    <div class="fit-store">
      <span>Oracle Store</span>
      <small>SKIP LOCKED claiming</small>
    </div>
    <div class="fit-store">
      <span>MongoDB Store</span>
      <small>Atomic document updates</small>
    </div>
  </div>
</div>

## Module Structure

Ratchet is organized into modules following the Jakarta EE API / RI / TCK pattern:

<div class="module-inventory" role="list" aria-label="Ratchet Maven modules">
  <section class="module-section module-section-api" role="listitem">
    <div class="module-section-header">
      <span class="fit-kicker">Public Contracts</span>
      <strong>Depend on this from applications and integrations</strong>
    </div>
    <div class="module-chip-grid">
      <div class="module-chip">
        <code>ratchet-api</code>
        <p>Public API, SPIs, lifecycle events, annotations, and Jakarta EE-facing contracts.</p>
      </div>
    </div>
  </section>

  <section class="module-section module-section-runtime" role="listitem">
    <div class="module-section-header">
      <span class="fit-kicker">Runtime</span>
      <strong>Engine and store implementations deployed at runtime</strong>
    </div>
    <div class="module-chip-grid">
      <div class="module-chip">
        <code>ratchet</code>
        <p>Reference implementation, CDI integration, scheduler engine, poller, and executors.</p>
      </div>
      <div class="module-chip">
        <code>ratchet-store-core</code>
        <p>Shared persistence entities and internal store support used by store adapters.</p>
      </div>
      <div class="module-chip">
        <code>ratchet-store-mysql</code>
        <p>MySQL JobStore implementation and DDL.</p>
      </div>
      <div class="module-chip">
        <code>ratchet-store-postgresql</code>
        <p>PostgreSQL JobStore implementation and DDL.</p>
      </div>
      <div class="module-chip">
        <code>ratchet-store-oracle</code>
        <p>Oracle 23ai JobStore implementation and DDL.</p>
      </div>
      <div class="module-chip">
        <code>ratchet-store-mongodb</code>
        <p>MongoDB JobStore implementation with collection and index bootstrap.</p>
      </div>
    </div>
  </section>

  <section class="module-section module-section-observability" role="listitem">
    <div class="module-section-header">
      <span class="fit-kicker">Observability</span>
      <strong>Optional adapters for metrics and traces</strong>
    </div>
    <div class="module-chip-grid">
      <div class="module-chip">
        <code>ratchet-micrometer</code>
        <p>Micrometer metrics integration built on the public API.</p>
      </div>
      <div class="module-chip">
        <code>ratchet-otel</code>
        <p>OpenTelemetry integration built on the public API.</p>
      </div>
    </div>
  </section>

  <section class="module-section module-section-validation" role="listitem">
    <div class="module-section-header">
      <span class="fit-kicker">Validation</span>
      <strong>Conformance, integration, load, JPMS, and coverage modules</strong>
    </div>
    <div class="module-chip-grid">
      <div class="module-chip">
        <code>ratchet-tck</code>
        <p>Technology Compatibility Kit aggregator.</p>
        <div class="module-tags">
          <span>util</span>
          <span>store</span>
          <span>api</span>
          <span>jakarta</span>
        </div>
      </div>
      <div class="module-chip">
        <code>ratchet-testsuite</code>
        <p>Container and store integration tests.</p>
      </div>
      <div class="module-chip">
        <code>ratchet-testsuite-jpms</code>
        <p>JPMS/module-system integration tests.</p>
      </div>
      <div class="module-chip">
        <code>ratchet-loadtest</code>
        <p>Load and stress-test harness.</p>
      </div>
      <div class="module-chip">
        <code>ratchet-coverage</code>
        <p>Aggregate coverage module for reactor reporting.</p>
      </div>
    </div>
  </section>

  <section class="module-section module-section-distribution" role="listitem">
    <div class="module-section-header">
      <span class="fit-kicker">Distribution</span>
      <strong>Version alignment for consumers</strong>
    </div>
    <div class="module-chip-grid">
      <div class="module-chip">
        <code>ratchet-bom</code>
        <p>Bill of Materials POM for importing consistent Ratchet dependency versions.</p>
      </div>
    </div>
  </section>
</div>

### Module Dependency Graph

<div class="module-dependency-diagram" role="img" aria-label="Ratchet module dependency graph. ratchet-api has only Jakarta EE API dependencies. The engine, store core, TCK, tests, observability modules, and BOM build on the API. Store implementations build on store core.">
  <div class="dependency-layer">
    <span class="dependency-layer-label">Public API</span>
    <div class="dependency-node dependency-node-api">
      <strong>ratchet-api</strong>
      <small>Jakarta EE APIs only</small>
    </div>
  </div>

  <div class="dependency-bridge">
    <span>runtime, observability, tests, and distribution build from the public contracts</span>
  </div>

  <div class="dependency-layer">
    <span class="dependency-layer-label">API Consumers</span>
    <div class="dependency-node-grid">
      <div class="dependency-node">
        <strong>ratchet</strong>
        <small>Engine + CDI integration</small>
      </div>
      <div class="dependency-node">
        <strong>ratchet-store-core</strong>
        <small>Internal shared store layer</small>
      </div>
      <div class="dependency-node">
        <strong>ratchet-tck</strong>
        <small>Conformance modules</small>
      </div>
      <div class="dependency-node">
        <strong>ratchet-micrometer</strong>
        <small>Metrics adapter</small>
      </div>
      <div class="dependency-node">
        <strong>ratchet-otel</strong>
        <small>Tracing adapter</small>
      </div>
      <div class="dependency-node">
        <strong>ratchet-bom</strong>
        <small>Version alignment</small>
      </div>
    </div>
  </div>

  <div class="dependency-bridge dependency-bridge-store">
    <span>store adapters share the internal store core</span>
  </div>

  <div class="dependency-layer">
    <span class="dependency-layer-label">Stores</span>
    <div class="dependency-node-grid dependency-store-grid">
      <div class="dependency-node dependency-node-store">
        <strong>ratchet-store-mysql</strong>
        <small>MySQL + DDL</small>
      </div>
      <div class="dependency-node dependency-node-store">
        <strong>ratchet-store-postgresql</strong>
        <small>PostgreSQL + DDL</small>
      </div>
      <div class="dependency-node dependency-node-store">
        <strong>ratchet-store-oracle</strong>
        <small>Oracle + DDL</small>
      </div>
      <div class="dependency-node dependency-node-store">
        <strong>ratchet-store-mongodb</strong>
        <small>MongoDB collections + indexes</small>
      </div>
    </div>
  </div>
</div>

**Key design constraint:** `ratchet-api` has zero runtime dependencies beyond Jakarta EE APIs supplied by the runtime. Your application can depend on `ratchet-api` for event types and annotations without pulling in the engine.

## Core Concepts

### Pull-Based Architecture

Ratchet uses a **pull model** where worker threads poll the database for available jobs. This provides natural backpressure -- workers only claim new jobs when they have capacity. The [Poller](./execution-model.md) uses adaptive algorithms to balance responsiveness against database load.

### Store as the Queue

Unlike message-broker-based schedulers, Ratchet uses your selected store backend as the job queue. SQL stores split storage across a cold `scheduler_job` metadata table and a hot `scheduler_job_queue` table that holds live state and is claimed with `SELECT ... FOR UPDATE SKIP LOCKED`; MongoDB keeps jobs in the `scheduler_job` collection and claims with atomic document updates. This gives you:

- **Transactional enqueueing** -- SQL job creation participates in your existing transaction; MongoDB uses store-level atomic writes
- **Durability** -- jobs survive application restarts
- **Visibility** -- query job status with standard SQL or MongoDB queries
- **No additional infrastructure** -- no Redis, RabbitMQ, or Kafka required

### Lambda-Based API

Jobs are defined as serializable lambda expressions. Ratchet uses ASM bytecode analysis to extract the target class, method, and arguments from the lambda, then persists this information as a portable payload. At execution time, the method is invoked reflectively on a CDI-managed bean:

```java
// This lambda is analyzed at enqueue time, not executed
scheduler.enqueue(() -> orderService.processOrder(orderId))
    .submit();

// Ratchet extracts: target=OrderService, method=processOrder, args=[orderId]
// At execution time: CDI resolves OrderService, invokes processOrder(orderId)
```

### SPI-Driven Extension

Ratchet separates API contracts from implementation through Service Provider Interfaces. The engine consults SPIs for persistence, invocation resolution, retry logic, security, metrics, logging, configuration, and cluster coordination. Default implementations are provided, and you can replace any of them:

| SPI | Purpose | Default |
|-----|---------|---------|
| `JobStore` | Persistence backend | MySQL / PostgreSQL / Oracle / SQL Server / MongoDB modules |
| `JobInvocationResolver` | Callback-to-method invocation resolution | ASM bytecode analysis |
| `ResultPersistenceStrategy` | Job return-value persistence | JSON metadata with size cap |
| `RatchetOptions` | Typed runtime options | Required CDI producer; see [Configuration](/getting-started/configuration) |
| `RetryPolicy` | Custom retry decisions | Passthrough (uses job config) |
| `ClassPolicy` | Security allowlist | Empty package allowlist; startup fails fast until you provide one |
| `MetricsCollector` | Observability hooks | No-op |
| `ClusterCoordinator` | Cross-node wakeups | No-op (single-node) |

Every SPI listed here is marked `@Incubating` and may change before 1.0. The following are the most likely to change:

| SPI | Purpose | Default |
|-----|---------|---------|
| `NodeIdentityProvider` | Cluster node identity | Hostname-PID-UUID suffix |
| `ExecutorProvider` | Thread pool management | Container-managed executors |
| `JobLoggerFactory` | Structured job logging | JBoss Logging-backed logger |
| `ErrorSanitizer` | Exception message sanitization | Truncate + strip PII |

### Event System

Ratchet publishes lifecycle events that your application can observe. Events live in `ratchet-api` so you can depend on event types without pulling in the engine. In CDI environments, use standard `@Observes`:

```java
public void onJobFailed(@Observes JobFailedEvent event) {
    alertService.notify(event.getJobId(), event.getErrorMessage());
}
```

In non-CDI environments, register a programmatic listener:

```java
scheduler.addEventListener(event -> {
    if (event instanceof JobDlqEvent dlq) {
        log.severe("Job " + dlq.getJobId() + " moved to DLQ");
    }
});
```

## Minimal Setup

Add Ratchet to your Jakarta EE application:

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
    <artifactId>ratchet</artifactId>
  </dependency>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-store-mysql</artifactId>
    <!-- or ratchet-store-postgresql / ratchet-store-oracle / ratchet-store-mongodb -->
  </dependency>
</dependencies>
```

For SQL stores, apply the DDL schema from the store module's `ddl/` directory. For MongoDB, let the store module initialize collections and indexes at startup. Then inject and use:

```java
@Inject JobSchedulerService scheduler;

// Fire-and-forget
scheduler.enqueueNow(() -> emailService.sendWelcome(userId));

// Configured job
scheduler.enqueue(() -> reportService.generate(month))
    .withPriority(JobPriority.HIGH)
    .withTimeout(Duration.ofMinutes(30))
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(10))
    .withTags("reports", "monthly")
    .submit();

// Recurring job via annotation
@Recurring(cron = "0 0 2 * * ?", name = "Nightly Cleanup")
public void cleanup() { ... }
```

## What's Next

- [Job Lifecycle](./job-lifecycle.md) -- Understand the complete state machine
- [Job Types](./job-types.md) -- SINGLE, BATCH, CHAIN, WORKFLOW, RECURRING, and SYSTEM
- [Scheduling](./scheduling.md) -- Immediate, delayed, and cron-based scheduling
- [Execution Model](./execution-model.md) -- How jobs are polled, claimed, and executed
- [Error Handling](./error-handling.md) -- Retries, DLQ, and `@DoNotRetry`
- [Persistence](./persistence.md) -- Entity model and JobStore SPI
- [Clustering](./clustering.md) -- Multi-node coordination
