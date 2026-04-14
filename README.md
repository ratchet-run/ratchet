# Ratchet

**Portable, CDI-based job scheduler for Jakarta EE.**

Ratchet gives Jakarta EE applications a clean, annotation-driven API for background job scheduling with persistent storage, automatic retries, workflow orchestration, and built-in resilience — all without pulling in heavyweight frameworks.

---

## Features

| Category | Capabilities |
|----------|-------------|
| **Scheduling** | Immediate, delayed, cron-based recurring jobs |
| **Workflows** | Job chaining, conditional branching, success/failure callbacks |
| **Batching** | In-memory batch builder and streaming batch for large datasets |
| **Resilience** | Configurable retries with backoff (fixed/exponential), built-in circuit breaker, dead letter queue |
| **Persistence** | Pluggable store SPI — MySQL, PostgreSQL, and MongoDB out of the box |
| **Observability** | Rich event system (CDI + programmatic), Micrometer metrics adapter |
| **Concurrency** | Permit-based backpressure, adaptive polling, resource limiting |
| **CDI Integration** | Zero-ceremony wiring — inject `JobSchedulerService` and go |

## Quick Start

### 1. Add Dependencies

Import the BOM and pick your store:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>run.ratchet</groupId>
      <artifactId>ratchet-bom</artifactId>
      <version>0.1.0-SNAPSHOT</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <!-- Core API -->
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-api</artifactId>
  </dependency>

  <!-- Reference implementation -->
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet</artifactId>
  </dependency>

  <!-- Pick your store -->
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-store-postgresql</artifactId>
  </dependency>

  <!-- Optional: Micrometer metrics -->
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-micrometer</artifactId>
  </dependency>
</dependencies>
```

### 2. Apply the Schema

Ratchet ships DDL as plain SQL files — no Flyway dependency, no migration lock-in. Apply the schema however your project manages DDL:

```bash
# PostgreSQL
psql -d mydb -f ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql

# MySQL
mysql mydb < ratchet-store-mysql/src/main/resources/ddl/mysql-schema.sql
```

### 3. Schedule Your First Job

Inject `JobSchedulerService` into any CDI bean and start scheduling:

```java
@ApplicationScoped
public class OrderService {

    @Inject
    JobSchedulerService scheduler;

    public void placeOrder(Order order) {
        // Fire-and-forget
        scheduler.enqueueNow(() -> processOrder(order.getId()));

        // Delayed execution
        scheduler.schedule(Duration.ofMinutes(30), () -> sendReminder(order.getId()))
            .withPriority(JobPriority.LOW)
            .submit();
    }

    public void processOrder(long orderId) {
        // Your business logic here
        JobContext ctx = JobContext.current();
        ctx.logger().info("Processing order {}", orderId);
    }

    public void sendReminder(long orderId) { /* ... */ }
}
```

## Usage Guide

### Job Chaining and Workflows

Build multi-step workflows with conditional branching:

```java
scheduler.enqueue(() -> validatePayment(orderId))
    .thenOnSuccess(() -> fulfillOrder(orderId))
    .thenOnFailure(() -> notifyPaymentFailure(orderId))
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
    .submit();
```

Advanced workflows can branch on the result of previous steps:

```java
scheduler.enqueue(() -> assessRisk(applicationId))
    .when(result -> result.isSuccess() && result.getValue() < 50,
          () -> autoApprove(applicationId))
    .when(result -> result.isSuccess() && result.getValue() >= 50,
          () -> manualReview(applicationId))
    .thenOnFailure(() -> escalateToManager(applicationId))
    .submit();
```

### Recurring Jobs

Use the `@Recurring` annotation for declarative cron scheduling:

```java
@ApplicationScoped
public class MaintenanceService {

    @Recurring(cron = "0 0 2 * * ?", name = "Nightly Cleanup")
    public void performCleanup() {
        // Runs at 2 AM daily (UTC)
    }

    @Recurring(
        cron = "0 */15 * * * ?",
        zone = "America/New_York",
        priority = 8,
        maxRetries = 5,
        backoffPolicy = BackoffPolicy.EXPONENTIAL,
        tags = {"health", "monitoring"}
    )
    public void healthCheck(JobContext context) {
        context.logger().info("Running health check");
    }
}
```

Or schedule programmatically:

```java
scheduler.scheduleRecurring(
    "0 */5 * * * ?",
    ZoneId.of("UTC"),
    () -> syncExternalData()
).withTags(List.of("sync")).submit();
```

### Batch Processing

Process collections in parallel with progress tracking:

```java
// In-memory batch
scheduler.enqueueBatch("process-invoices")
    .forEach(List.of(invoice1, invoice2, invoice3), inv -> processInvoice(inv))
    .submit();

// Streaming batch for large datasets
scheduler.<Invoice>streamingBatch("import-invoices")
    .fromStream(invoiceStream)
    .process(invoice -> importInvoice(invoice))
    .withChunkSize(100)
    .start();
```

### Circuit Breaker Protection

Protect external service calls with the built-in circuit breaker — no Resilience4j required:

```java
@CircuitBreakerProtected(
    service = "payment-gateway",
    profile = CircuitBreakerProfile.EXTERNAL_API
)
public PaymentResult processPayment(PaymentRequest request) {
    return gateway.charge(request);
}
```

### Event Observation

Monitor job lifecycle via CDI observers or programmatic listeners:

```java
// CDI observer
public void onJobFailed(@Observes JobFailedEvent event) {
    log.error("Job {} failed: {}", event.getJobId(), event.getErrorMessage());
    alerting.notify(event);
}

// Programmatic listener
scheduler.addEventListener(event -> {
    if (event instanceof PerformanceMetricsEvent metrics) {
        dashboard.update(metrics);
    }
});
```

### Job Control

Manage running jobs at runtime:

```java
JobHandle handle = scheduler.enqueue(() -> longRunningTask())
    .withTimeout(Duration.ofMinutes(30))
    .withIdempotencyKey("import-2024-q4")
    .withBusinessKey("quarterly-import")
    .withTags("import", "finance")
    .submit();

long jobId = handle.id();

scheduler.pauseJob(jobId);    // Pause a running job
scheduler.resumeJob(jobId);   // Resume a paused job
scheduler.cancelJob(jobId);   // Cancel a job
scheduler.retryJob(jobId);    // Retry a failed job (resets to PENDING)
```

### Callbacks

Attach success and failure handlers:

```java
scheduler.enqueue(() -> generateReport(reportId))
    .onSuccess(ctx -> notifyUser(ctx.param("email"), "Report ready"))
    .onFailure((ctx, error) -> log.error("Report {} failed", reportId, error))
    .withParam("email", user.getEmail())
    .submit();
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Your Application                     │
│                                                          │
│   @Inject JobSchedulerService    @Recurring methods      │
│            │                          │                   │
├────────────┼──────────────────────────┼───────────────────┤
│            ▼                          ▼                   │
│   ┌─────────────────────────────────────────────┐        │
│   │              ratchet-api                     │        │
│   │  JobSchedulerService, JobBuilder, Events,   │        │
│   │  Annotations, SPI interfaces                │        │
│   └──────────────────┬──────────────────────────┘        │
│                      │                                    │
│   ┌──────────────────▼──────────────────────────┐        │
│   │              ratchet                      │        │
│   │  DefaultJobSchedulerService, Poller,         │        │
│   │  JobTask, CircuitBreaker, RetryEngine,       │        │
│   │  RecurringScheduler, CDI wiring              │        │
│   └──────────────────┬──────────────────────────┘        │
│                      │                                    │
│   ┌──────────────────▼──────────────────────────┐        │
│   │           ratchet-store-core                 │        │
│   │  Entities, 15 SPI sub-interfaces → JobStore  │        │
│   └────┬─────────────┬───────────────────┬──────┘        │
│        ▼             ▼                   ▼               │
│   ┌─────────┐  ┌────────────┐  ┌─────────────┐          │
│   │  MySQL  │  │ PostgreSQL │  │   MongoDB   │          │
│   └─────────┘  └────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────┘
```

### Module Overview

| Module | Purpose | Dependencies |
|--------|---------|-------------|
| `ratchet-api` | Public API, annotations, events, SPI interfaces | Jakarta CDI API only |
| `ratchet` | Core engine — polling, execution, retry, circuit breaker, CDI wiring | ratchet-api, ASM, Jackson, cron-utils |
| `ratchet-store-core` | Persistence abstractions — entities, 15 repository interfaces | ratchet-api, Jakarta Persistence |
| `ratchet-store-mysql` | MySQL store implementation with optimized DDL | ratchet-store-core |
| `ratchet-store-postgresql` | PostgreSQL store with partial indexes and JSONB | ratchet-store-core |
| `ratchet-store-mongodb` | MongoDB document store implementation | ratchet-store-core |
| `ratchet-micrometer` | Micrometer metrics adapter | ratchet-api, Micrometer |
| `ratchet-tck` | Technology Compatibility Kit for custom store implementations | ratchet-store-core, JUnit 5 |
| `ratchet-bom` | Bill of Materials for version alignment | — |

### SPI Extension Points

Ratchet is designed to be extended. Provide a CDI `@Alternative @Priority(APPLICATION)` bean for any of these interfaces:

| SPI Interface | Purpose | Default |
|---------------|---------|---------|
| `RetryPolicy` | Custom retry/no-retry decisions | Defers to `maxRetries` |
| `ResilienceStrategy` | Circuit breaker behavior | Built-in 3-state machine |
| `ClassPolicy` | Security — which classes can be deserialized | `PackagePrefixClassPolicy` with an empty allowlist; startup fails fast until you provide an override |
| `ErrorSanitizer` | Scrub sensitive data from error messages | `DefaultErrorSanitizer` |
| `SerializationStrategy` | Custom payload serialization | `JdkSerializationStrategy` |
| `LambdaAnalyzer` | Method reference extraction from lambdas | ASM bytecode analysis |
| `ClusterCoordinator` | Distributed leader election for recurring jobs | Single-node default |
| `MetricsCollector` | Metrics sink (counters, gauges, timers) | No-op |
| `BeanResolver` | Bean instantiation strategy | CDI `Instance<T>` |
| `ExecutorProvider` | Thread pool / virtual thread configuration | Platform default |
| `NodeIdentityProvider` | Node identification in clusters | Hostname-based |
| `JobLogger` | Per-job structured logging | JUL bridge |

### Custom Store Implementation

Implement the `JobStore` interface (a composition of 15 focused sub-interfaces) and validate your implementation using the TCK:

```java
// In your test module
public class MyCustomStoreTest extends AbstractJobCrudStoreContract {
    @Override
    public JobStore store() {
        return new MyCustomStore(dataSource);
    }

    @Override
    public JobEntity newPendingJob() { /* create a PENDING JobEntity */ }

    @Override
    public JobEntity newBatchParentJob() { /* create a batch parent JobEntity */ }

    @Override
    public void cleanupStore() { /* truncate tables / clear state */ }
}
```

The TCK provides 15 abstract test classes covering CRUD, claiming, status transitions, archiving, execution tracking, batches, locks, and more.

## Production Checklist

Before deploying Ratchet to a production-shaped environment, work through this checklist. Each item is a footgun that has bitten someone, somewhere.

### Required

- [ ] **Configure `ClassPolicy`.** Out of the box, Ratchet ships a deny-all `PackagePrefixClassPolicy()`. The CDI producer **refuses to start** with an empty allowlist (`jakarta.enterprise.inject.spi.DeploymentException`). Provide your own `@Alternative @Priority` bean naming the package prefixes your application uses for job targets:
  ```java
  @Produces
  @Alternative
  @Priority(jakarta.interceptor.Interceptor.Priority.APPLICATION)
  @ApplicationScoped
  public ClassPolicy myClassPolicy() {
    return new PackagePrefixClassPolicy(Set.of("com.acme.jobs."));
  }
  ```
  A hardcoded denylist (`Runtime`, `ProcessBuilder`, `javax.script`, reflection, JDK internals) blocks well-known RCE gadgets regardless of allowlist content. To opt out of the fail-fast guard for demos and tests, set `-Dratchet.allow-empty-class-policy=true`.

- [ ] **Apply the schema.** Ratchet ships DDL as plain SQL files — no Flyway lock-in. Apply once per database before starting any node. See `ratchet-store-{mysql,postgresql}/src/main/resources/ddl/`.

- [ ] **Verify `READ COMMITTED` isolation.** Ratchet's claim/poll path assumes statement-level snapshotting. The default on most servers is correct; verify with `SELECT @@tx_isolation` (MySQL) or `SHOW default_transaction_isolation` (Postgres).

### Multi-node deployments

- [ ] **Provide a real `ClusterCoordinator`.** The default `NoOpClusterCoordinator` returns `isLeader() == true` for every node — safe for **single-node only**. For multi-node, supply an `@Alternative` implementation backed by a database lease, ZooKeeper, or similar leader-election primitive. Without one, every node will run destructive startup actions (recurring job cleanup) and may cancel jobs another node just registered.

- [ ] **Tune `ratchet.recurring.convergence-window-seconds`.** The default 120-second window protects against rolling-deploy races where Node B's startup-time cleanup might cancel jobs Node A just registered. Increase if your rolling-deploy pause is longer than two minutes.

### Operational

- [ ] **Configure a `MeterRegistry`.** Add `ratchet-micrometer` to your classpath for drop-in metrics; Ratchet provides a `SimpleMeterRegistry` by default. For production, override with a real backend (Prometheus, Datadog, OpenTelemetry):
  ```java
  @Produces
  @Alternative
  @Priority(2000)
  @ApplicationScoped
  public MeterRegistry prometheusRegistry() {
    return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
  }
  ```

- [ ] **Plan `scheduler_job_log` retention.** The bundled `LogPurgeTimer` runs `DELETE` on a schedule, which is fine up to ~10M log rows. For high-volume deployments combine purging with **time-range partitioning** of `scheduler_job_log`. See [`docs/ops/partitioning.md`](docs/ops/partitioning.md) for MySQL and PostgreSQL recipes.

- [ ] **Cap job result size if your jobs return large objects.** Default cap is 64KB (`ratchet.jobs.max-result-bytes`); larger results are truncated to a marker JSON noting the original size. Tune via `-D` or store large results out-of-band in object storage.

### Known limitations (0.1.0-alpha)

- **Logging is JUL.** Ratchet currently uses `java.util.logging` throughout. In Jakarta EE containers (WildFly, Open Liberty) this is bridged to the container's logging subsystem. In plain SE / Quarkus / non-EE harnesses, install the `jul-to-slf4j` bridge if you want logs to appear via SLF4J. **Migration to SLF4J is planned for 0.2.**
- **`@Incubating` SPIs may evolve.** Method names, parameters, and semantics on any interface marked `@Incubating` are subject to change between alpha releases.

## Requirements

- **Java**: 17+
- **Jakarta EE**: 10 (Web Profile) — CDI 4.0, JPA 3.1, Interceptors 2.1
- **Runtime**: Any Jakarta EE 10 compatible server (WildFly, Open Liberty, Payara, etc.)
- **Database**: MySQL 8+, PostgreSQL 14+, or MongoDB 6+

## Building from Source

```bash
# Compile
mvn clean compile

# Run unit tests
mvn clean test

# Run unit + integration tests (requires Docker for Testcontainers)
mvn clean verify

# Auto-format code (Google Java Format)
mvn spotless:apply
```

## Project Status

Ratchet is currently in **0.1.0-SNAPSHOT** — the API is stabilizing but interfaces marked `@Incubating` may change. Feedback and contributions are welcome.

## License

Ratchet is licensed under the [Apache License 2.0](./LICENSE).
