---
sidebar_position: 2
title: Installation
description: Add Ratchet to your Maven project with the BOM, choose your store module, and apply the database schema
---

# Installation

Ratchet is distributed as a set of Maven modules. You pick the modules you need, import the BOM for version alignment, and apply the database schema. This page walks through each step.

## Maven BOM setup

The Bill of Materials (BOM) ensures all Ratchet modules use the same version. Import it in your `<dependencyManagement>` section:

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
```

With the BOM imported, you can declare Ratchet dependencies without specifying versions:

```xml
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

## Module overview

Ratchet is split into focused modules so you only pull in what you need.

### Required modules

| Module | Purpose | You need it when... |
|--------|---------|---------------------|
| `ratchet-api` | Public API: `JobSchedulerService`, `JobBuilder`, annotations (`@Recurring`, `@CircuitBreakerProtected`), events, and SPI interfaces | Always. This is the module you code against. |
| `ratchet` | Reference implementation: poller, execution engine, CDI producers, retry logic, circuit breaker, lambda serialization | Always (unless you're writing your own implementation of the API). |
| `ratchet-store-core` | Shared persistence abstractions: entity classes and the composed `JobStore` SPI | Always. Pulled in transitively by any store module. |

### Store modules

You need exactly one store module matching your database:

| Module | Database | Notes |
|--------|----------|-------|
| `ratchet-store-postgresql` | PostgreSQL 14+ | Uses JSONB columns, partial indexes, and `FOR UPDATE SKIP LOCKED` for efficient job claiming |
| `ratchet-store-mysql` | MySQL 8+ | Uses JSON columns and `SELECT ... FOR UPDATE SKIP LOCKED` |
| `ratchet-store-mongodb` | MongoDB 6+ | Document-based store with TTL indexes |

### Optional modules

| Module | Purpose | You need it when... |
|--------|---------|---------------------|
| `ratchet-micrometer` | Micrometer metrics adapter implementing `MetricsCollector` SPI | You want to export scheduler metrics to Prometheus, Datadog, or any Micrometer-supported backend |
| `ratchet-tck-store` | Store SPI conformance contracts (CRUD, claiming, archiving, batches, locks). | You're validating a custom `JobStore` for "Ratchet Store Compatible". |
| `ratchet-tck-api` | Public-API conformance contracts (submit / cancel / retry / idempotency / workflow / delayed scheduling). Container-free, pure-JVM JUnit. | You're validating a custom `JobSchedulerService` for "Ratchet API Compatible". |
| `ratchet-tck-jakarta` | Jakarta-EE conformance contracts (CDI injection, CDI events, JTA enqueue) driven by Arquillian. | You're validating a runtime for "Ratchet Jakarta Runtime Compatible". |

## Choosing your modules

### Minimal setup

For most applications, you need three dependencies: the API, the reference implementation, and your store:

```xml
<dependencies>
  <!-- The API you code against -->
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-api</artifactId>
  </dependency>

  <!-- The engine that runs your jobs -->
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet</artifactId>
  </dependency>

  <!-- Your database store (pick one) -->
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-store-postgresql</artifactId>
  </dependency>
</dependencies>
```

:::tip Why three modules?
Ratchet separates the API from the implementation so that modules in your application that only *submit* jobs can depend on `ratchet-api` alone, without pulling in the execution engine. This is useful in multi-module projects where a shared library needs to enqueue jobs but doesn't run the scheduler.
:::

### With metrics

Add `ratchet-micrometer` to export scheduler metrics (job counts, execution times, retry rates, circuit breaker state) to your monitoring stack:

```xml
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
    <artifactId>ratchet-store-mysql</artifactId>
  </dependency>
  <!-- Metrics export -->
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-micrometer</artifactId>
  </dependency>
</dependencies>
```

### API-only (shared library)

If you have a shared library that needs to define job signatures but doesn't run the scheduler itself:

```xml
<dependencies>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-api</artifactId>
  </dependency>
</dependencies>
```

This gives you access to `JobSchedulerService`, `@Recurring`, `JobBuilder`, and all event types -- enough to write code that submits and observes jobs. The deployment that actually runs the scheduler adds `ratchet` and a store module.

## Transitive dependencies

Ratchet keeps its dependency footprint small:

| Module | Key Dependencies |
|--------|-----------------|
| `ratchet-api` | `jakarta.enterprise.cdi-api` (for `@InterceptorBinding` on `@CircuitBreakerProtected`) |
| `ratchet` | ASM 9.8 (lambda bytecode analysis), cron-utils 9.2.1 (cron expression parsing), JBoss Logging; Jakarta EE APIs are provided by the runtime |
| `ratchet-store-core` | Jakarta Persistence / JSON APIs, ASM, JBoss Logging |
| `ratchet-store-mysql` / `ratchet-store-postgresql` | SQL store logic; JDBC drivers are supplied by the application server or your deployment |
| `ratchet-store-mongodb` | MongoDB sync driver |
| `ratchet-micrometer` | Micrometer Core 1.14 |

Jakarta EE APIs are declared with `provided` scope, since your Jakarta EE 10/11 application server supplies them at runtime.

## Database schema setup

SQL stores ship DDL as plain SQL files -- no Flyway dependency, no migration framework lock-in. You apply the schema using whatever mechanism your project already uses for DDL management. MongoDB collections and indexes are initialized by `ratchet-store-mongodb` at startup.

The SQL files are located in each SQL store module's resources:

```
stores/ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql
stores/ratchet-store-mysql/src/main/resources/ddl/mysql-schema.sql
```

### Applying the schema

**Command line:**

```bash
# PostgreSQL
psql -d mydb -f stores/ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql

# MySQL
mysql mydb < stores/ratchet-store-mysql/src/main/resources/ddl/mysql-schema.sql
```

**Flyway (if you already use it):**

Copy the DDL file into your Flyway migrations directory with an appropriate version number:

```
src/main/resources/db/migration/V10__ratchet_schema.sql
```

**Liquibase:**

Reference the DDL file as a `sqlFile` changeset in your changelog.

:::info Why plain SQL?
This is a deliberate design decision. Other schedulers (like Quartz) bundle their own migration framework, which creates conflicts when your application already manages schema migrations with Flyway or Liquibase. By shipping raw DDL, Ratchet integrates with whatever migration strategy you've already chosen -- or none at all, if you apply schema changes manually.
:::

### Store-specific UUID wiring

Ratchet job IDs are UUIDv7 values. PostgreSQL stores them as native `uuid`.
MySQL stores them as `BINARY(16)` and production persistence units must include
the MySQL mapping file:

```xml
<mapping-file>META-INF/orm-mysql.xml</mapping-file>
```

That mapping applies the MySQL store-local `UuidByteArrayConverter` for
non-Hibernate JPA providers. PostgreSQL does not use this mapping file.
MongoDB clients must use `UuidRepresentation.STANDARD`; prefer
`MongoClientFactory.create(...)` when creating the client.

## Jakarta EE server compatibility

Ratchet targets Jakarta EE 10/11 runtimes with the services used by the reference implementation. The
default runtime requirements are:

- **CDI 4.0/4.1** -- for dependency injection, bean discovery, and event observation
- **JPA 3.1/3.2** -- for the store implementations' entity mapping
- **Interceptors 2.1/2.2** -- for `@CircuitBreakerProtected` support
- **Jakarta Concurrency 3.0/3.1** -- for the default managed executor provider

Servers known to work:

| Server | Version | Notes |
|--------|---------|-------|
| WildFly | Jakarta EE 10 line; CI-managed tests currently use 39.0.1.Final | Primary managed test target; an EE 11 line (WildFly 40.x) is also CI-verified |
| Open Liberty | Jakarta EE 10 `webProfile-10.0`; CI-managed tests currently use 26.0.0.2 | Requires CDI, Persistence, and managed executor support |
| Payara | Payara 6 line; CI-managed tests currently use 6.2025.11 | Jakarta EE 10 runtime |
| GlassFish | GlassFish 8 / Omnifish line | Jakarta EE 11 verification profile; currently tracked with a known upstream workaround |

Plain Web Profile or standalone CDI environments can opt into
`run.ratchet.ri.cdi.StandaloneExecutorProvider` as an alternative
`ExecutorProvider`. That fallback owns unmanaged executors and is intended for tests, demos, and
non-container deployments.

## What's next

With your dependencies in place and the schema applied, you're ready to write your first job:

- [Quick Start](./quickstart.md) -- Minimal working example in under 5 minutes
- [Your First Job](./first-job.md) -- Complete walkthrough with retries, callbacks, and monitoring
