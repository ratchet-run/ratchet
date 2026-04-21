---
title: Installation & Setup
---

# Installation & Setup

Getting Ratchet running in your application.

## Prerequisites

- **Java**: 17 or later
- **Jakarta EE**: 10 with CDI, JPA, Interceptors, and Jakarta Concurrency for the default RI runtime
- **Database**: MySQL 8+, PostgreSQL 14+, or MongoDB 6+
- **Maven**: 3.8+

## Step 1: Add Dependencies

### BOM Import

Add the Ratchet BOM to your `dependencyManagement`:

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

### Core Dependencies

Add to your `dependencies`:

```xml
<!-- API and reference implementation -->
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-api</artifactId>
</dependency>

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
```

## Step 2: Apply the Database Schema

Ratchet ships DDL as plain SQL files. No Flyway dependency required.

### PostgreSQL

```bash
# From ratchet-store-postgresql/src/main/resources/ddl/
psql -U ratchet -d mydb -f postgresql-schema.sql
```

Or copy into your migration tool (Liquibase, Flyway, etc.):

```bash
# Copy DDL into Flyway migration directory
cp postgresql-schema.sql src/main/resources/db/migration/V1__ratchet_schema.sql
flyway migrate
```

### MySQL

```bash
mysql -u ratchet -p mydb < mysql-schema.sql
```

:::caution
MySQL requires `READ COMMITTED` isolation. Set `transaction-isolation=TRANSACTION_READ_COMMITTED` on your DataSource or append `?sessionVariables=transaction_isolation='READ-COMMITTED'` to the JDBC URL.
:::

### MongoDB

MongoDB collections and indexes are created automatically by the store module on startup. No manual schema application is needed.

## Step 3: Configure CDI Wiring

Ratchet integrates via CDI. Create or update your `beans.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans
    xmlns="https://jakarta.ee/xml/ns/jakartaee"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="https://jakarta.ee/xml/ns/jakartaee
                        https://jakarta.ee/xml/ns/jakartaee/beans_4_0.xsd"
    version="4.0"
    bean-discovery-mode="annotated">
</beans>
```

## Step 4: Inject JobSchedulerService

In any CDI bean, inject and use:

```java
@ApplicationScoped
public class MyService {
  
  @Inject
  JobSchedulerService scheduler;
  
  public void scheduleWork() {
    scheduler.enqueueNow(() -> doWork());
  }
  
  public void doWork() {
    System.out.println("Running in Ratchet!");
  }
}
```

## Step 5: Configuration

Produce `RatchetOptions` for runtime tuning. If no options bean exists, Ratchet can fall back to CDI `RatchetConfigSource`, MicroProfile Config, environment variables, system properties, and built-in defaults. A few common knobs:

| Setting | Default | Purpose |
|---------|---------|---------|
| `polling.batchSize(...)` | `50` | Jobs claimed per poll cycle |
| `polling.minDelayMs(...)` | `2000` | Minimum poll interval |
| `execution.maxConcurrency("SINGLE", ...)` | `20` | Worker threads for one-off jobs |
| `maintenance.jobRetentionDays(...)` | `90` | Completed-job retention before archiving |

SPI customizations such as `ClassPolicy`, `JobInvocationResolver`, `ResultPersistenceStrategy`, `ExecutionTuningProvider`, `ExecutorProvider`, `RatchetEntityManagerProvider`, and `ErrorSanitizer` are overridden with CDI `@Alternative` beans, not string property names. See [Configuration](/docs/getting-started/configuration).

## Step 6: Verify

Start your application and verify Ratchet initialized:

```
INFO [run.ratchet.ri.cdi.RatchetLifecycle] Ratchet starting
INFO [run.ratchet.ri.core.DefaultNodeIdentityProvider] Scheduler nodeId=...
INFO [run.ratchet.ri.core.Poller] Poller initialized (batch=50)
INFO [run.ratchet.ri.cdi.RatchetLifecycle] Ratchet started
```

Submit a simple job:

```java
scheduler.enqueueNow(() -> System.out.println("It works!"));
```

Check your database:

```sql
SELECT COUNT(*) FROM scheduler_job;
```

## Next Steps

- [Getting Started](/docs/getting-started/introduction)
- [Configuration](/docs/deployment/configuration)
- [Concepts](/docs/concepts/overview)

## Troubleshooting

**JobSchedulerService injection fails**
- Ensure `beans.xml` exists and is in `src/main/resources/META-INF/`
- Verify your runtime supports CDI (WildFly, Open Liberty, Payara)

**Database connection error**
- Check data source configuration
- Verify schema was applied

**Jobs not executing**
- Check for `Ratchet started` and `Poller initialized (...)` in the logs
- Verify executor thread count > 0
