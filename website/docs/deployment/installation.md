---
title: Runtime setup
description: Configure CDI, runtime options, ClassPolicy, and container resources after adding Ratchet dependencies
---

# Runtime setup

This page starts after Ratchet is on your classpath and its database schema is available. For Maven coordinates, module selection, and released-version guidance, use the canonical [Installation guide](/getting-started/installation). Keeping dependency setup in one place prevents the two deployment paths from drifting apart.

## Prerequisites

- Java 17 or later
- A Jakarta EE 10 or 11 runtime with CDI, Persistence, Interceptors, and Jakarta Concurrency
- One supported store: MySQL 8+, PostgreSQL 14+, Oracle 23ai+, SQL Server 2022+, or MongoDB 6+
- The selected store's resources and schema, as described in [Database setup](/deployment/database-setup)

## 1. Enable CDI discovery

Ratchet's beans use annotated discovery. CDI 4 normally discovers them without a descriptor. If your deployment uses `beans.xml`, keep discovery mode at `annotated` or `all`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="https://jakarta.ee/xml/ns/jakartaee"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="https://jakarta.ee/xml/ns/jakartaee
                           https://jakarta.ee/xml/ns/jakartaee/beans_4_0.xsd"
       version="4.0"
       bean-discovery-mode="annotated">
</beans>
```

Place the descriptor in `WEB-INF/beans.xml` for a WAR or `META-INF/beans.xml` for a bean archive.

## 2. Produce RatchetOptions

Every running Ratchet deployment must produce exactly one unqualified, application-scoped `RatchetOptions` bean. Without it, CDI fails deployment with `UnsatisfiedResolutionException`; Ratchet does not silently start with ambient defaults.

The smallest environment-driven producer is:

```java
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;

@ApplicationScoped
public class RatchetConfiguration {

    @Produces
    @ApplicationScoped
    RatchetOptions ratchetOptions() {
        return RatchetOptionsFactory.fromEnvironment();
    }
}
```

Use the [Configuration guide](/getting-started/configuration) for a programmatic producer and the [Configuration reference](/deployment/configuration-reference) for every property, environment variable, and default.

## 3. Provide a ClassPolicy

Ratchet also refuses to start while the default `ClassPolicy` allowlist is empty. Jobs execute application methods reconstructed from durable payloads, so the allowlist is a required security boundary rather than optional hardening.

Install a CDI alternative that admits only your job-target packages:

```java
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;
import run.ratchet.spi.ClassPolicy;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class ApplicationClassPolicy implements ClassPolicy {

    @Override
    public boolean isAllowed(String className) {
        return className.startsWith("com.example.jobs.");
    }
}
```

Do not allow broad namespaces such as `java.`, `jakarta.`, or an entire organization's root package. Include each package that contains a scheduled target. Typed workflow-result deserialization uses the separate, narrower `isAllowedForResultType` check, which defaults to deny.

For demos and tests only, `RatchetOptions.builder().security(s -> s.allowEmptyClassPolicy(true)).build()` bypasses the startup guard. It does not make the default policy permissive: jobs remain rejected until a real allowlist is installed.

## 4. Configure store and concurrency resources

SQL stores need the selected Jakarta Persistence unit and datasource; MongoDB needs a `MongoDatabase` using `UuidRepresentation.STANDARD`. The database-specific pages cover provider and isolation requirements.

By default, Ratchet looks up the Jakarta Concurrency resources below:

| Purpose | Default JNDI name | Configuration key |
|---|---|---|
| Job execution | `java:comp/DefaultManagedExecutorService` | `ratchet.worker.job-executor-jndi` |
| Scheduled maintenance | `java:comp/DefaultManagedScheduledExecutorService` | `ratchet.worker.scheduled-executor-jndi` |
| Coordinator threads | `java:comp/DefaultManagedThreadFactory` | `ratchet.coordinator.thread-factory-jndi` |

Override these only when the application server binds managed resources under different names. Ratchet never creates unmanaged worker threads as a fallback.

## 5. Verify startup

Deploy the application and confirm all four stages appear without a ClassPolicy or CDI error:

```text
INFO  Ratchet starting
INFO  Scheduler nodeId=...
INFO  Poller initialized (batch=50)
INFO  Ratchet started
```

Then submit one job and verify that its cold row exists:

```java
scheduler.enqueueNow(() -> workService.runOnce());
```

```sql
SELECT job_id, terminal_status, created_at
FROM scheduler_job
ORDER BY created_at DESC;
```

## Troubleshooting

- `RatchetOptions` is unsatisfied: add the producer above and ensure its method is `@ApplicationScoped`.
- `ClassPolicy allowedPackages is empty`: install the application alternative; do not use the demo escape hatch in production.
- `JobSchedulerService` is unsatisfied: verify CDI discovery and that `ratchet` is packaged with exactly one store implementation.
- Managed executor lookup fails: compare the server's JNDI bindings with the three keys above.
- Store startup fails: follow the selected database page and verify schema, datasource, transaction isolation, and UUID mapping.

## Next steps

- [Configuration](/getting-started/configuration)
- [Database setup](/deployment/database-setup)
- [Clustering](/deployment/clustering)
- [Monitoring](/deployment/monitoring)
