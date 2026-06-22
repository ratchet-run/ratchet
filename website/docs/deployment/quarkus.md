---
title: Quarkus Deployment
description: "Run Ratchet on Quarkus, on the JVM and as a GraalVM native image, with the ratchet-quarkus extension."
---

# Quarkus Deployment

Ratchet runs on Quarkus, on the JVM and as a GraalVM native image, through the `ratchet-quarkus`
extension. The extension wires the engine into Quarkus so you add a few dependencies and some
persistence config, then submit jobs through the usual `JobSchedulerService`.

## Prerequisites

- Quarkus 3.20 or later
- JDK 21 for JVM mode; a GraalVM or Mandrel distribution for native
- PostgreSQL 14+ or MySQL 8+. This is the Hibernate ORM cell; EclipseLink is a Jakarta EE feature and
  does not apply on Quarkus.

## Dependencies

Add the extension, a store, and the matching Quarkus JDBC driver. The extension brings the engine and
Hibernate ORM with it.

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-quarkus</artifactId>
  <version>0.1.1-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-store-postgresql</artifactId>
  <version>0.1.1-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-jdbc-postgresql</artifactId>
</dependency>
<!-- The engine references jakarta.security.enterprise.SecurityContext. -->
<dependency>
  <groupId>jakarta.security.enterprise</groupId>
  <artifactId>jakarta.security.enterprise-api</artifactId>
</dependency>
```

For MySQL, swap in `ratchet-store-mysql` and `quarkus-jdbc-mysql`.

## Configuration

Ratchet runs on its own persistence unit, named `ratchet`, so its entities and schema settings stay
out of your application's default unit. Put this in `application.properties`:

```properties
# Your datasource. Ratchet rides on it too.
quarkus.datasource.db-kind=postgresql
quarkus.datasource.username=ratchet
quarkus.datasource.password=ratchet
quarkus.datasource.jdbc.url=jdbc:postgresql://localhost:5432/ratchet

# Ratchet's persistence unit.
quarkus.hibernate-orm."ratchet".packages=run.ratchet.store.entity
quarkus.hibernate-orm."ratchet".datasource=<default>
quarkus.hibernate-orm."ratchet".database.generation=none
```

Two of those lines trip people up:

- `datasource=<default>` is required. A named persistence unit does not pick up the default
  datasource on its own, and the build fails without it. `<default>` is the literal token for the
  default datasource; name a different datasource here to put Ratchet on a separate database.
- `database.generation=none` because Ratchet ships its own schema (see [Schema](#schema)). Apply the
  DDL yourself and Hibernate leaves the tables alone.

### Applications with their own entities

A named persistence unit turns off Quarkus's automatic default unit, so declare yours and keep
Ratchet's mapping file out of it:

```properties
quarkus.hibernate-orm.packages=com.example.myapp
quarkus.hibernate-orm.mapping-files=no-file
```

`mapping-files=no-file` is the line that matters. Ratchet ships a `META-INF/orm.xml`, and Quarkus
attaches it to every persistence unit by default. Without `no-file`, your default unit also loads
Ratchet's entities, and a unit set to `drop-and-create` will drop Ratchet's tables. `no-file` keeps
that mapping on Ratchet's unit alone.

If your application has no entities of its own, skip this step. The `ratchet` unit is the only one.

## Schema

Ratchet does not create its tables, which is what `generation=none` means above. Apply the DDL before
the first run:

```bash
psql -U ratchet -d ratchet -f stores/ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql
```

Flyway, Liquibase, or any migration tool works too. See [PostgreSQL Deployment](/deployment/postgresql)
or [MySQL Deployment](/deployment/mysql) for the schema details.

## Submitting jobs

Inject `JobSchedulerService` and submit a method reference:

```java
@ApplicationScoped
public class Reports {
  public void rebuild() { /* ... */ }
}

@Path("/reports")
public class ReportResource {
  @Inject JobSchedulerService scheduler;
  @Inject Reports reports;

  @POST
  public void schedule() {
    scheduler.enqueueNow(reports::rebuild);
  }
}
```

Recurring jobs, signals, and batches work the same as on a Jakarta EE server.

## Native image

```bash
mvn package -Pnative -Dquarkus.native.container-build=false
```

Build on a host GraalVM or Mandrel. The extension registers the reflection, runtime-init, and
lambda-serialization metadata the engine needs, so method-reference jobs run in native. Inline
lambdas such as `() -> svc.work(arg)` are JVM-only; for a job you want to run in native, pass a method
reference or a bean method instead.

## What the extension handles

- Starts the engine on Quarkus's `StartupEvent`, after persistence is ready. Ratchet's Jakarta EE
  auto-start fires at static-init, which is too early on Quarkus.
- Supplies the JNDI-free `StandaloneExecutorProvider`, since Quarkus has no Jakarta Concurrency
  managed executor.
- Binds the stores to the `ratchet` persistence unit.
- Keeps Ratchet's beans from being pruned by ArC and registers the native metadata.

## Differences from a Jakarta EE server

- Hibernate ORM only. EclipseLink is a Jakarta EE feature and is not part of the Quarkus cell.
- Ratchet uses its own persistence unit rather than the container's default, as configured above.
- The standalone executor backs job execution instead of a Jakarta Concurrency managed executor.
