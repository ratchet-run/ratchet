---
title: Quarkus Deployment
description: "Run Ratchet on Quarkus, on the JVM and as a GraalVM native image, with the ratchet-quarkus extension."
---

# Quarkus Deployment

Ratchet runs on Quarkus, on the JVM and as a GraalVM native image, through the `ratchet-quarkus`
extension. The extension wires the engine into Quarkus so you add a few dependencies and a
datasource, then submit jobs through the usual `JobSchedulerService`. Ratchet's own persistence
unit is supplied by the extension, so there is no persistence-unit configuration to write.

## Prerequisites

- Quarkus 3.20 or later
- JDK 21 for JVM mode; a GraalVM or Mandrel distribution for native
- A SQL database for the Hibernate ORM SQL flavor: PostgreSQL 14+, MySQL 8+, Oracle, or SQL Server.
  On this flavor the JPA provider is Hibernate ORM; EclipseLink is a Jakarta EE feature and does not
  apply on Quarkus. For NoSQL, a separate `ratchet-quarkus-mongodb` flavor runs on MongoDB with no
  JPA provider at all (see [MongoDB](#mongodb-flavor) below).

## Dependencies

Add the extension, a store, and the matching Quarkus JDBC driver. The extension brings the engine and
Hibernate ORM with it.

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-quarkus</artifactId>
  <version>0.2.1-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-store-postgresql</artifactId>
  <version>0.2.1-SNAPSHOT</version>
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

Ratchet runs on its own extension-supplied persistence unit, named `ratchet`, so its entities and
schema settings stay out of your application's default unit. Configure your datasource as usual:

```properties
# Your datasource. Ratchet rides on it too.
quarkus.datasource.db-kind=postgresql
quarkus.datasource.username=ratchet
quarkus.datasource.password=ratchet
quarkus.datasource.jdbc.url=jdbc:postgresql://localhost:5432/ratchet
```

The extension binds the `ratchet` unit to the default datasource, scopes it to Ratchet's entities,
sets `database.generation=none`, and keeps Ratchet's `META-INF/orm.xml` off the default unit.

### Applications with their own entities

A named persistence unit turns off Quarkus's automatic default unit, so declare yours explicitly:

```properties
quarkus.hibernate-orm.packages=com.example.myapp
quarkus.hibernate-orm.database.generation=drop-and-create
```

Ratchet's extension defaults keep its mapping file on the `ratchet` unit only, so your default unit
does not need a defensive `mapping-files=no-file` line.

If your application has no entities of its own, skip this step. The `ratchet` unit is the only one.

## Schema

Ratchet does not create its tables. Apply the DDL before the first run:

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

## MongoDB flavor

Everything above describes the SQL flavor. To run Ratchet on MongoDB instead, use the
`ratchet-quarkus-mongodb` artifact and the Mongo store. There is no persistence unit, no `orm.xml`,
and no schema DDL to apply, so the setup is shorter than the SQL flavor.

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-quarkus-mongodb</artifactId>
  <version>0.2.1-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-store-mongodb</artifactId>
  <version>0.2.1-SNAPSHOT</version>
</dependency>
<!-- Same engine dependency as the SQL flavor, until the extension declares it transitively. -->
<dependency>
  <groupId>jakarta.security.enterprise</groupId>
  <artifactId>jakarta.security.enterprise-api</artifactId>
</dependency>
```

The flavor brings `quarkus-mongodb-client` with it. Point it at your database:

```properties
quarkus.mongodb.connection-string=mongodb://localhost:27017
quarkus.mongodb.database=ratchet
```

The extension forces the driver's `UuidRepresentation` to `STANDARD` at construction, so Ratchet's
UUID job identifiers round-trip correctly with no configuration on your part. Submitting jobs, plus
recurring, signals, and batches, is identical to the SQL flavor, and the extension runs on the JVM
and as a native image the same way. See [MongoDB Deployment](/deployment/mongodb) for collection and
index details.

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

- On the SQL flavor the JPA provider is Hibernate ORM; EclipseLink is a Jakarta EE feature and is not
  part of the Quarkus cell. A separate `ratchet-quarkus-mongodb` flavor runs Ratchet on MongoDB with
  no JPA provider at all.
- Ratchet uses its own persistence unit rather than the container's default, as configured above.
- The standalone executor backs job execution instead of a Jakarta Concurrency managed executor.
