---
title: Quarkus Deployment
description: "Run Ratchet on Quarkus, on the JVM and as a GraalVM native image, with the ratchet-quarkus extension."
---

# Quarkus Deployment

Ratchet runs on Quarkus, on the JVM and as a GraalVM native image, through the `ratchet-quarkus`
extension. The extension wires the engine into Quarkus, so you add the extension, a store, and a
datasource, then submit jobs through the usual `JobSchedulerService`. Ratchet's own persistence unit
is supplied by the extension, so there is no persistence-unit configuration to write.

## Quickstart

This is a complete dev application. In dev mode Quarkus provisions a throwaway PostgreSQL container
for you, and Ratchet creates its own schema on startup, so there is nothing to install first. You
need JDK 21 and a running Docker (or Podman) for Dev Services.

**1. Dependencies.** The extension brings the engine and Hibernate ORM with it; add a store and the
matching Quarkus JDBC driver.

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
```

Add `quarkus-rest` too if you want the REST endpoint below. When a request has a Quarkus
`SecurityIdentity`, Ratchet captures that principal automatically at job submission.

**2. Configuration.** Two lines. No datasource URL is set, so Dev Services starts a container in dev
mode. `auto-migrate` tells Ratchet to create its tables on startup.

```properties
quarkus.datasource.db-kind=postgresql
ratchet.schema.auto-migrate=true
```

**3. A job.** Any bean method is a job. Submit a method reference through `JobSchedulerService`.

```java
package com.example;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

@ApplicationScoped
public class Reports {
  private static final Logger LOG = Logger.getLogger(Reports.class);

  public void rebuild() {
    LOG.info("report rebuilt");
  }
}
```

```java
package com.example;

import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import run.ratchet.api.JobSchedulerService;

@Path("/reports")
public class ReportResource {
  @Inject JobSchedulerService scheduler;
  @Inject Reports reports;

  @POST
  public String schedule() {
    scheduler.enqueueNow(reports::rebuild);
    return "submitted";
  }
}
```

**4. Allow your job classes.** Ratchet refuses to run a job whose target class is not on an
allowlist, and it fails startup if no allowlist is configured. Permit your own package with a
`PackagePrefixClassPolicy`:

```java
package com.example;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;
import java.util.Set;
import run.ratchet.ri.security.PackagePrefixClassPolicy;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class JobsClassPolicy extends PackagePrefixClassPolicy {
  public JobsClassPolicy() {
    super(Set.of("com.example"));
  }
}
```

**5. Run it.**

```bash
quarkus dev
# in another shell:
curl -X POST http://localhost:8080/reports
```

The log shows Dev Services starting PostgreSQL, Ratchet applying its migrations, then `report
rebuilt` when the job runs.

## How the dev setup works

- **Dev Services** provisions a throwaway PostgreSQL container whenever no datasource URL is
  configured in dev and test mode. Nothing to install, and the container is discarded when you stop.
- **Auto-migration** runs Ratchet's bundled schema migrations on startup when
  `ratchet.schema.auto-migrate=true`. This is a convenience for development, CI, and embedded
  deployments; see [Schema](#schema) for the production story. It is a plain `ratchet.*` property, so
  it reads from `application.properties` with no environment variable required.
- **The class allowlist** is mandatory. The `PackagePrefixClassPolicy` above names the packages whose
  methods Ratchet will run. Without one the application fails fast at startup rather than silently
  rejecting every job.
- **The persistence unit** named `ratchet` is supplied by the extension and kept separate from your
  application's default unit, so you write no persistence-unit configuration and an application with
  no entities of its own still boots.

On Docker Engine 29 or newer, Dev Services may fail to negotiate the Docker API version. If you see a
`client version ... is too old` error, create `~/.docker-java.properties` with a single line
`api.version=1.44`.

## Prerequisites

- Quarkus 3.20 or later
- JDK 21 for JVM mode; a GraalVM or Mandrel distribution for native
- A SQL database for the Hibernate ORM SQL flavor: PostgreSQL 14+, MySQL 8+, Oracle, or SQL Server.
  On this flavor the JPA provider is Hibernate ORM; EclipseLink is a Jakarta EE feature and does not
  apply on Quarkus. For NoSQL, a separate `ratchet-quarkus-mongodb` flavor runs on MongoDB with no
  JPA provider at all (see [MongoDB](#mongodb-flavor) below).

For MySQL, swap in `ratchet-store-mysql` and `quarkus-jdbc-mysql`. Oracle and SQL Server work the same
way with their own store and driver artifacts.

## Moving to production

**Datasource.** Point the default datasource at your real database instead of Dev Services:

```properties
quarkus.datasource.db-kind=postgresql
quarkus.datasource.username=ratchet
quarkus.datasource.password=ratchet
quarkus.datasource.jdbc.url=jdbc:postgresql://db:5432/ratchet
```

The extension binds the `ratchet` unit to this datasource, scopes it to Ratchet's entities, sets
`database.generation=none`, and keeps Ratchet's `META-INF/orm.xml` off the default unit.

**Applications with their own entities.** A named persistence unit turns off Quarkus's automatic
default unit, so declare yours explicitly:

```properties
quarkus.hibernate-orm.packages=com.example.myapp
quarkus.hibernate-orm.database.generation=drop-and-create
```

Ratchet's extension defaults keep its mapping file on the `ratchet` unit only, so your default unit
does not need a defensive `mapping-files=no-file` line. If your application has no entities of its
own, skip this step; the `ratchet` unit is the only one, and the application boots without a default
unit.

## Schema

For development and embedded use, `ratchet.schema.auto-migrate=true` applies Ratchet's bundled
migrations on startup, as in the [Quickstart](#quickstart). The migrations ship with the store and
run in native images as well.

For production, many teams prefer to apply schema changes out of band with their own tooling. Ratchet
ships the DDL as plain SQL, so you can run it with Flyway, Liquibase, `psql`, or a container init
script and leave `auto-migrate` off:

```bash
psql -U ratchet -d ratchet -f ddl/postgresql-schema.sql
```

See [PostgreSQL Deployment](/deployment/postgresql) or [MySQL Deployment](/deployment/mysql) for the
schema details.

## Submitting jobs

Inject `JobSchedulerService` and submit a method reference, as in the [Quickstart](#quickstart).
Recurring jobs, signals, and batches work the same as on a Jakarta EE server. Every job target class
must be permitted by the class allowlist.

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
```

The flavor brings `quarkus-mongodb-client` with it. Point it at your database:

```properties
quarkus.mongodb.connection-string=mongodb://localhost:27017
quarkus.mongodb.database=ratchet
```

Mongo initializes its own collections and indexes, so there is no `auto-migrate` step. The extension
forces the driver's `UuidRepresentation` to `STANDARD` at construction, so Ratchet's UUID job
identifiers round-trip correctly with no configuration on your part. Submitting jobs, plus recurring,
signals, and batches, is identical to the SQL flavor, and the extension runs on the JVM and as a
native image the same way. See [MongoDB Deployment](/deployment/mongodb) for collection and index
details.

## Native image

```bash
mvn package -Pnative -Dquarkus.native.container-build=false
```

Build on a host GraalVM or Mandrel. The extension registers the reflection, runtime-init, and
lambda-serialization metadata the engine needs, and it includes the schema migrations in the image,
so method-reference jobs and `auto-migrate` both work in native. Inline lambdas such as `() ->
svc.work(arg)` are JVM-only; for a job you want to run in native, pass a method reference or a bean
method instead.

## What the extension handles

- Starts the engine on Quarkus's `StartupEvent`, after persistence is ready. Ratchet's Jakarta EE
  auto-start fires at static-init, which is too early on Quarkus.
- Supplies the JNDI-free `StandaloneExecutorProvider`, since Quarkus has no Jakarta Concurrency
  managed executor.
- Supplies the `ratchet` persistence unit's build-time settings and binds the stores to it.
- Discovers the schema-migration hook and the active store's dialect so `auto-migrate` works, and
  includes the migration scripts in native images.
- Excludes the store's default `EntityManagerProvider`, which the Quarkus provider supersedes, so an
  application with no entities of its own still boots.
- Turns off dev-mode Hibernate validation of the `ratchet` unit, whose entities are a denormalized
  view served by native row mappers rather than a literal image of the physical schema.
- Supplies a caller-principal source backed by Quarkus `SecurityIdentity`, so a job submitted during
  an authenticated request records that caller. It reads the identity defensively, so submitting from
  a background thread with no active request simply records no caller instead of failing.
- Keeps Ratchet's beans from being pruned by ArC and registers the native metadata.

## Differences from a Jakarta EE server

- On the SQL flavor the JPA provider is Hibernate ORM; EclipseLink is a Jakarta EE feature and is not
  part of the Quarkus cell. A separate `ratchet-quarkus-mongodb` flavor runs Ratchet on MongoDB with
  no JPA provider at all.
- Ratchet uses its own persistence unit rather than the container's default, as configured above.
- The standalone executor backs job execution instead of a Jakarta Concurrency managed executor.
