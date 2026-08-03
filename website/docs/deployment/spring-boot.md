---
title: Spring Boot Deployment
description: "Run Ratchet with Spring Boot on the JVM, or with PostgreSQL as a GraalVM native image."
---

# Spring Boot Deployment

Ratchet runs as Spring-managed infrastructure on Spring Boot. The integration supplies the runtime
beans and lifecycle, joins the application's persistence unit for SQL stores, and exposes the usual
`JobSchedulerService` for job submission. Choose either the JPA starter plus exactly one SQL store,
or the isolated MongoDB starter.

## PostgreSQL quickstart

This setup uses Spring Boot's datasource and dependency management. Import the Ratchet BOM, add the
JPA starter and PostgreSQL store without versions, and let Spring Boot manage the PostgreSQL JDBC
driver.

**1. Import the BOM.**

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>run.ratchet</groupId>
      <artifactId>ratchet-bom</artifactId>
      <version>0.3.1</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

**2. Add the starter, one store, and the driver.**

```xml
<dependencies>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-spring-boot-starter</artifactId>
  </dependency>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-store-postgresql</artifactId>
  </dependency>
  <dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <scope>runtime</scope>
  </dependency>
</dependencies>
```

The JPA starter declares Spring Boot Data JPA, Ratchet's core and JPA auto-configuration modules,
and Yasson. It deliberately ships no store. Add exactly one `ratchet-store-*` dependency: adding
more than one makes startup fail before the scheduler starts.

**3. Configure the datasource and Ratchet.** Use Spring Boot's normal datasource properties.
`auto-migrate` applies the selected store's bundled schema migrations during startup. The package
allowlist names the application classes Ratchet may invoke.

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/ratchet
spring.datasource.username=ratchet
spring.datasource.password=ratchet

ratchet.schema.auto-migrate=true
ratchet.class-policy.allowed-packages=com.example
```

**4. Submit a job.** Any public Spring bean method can be a job target. Inject
`JobSchedulerService` and submit a bound method reference.

```java
package com.example;

import org.springframework.stereotype.Service;

@Service
public class Reports {
  public void rebuild() {
    // Rebuild the report.
  }
}
```

```java
package com.example;

import org.springframework.stereotype.Service;
import run.ratchet.api.JobSchedulerService;

@Service
public class ReportJobs {
  private final JobSchedulerService scheduler;
  private final Reports reports;

  public ReportJobs(JobSchedulerService scheduler, Reports reports) {
    this.scheduler = scheduler;
    this.reports = reports;
  }

  public void schedule() {
    scheduler.enqueueNow(reports::rebuild);
  }
}
```

The application and Ratchet share one Boot-managed persistence unit. Ratchet's JPA entities join the
application's `EntityManagerFactory`, and the selected `JpaTransactionManager` must own that same
factory. Do not create a second persistence unit for Ratchet.

## MongoDB quickstart

The MongoDB flavor is isolated from JPA. Import the same BOM, then add the MongoDB starter without a
version.

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>run.ratchet</groupId>
      <artifactId>ratchet-bom</artifactId>
      <version>0.3.1</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-spring-boot-starter-mongodb</artifactId>
  </dependency>
</dependencies>
```

`ratchet-spring-boot-starter-mongodb` bundles `ratchet-store-mongodb` and
`mongodb-driver-sync`; do not add separate copies. Point the starter at the MongoDB database and
configure the same job-class allowlist used by the SQL flavor.

```properties
ratchet.mongodb.connection-string=mongodb://localhost:27017
ratchet.mongodb.database=ratchet
ratchet.class-policy.allowed-packages=com.example
```

Inject and use `JobSchedulerService` exactly as in the PostgreSQL quickstart. The MongoDB flavor has
no `EntityManagerFactory`, `JpaTransactionManager`, or schema-migration step.

## SQL database support

Use the matching store dependency and JDBC driver for the database. These minimums are the floors
documented by each Ratchet store.

| Database | Store dependency | Minimum supported version |
|---|---|---|
| PostgreSQL | `ratchet-store-postgresql` | PostgreSQL 14 |
| MySQL | `ratchet-store-mysql` | MySQL 8.0 |
| Oracle | `ratchet-store-oracle` | Oracle Database 23ai |
| SQL Server | `ratchet-store-sqlserver` | SQL Server 2022 |

Every SQL flavor uses the same model: one Boot-managed `EntityManagerFactory` and one selected
`JpaTransactionManager`, with the application's entities and Ratchet's entities in that persistence
unit. See [Spring Boot Configuration](/deployment/spring-boot-configuration) for multiple transaction
manager remediation and the complete property reference.

## Spring Boot and Java compatibility

The JVM compatibility lanes use consumer Java 17.

| Spring Boot | Consumer Java | JVM | GraalVM native image |
|---|---:|---|---|
| 3.5.16 | 17 | Supported | Not supported |
| 4.1.0 | 17 | Supported | PostgreSQL only |

The same shared Ratchet jars run on both JVM lanes; there is no Boot-specific copy of the starter or
auto-configuration jars.

## Spring 7 AOT and native images

Ordinary JVM applications do not add an AOT module. For AOT or native-image builds, add the Spring 7
overlay explicitly alongside the JPA starter:

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-spring-boot-aot-spring7</artifactId>
</dependency>
```

`ratchet-spring-boot-aot-spring7` is for Spring Boot 4.1 and Spring Framework 7 only. GraalVM native
image support is limited to the Boot 4.1 PostgreSQL flavor. Spring Boot 3.5 is JVM-only, and the
MongoDB, MySQL, Oracle, and SQL Server flavors are JVM-only.
