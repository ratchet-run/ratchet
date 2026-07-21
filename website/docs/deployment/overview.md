---
sidebar_position: 1
title: Deployment Overview
description: "What you need to deploy Ratchet: application server, database, modules, and configuration."
---

# Deployment Overview

Ratchet is a portable, CDI-based job scheduler for Jakarta EE 10/11. It deploys as a set of JAR modules inside your application, running on Jakarta EE runtimes with the services used by the reference implementation.

## What you need

| Component | Requirement | Notes |
|-----------|-------------|-------|
| **Java** | 17 or later | Virtual threads available on 21+ |
| **Jakarta EE Runtime** | 10/11 with CDI, JPA, Interceptors, and Jakarta Concurrency | WildFly, Payara, Open Liberty, GlassFish 8, etc. |
| **CDI** | 4.0+ | `beans.xml` with `bean-discovery-mode="annotated"` |
| **Database** | MySQL 8+, PostgreSQL 14+, Oracle 23ai+, SQL Server 2022+, or MongoDB 6+ | One store module per database |
| **Build Tool** | Maven 3.8+ | BOM import for version management |

## Ratchet modules

A typical deployment includes three Ratchet JARs:

```
ratchet-api          Public API, events, enums, SPI interfaces (Jakarta EE APIs only)
ratchet           Reference implementation — core engine, CDI integration, polling
ratchet-store-*      One of: ratchet-store-mysql, ratchet-store-postgresql, ratchet-store-oracle, ratchet-store-mongodb
```

Optional modules:

```
ratchet-micrometer   Micrometer metrics integration
```

All versions are managed through the `ratchet-bom`:

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
```

## Deployment scenarios

### Single-node

The simplest deployment: one application instance connected to one database. No clustering configuration needed. Ratchet's polling engine runs inside the application server and executes jobs using its managed thread pool.

This is suitable for:
- Development and testing
- Low-throughput workloads (< 1,000 jobs/hour)
- Applications where high availability is not critical

### Multi-node (clustered)

Multiple application instances share the same database. Ratchet uses database-level claiming (`SELECT ... FOR UPDATE SKIP LOCKED` on PostgreSQL/MySQL/Oracle, `UPDLOCK, READPAST` row locks on SQL Server, atomic document updates on MongoDB) to ensure each job is claimed by exactly one node.

Recurring scans and destructive startup cleanup are already serialized through store-backed locks and leases. Implement `ClusterCoordinator` only if you want low-latency cross-node wakeups.

See [Cluster Configuration](/deployment/cluster-configuration) for details.

### Containerized

Ratchet runs in Docker or Kubernetes without any special configuration beyond what a standard Jakarta EE application needs. The database runs as a separate container or managed service.

See [Docker Deployment](/deployment/docker) and [Kubernetes Deployment](/deployment/kubernetes).

## Database schema

Ratchet ships SQL DDL as plain files. No Flyway or Liquibase dependency is required. The schema files are bundled inside each SQL store module JAR at `ddl/`:

- `ratchet-store-mysql` contains `ddl/mysql-schema.sql`
- `ratchet-store-postgresql` contains `ddl/postgresql-schema.sql`
- `ratchet-store-mongodb` initializes collections and indexes at startup

For SQL stores, apply the schema using whatever mechanism your team prefers: CLI tools, migration frameworks, or application startup scripts. MongoDB bootstraps collections and indexes automatically. See [Database Setup](/deployment/database-setup) for step-by-step instructions.

### Core tables and collections

The SQL schema creates these primary tables. MongoDB uses analogous collections created by the store module.

| Table | Purpose |
|-------|---------|
| `ratchet_schema_version` | SQL schema migration/checksum tracking |
| `scheduler_job` | Cold job metadata, payload, and terminal state |
| `scheduler_business_key_reservation` | Active business-key reservation guard |
| `scheduler_job_queue` | Hot executable queue table for claim/poll state |
| `scheduler_job_tag` | Tags for job categorization and querying |
| `scheduler_job_execution` | Per-attempt execution history with timing and errors |
| `scheduler_job_log` | Optional per-job log entries if your application persists `JobLogLine` events |
| `scheduler_batch` | Batch progress tracking |
| `scheduler_batch_metrics` | Batch performance metrics |
| `scheduler_job_archive` | Archived completed/failed jobs |
| `scheduler_recurring_job` | Recurring job masters |
| `scheduler_recurring_job_archive` | Archived recurring job masters |
| `scheduler_node` | Cluster node heartbeats |
| `scheduler_lock` | Distributed lock management |
| `scheduler_resource_limit` | Resource concurrency configuration |
| `scheduler_resource_permit` | Active resource permits for concurrency control |
| `scheduler_workflow_condition` | Workflow branching conditions |

## Configuration

Ratchet **requires** a CDI-produced `RatchetOptions` bean. If no producer is found, CDI fails deployment with `UnsatisfiedResolutionException` and the scheduler never starts. This acts as a first-class kill-switch for any deployment that includes `ratchet` without wanting it active.

The producer may build options programmatically or read `RATCHET_*` environment variables and MicroProfile Config via `RatchetOptionsFactory.fromEnvironment()`. See [Configuration](/getting-started/configuration) for both patterns.

Key configuration areas:

| Area | RatchetOptions path | Default |
|------|---------------------|---------|
| **Thread pool** | `execution.maxConcurrency("SINGLE", ...)` | `20` |
| **Polling** | `polling.minDelayMs(...)` | `2000` |
| **Batch size** | `polling.batchSize(...)` | `50` |
| **Job retention** | `maintenance.jobRetentionDays(...)` | `90` |
| **Clustering / node health** | `node.heartbeatIntervalSeconds(...)` | `10` |

See [Configuration](/deployment/configuration) for the full reference.

## Monitoring and observability

Ratchet provides multiple monitoring integration points:

- **Event system:** CDI events for job lifecycle (started, completed, failed, DLQ)
- **MetricsCollector SPI:** plug in Micrometer or any custom metrics backend
- **MicroProfile Health:** implement health checks against the job store
- **Store queries:** direct SQL queries or MongoDB queries against Ratchet storage for dashboards

See [Monitoring & Observability](/deployment/monitoring) for integration guides.

## Deployment checklist

Before going to production:

1. **Apply or initialize storage:** run schema SQL for MySQL/PostgreSQL/Oracle/SQL Server; let MongoDB initialize collections and indexes at startup
2. **Configure the store resource:** JNDI-bound, JTA-managed `DataSource` for SQL stores, or a CDI-produced `MongoDatabase` for MongoDB
3. **Set isolation level for SQL stores:** MySQL requires `READ COMMITTED` (not the default `REPEATABLE READ`)
4. **Tune polling:** adjust `polling.minDelayMs`, `polling.maxDelayMs`, and `polling.batchSize` for your workload
5. **Set up retention:** configure `maintenance.jobRetentionDays`, `maintenance.dlqPurgeDays`, and `maintenance.logRetentionDays` to prevent unbounded table growth
6. **Enable metrics:** wire `MetricsCollector` to your monitoring stack
7. **Configure wakeups if needed:** if running multiple nodes and you want faster cross-node responsiveness, implement `ClusterCoordinator`
8. **Test failover:** verify jobs recover when a node goes down

## Next steps

- [Runtime setup](/deployment/installation) -- CDI, security policy, and container resources
- [Database Setup](/deployment/database-setup) -- Schema application for all stores
- [Rolling Upgrades](/deployment/rolling-upgrades) -- Version coexistence boundaries and rollout order
- [Configuration](/deployment/configuration) -- Full configuration reference
- [Docker Deployment](/deployment/docker) -- Containerized deployments
- [Kubernetes Deployment](/deployment/kubernetes) -- Orchestrated deployments
