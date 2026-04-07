---
sidebar_position: 1
title: Deployment Overview
description: What you need to deploy Ratchet — application server, database, modules, and configuration.
---

# Deployment Overview

Ratchet is a portable, CDI-based job scheduler for Jakarta EE. It deploys as a set of JAR modules inside your application, running on any Jakarta EE 10 Web Profile runtime with a supported database backend.

## What You Need

| Component | Requirement | Notes |
|-----------|-------------|-------|
| **Java** | 17 or later | Virtual threads available on 21+ |
| **Jakarta EE Runtime** | 10 (Web Profile) | WildFly, Payara, Open Liberty, etc. |
| **CDI** | 4.0+ | `beans.xml` with `bean-discovery-mode="all"` |
| **Database** | MySQL 8+, PostgreSQL 14+, or MongoDB 6+ | One store module per database |
| **Build Tool** | Maven 3.8+ | BOM import for version management |

## Ratchet Modules

A typical deployment includes three Ratchet JARs:

```
ratchet-api          Public API, events, enums, SPI interfaces (zero runtime dependencies)
ratchet           Reference implementation — core engine, CDI integration, polling
ratchet-store-*      One of: ratchet-store-mysql, ratchet-store-postgresql, ratchet-store-mongodb
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
      <version>0.1.0-SNAPSHOT</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

## Deployment Scenarios

### Single-Node

The simplest deployment: one application instance connected to one database. No clustering configuration needed. Ratchet's polling engine runs inside the application server and executes jobs using its managed thread pool.

This is suitable for:
- Development and testing
- Low-throughput workloads (< 1,000 jobs/hour)
- Applications where high availability is not critical

### Multi-Node (Clustered)

Multiple application instances share the same database. Ratchet uses database-level locking (`SELECT ... FOR UPDATE SKIP LOCKED` on PostgreSQL, InnoDB row locking on MySQL) to ensure each job is claimed by exactly one node.

For recurring jobs, implement the `ClusterCoordinator` SPI to prevent duplicate scheduling across nodes.

See [Cluster Configuration](/docs/deployment/cluster-configuration) for details.

### Containerized

Ratchet runs in Docker or Kubernetes without any special configuration beyond what a standard Jakarta EE application needs. The database runs as a separate container or managed service.

See [Docker Deployment](/docs/deployment/docker) and [Kubernetes Deployment](/docs/deployment/kubernetes).

## Database Schema

Ratchet ships DDL as plain SQL files — no Flyway or Liquibase dependency is required. The schema files are bundled inside each store module JAR at `ddl/`:

- `ratchet-store-mysql` contains `ddl/mysql-schema.sql`
- `ratchet-store-postgresql` contains `ddl/postgresql-schema.sql`

You apply the schema using whatever mechanism your team prefers: CLI tools, migration frameworks, or application startup scripts. See [Database Setup](/docs/deployment/database-setup) for step-by-step instructions.

### Core Tables

The schema creates these primary tables:

| Table | Purpose |
|-------|---------|
| `scheduler_job` | Job definitions, status, payload, scheduling metadata |
| `scheduler_job_tag` | Tags for job categorization and querying |
| `scheduler_job_execution` | Per-attempt execution history with timing and errors |
| `scheduler_job_log` | Per-job structured log entries |
| `scheduler_batch` | Batch progress tracking |
| `scheduler_batch_metrics` | Batch performance metrics |
| `scheduler_job_archive` | Archived completed/failed jobs |
| `scheduler_node` | Cluster node heartbeats |
| `scheduler_lock` | Distributed lock management |
| `scheduler_resource_limit` | Resource concurrency configuration |
| `scheduler_resource_permit` | Active resource permits for concurrency control |
| `scheduler_workflow_condition` | Workflow branching conditions |
| `scheduler_dlq_alerts` | Dead letter queue alert tracking |

## Configuration

Ratchet uses standard Jakarta EE configuration. Properties can be set via:

- Environment variables (`RATCHET_*`)
- System properties (`-Dratchet.*`)
- `microprofile-config.properties`
- Your runtime's configuration mechanism (e.g., WildFly CLI, Open Liberty `server.xml`)

Key configuration areas:

| Area | Example Property | Default |
|------|-----------------|---------|
| **Thread pool** | `ratchet.executor.threads` | CPU core count |
| **Polling** | `ratchet.polling.interval` | 5000ms |
| **Batch size** | `ratchet.polling.batch-size` | 100 |
| **Job retention** | `ratchet.retention.completed-days` | 30 |
| **Clustering** | `ratchet.cluster.enabled` | false |

See [Configuration](/docs/deployment/configuration) for the full reference.

## Monitoring and Observability

Ratchet provides multiple monitoring integration points:

- **Event system** — CDI events for job lifecycle (started, completed, failed, DLQ)
- **MetricsCollector SPI** — Plug in Micrometer or any custom metrics backend
- **MicroProfile Health** — Implement health checks against the job store
- **Database queries** — Direct SQL queries against Ratchet tables for dashboards

See [Monitoring & Observability](/docs/deployment/monitoring) for integration guides.

## Deployment Checklist

Before going to production:

1. **Apply the DDL** — Run the schema SQL for your chosen database
2. **Configure the DataSource** — JNDI-bound, JTA-managed, with connection pooling
3. **Set isolation level** — MySQL requires `READ COMMITTED` (not the default `REPEATABLE READ`)
4. **Tune polling** — Adjust `ratchet.polling.interval` and `ratchet.polling.batch-size` for your workload
5. **Set up retention** — Configure `ratchet.retention.completed-days` to prevent unbounded table growth
6. **Enable metrics** — Wire `MetricsCollector` to your monitoring stack
7. **Configure clustering** — If running multiple nodes, implement `ClusterCoordinator`
8. **Test failover** — Verify jobs recover when a node goes down

## Next Steps

- [Installation & Setup](/docs/deployment/installation) — Step-by-step getting started
- [Database Setup](/docs/deployment/database-setup) — Schema application for all stores
- [Configuration](/docs/deployment/configuration) — Full configuration reference
- [Docker Deployment](/docs/deployment/docker) — Containerized deployments
- [Kubernetes Deployment](/docs/deployment/kubernetes) — Orchestrated deployments
