---
title: PostgreSQL Deployment
---

# PostgreSQL Deployment

Ratchet on PostgreSQL 14+.

## Prerequisites

- PostgreSQL 14 or later
- UTF-8 encoding
- psql CLI tool

## Schema Setup

### Apply DDL

```bash
psql -U ratchet -d mydb -f ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql
```

Or copy into your migration tool:

```bash
cp postgresql-schema.sql src/main/resources/db/migration/V1__ratchet_schema.sql
flyway migrate
```

### Verify Installation

```sql
\dt scheduler_*
```

You should see:
- `ratchet_schema_version`
- `scheduler_job`
- `scheduler_job_queue`
- `scheduler_business_key_reservation`
- `scheduler_job_tag`
- `scheduler_job_execution`
- `scheduler_job_log`
- `scheduler_job_archive`
- `scheduler_batch`
- `scheduler_batch_metrics`
- `scheduler_node`
- `scheduler_lock`
- `scheduler_resource_limit`
- `scheduler_resource_permit`
- `scheduler_workflow_condition`
- `scheduler_dlq_alerts`

## Configuration

### DataSource

Configure your data source for PostgreSQL:

```xml
<!-- persistence.xml -->
<persistence-unit name="your-application-pu" transaction-type="JTA">
  <jta-data-source>java:/RatchetDS</jta-data-source>
  <class>run.ratchet.store.entity.JobEntity</class>
  <class>run.ratchet.store.entity.JobExecutionEntity</class>
  <class>run.ratchet.store.entity.ResourceLimitEntity</class>
  <class>run.ratchet.store.entity.BatchMetricsEntity</class>
  <class>run.ratchet.store.entity.WorkflowConditionEntity</class>
  <class>run.ratchet.store.entity.ArchivedJobEntity</class>
  <class>run.ratchet.store.entity.NodeEntity</class>
  <class>run.ratchet.store.entity.DlqAlertEntity</class>
  <class>run.ratchet.store.entity.JobLogEntity</class>
  <class>run.ratchet.store.entity.ResourcePermitEntity</class>
  <class>run.ratchet.store.entity.BatchEntity</class>
  <exclude-unlisted-classes>true</exclude-unlisted-classes>
  <properties>
    <property name="hibernate.dialect" value="org.hibernate.dialect.PostgreSQL14Dialect" />
  </properties>
</persistence-unit>
```

The PostgreSQL store does not require a fixed persistence-unit name. By default it uses the
deployment's unnamed `@PersistenceContext`. If your application has multiple persistence units,
provide a CDI alternative for `RatchetEntityManagerProvider`:

```java
@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
public class RatchetPuProvider implements RatchetEntityManagerProvider {
  @PersistenceContext(unitName = "your-application-pu")
  EntityManager em;

  @Override
  public EntityManager getEntityManager() {
    return em;
  }
}
```

### WildFly CLI

```bash
/subsystem=datasources/data-source=RatchetDS:add( \
    jndi-name=java:/RatchetDS, \
    driver-name=postgresql, \
    connection-url=jdbc:postgresql://localhost:5432/ratchet, \
    user-name=ratchet, \
    password=secret, \
    min-pool-size=5, \
    max-pool-size=20, \
    valid-connection-checker-class-name=org.jboss.jca.adapters.jdbc.extensions.postgres.PostgreSQLValidConnectionChecker)
```

### Connection String

```
jdbc:postgresql://localhost:5432/ratchet
```

## Advanced Features

### SKIP LOCKED (Optimistic Claiming)

Ratchet uses PostgreSQL's `SKIP LOCKED` clause for lock-free job claiming:

```sql
SELECT * FROM scheduler_job
WHERE status = 'PENDING'
  AND scheduled_time <= NOW()
ORDER BY priority + FLOOR(GREATEST(0, EXTRACT(EPOCH FROM (statement_timestamp() - scheduled_time)) / 60) / 15) DESC,
         scheduled_time ASC
LIMIT 10
FOR UPDATE SKIP LOCKED;
```

This allows multiple Ratchet nodes to safely claim different jobs simultaneously without blocking each other.

### Partial Indexes

The schema uses partial indexes for performance. Instead of indexing all rows, partial indexes only cover the rows that matter for active queries:

```sql
-- Unique business key only for active jobs (PENDING, RUNNING, PAUSED)
CREATE UNIQUE INDEX idx_job_active_business_key
ON scheduler_job (business_key)
WHERE status IN ('PENDING', 'RUNNING', 'PAUSED') AND business_key IS NOT NULL;
```

This approach replaces MySQL's generated `active_business_key` column with a more space-efficient partial unique index.

### Generated Columns

Target class and method name are extracted from the JSON payload as generated columns for indexing:

```sql
target_class TEXT GENERATED ALWAYS AS (payload::jsonb ->> 'target') STORED
method_name  TEXT GENERATED ALWAYS AS (payload::jsonb ->> 'method') STORED
```

### CHECK Constraints

PostgreSQL uses `CHECK` constraints for data validation instead of MySQL's `ENUM` types:

```sql
CONSTRAINT chk_job_status CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED', 'PAUSED'))
CONSTRAINT chk_job_type CHECK (job_type IN ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD', ...))
CONSTRAINT chk_job_priority CHECK (priority BETWEEN 0 AND 4)
```

## Performance Tuning

### Connection Pooling

Use PgBouncer in transaction mode for efficient connection pooling:

```ini
[pgbouncer]
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 25
```

### Shared Buffers

Set to 25% of available RAM:

```sql
ALTER SYSTEM SET shared_buffers = '4GB';
```

Restart PostgreSQL after changing.

### Work Memory

Increase for complex queries:

```sql
ALTER SYSTEM SET work_mem = '256MB';
SELECT pg_reload_conf();
```

### Effective Cache Size

Help the planner estimate cache hit rates (set to 75% of available RAM):

```sql
ALTER SYSTEM SET effective_cache_size = '12GB';
SELECT pg_reload_conf();
```

### Autovacuum Tuning

Ratchet performs frequent updates to `scheduler_job`. Tune autovacuum to keep up:

```sql
ALTER TABLE scheduler_job SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.02
);
```

## Monitoring

### Monitor Job Queue

```sql
SELECT
  COUNT(*) AS total,
  COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending,
  COUNT(CASE WHEN status = 'RUNNING' THEN 1 END) AS running,
  COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed
FROM scheduler_job;
```

### Query Performance

Enable `pg_stat_statements` and find slow queries:

```sql
SELECT query, calls, mean_exec_time, max_exec_time
FROM pg_stat_statements
WHERE query LIKE '%scheduler_%'
ORDER BY mean_exec_time DESC
LIMIT 10;
```

### Index Usage

Verify indexes are being used:

```sql
SELECT schemaname, tablename, indexname, idx_scan
FROM pg_stat_user_indexes
WHERE indexname LIKE 'idx_%'
  AND tablename LIKE 'scheduler_%'
ORDER BY idx_scan ASC;
```

### Active Nodes

```sql
SELECT node_id, heartbeat_ts, started_at
FROM scheduler_node
WHERE heartbeat_ts > NOW() - INTERVAL '30 seconds'
ORDER BY started_at;
```

## Maintenance

### Vacuum & Analyze

Regular maintenance is important for tables with frequent updates:

```bash
# Manual vacuum during off-peak
psql -d ratchet -c "VACUUM ANALYZE scheduler_job;"
```

### Reindex

Rebuild indexes to reclaim space after heavy updates:

```sql
REINDEX TABLE scheduler_job;
REINDEX TABLE scheduler_job_execution;
REINDEX TABLE scheduler_job_archive;
```

### Monitor Table Bloat

```sql
SELECT
  relname AS table_name,
  pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
  n_live_tup AS live_rows,
  n_dead_tup AS dead_rows,
  ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_pct
FROM pg_stat_user_tables
WHERE relname LIKE 'scheduler_%'
ORDER BY n_dead_tup DESC;
```

## Backup & Recovery

### Logical Backup

```bash
pg_dump -Fc -v ratchet > ratchet-backup.dump
```

### Point-in-Time Recovery

```bash
pg_basebackup -D /backup -Ft -z
```

### Restore

```bash
pg_restore -d ratchet ratchet-backup.dump
```

## High Availability

### Streaming Replication

For HA, use PostgreSQL streaming replication:

```sql
-- On primary
ALTER SYSTEM SET wal_level = replica;
ALTER SYSTEM SET max_wal_senders = 5;
SELECT pg_reload_conf();
```

### Failover

Use Patroni or pg_auto_failover for automatic failover. Ratchet reconnects automatically when the connection pool detects a new primary.

## See Also

- [MySQL Deployment](/docs/deployment/mysql)
- [Database Setup](/docs/deployment/database-setup)
- [Installation](/docs/deployment/installation)
