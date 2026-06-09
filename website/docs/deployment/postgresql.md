---
title: PostgreSQL Deployment
---

# PostgreSQL Deployment

Ratchet on PostgreSQL 14+.

## Prerequisites

- PostgreSQL 14 or later
- UTF-8 encoding
- psql CLI tool

## Schema setup

### Apply DDL

```bash
psql -U ratchet -d mydb -f ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql
```

Or copy into your migration tool:

```bash
cp postgresql-schema.sql src/main/resources/db/migration/V1__ratchet_schema.sql
flyway migrate
```

### Verify installation

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
- `scheduler_recurring_job`
- `scheduler_recurring_job_archive`

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

### Connection string

```
jdbc:postgresql://localhost:5432/ratchet
```

## Advanced features

### SKIP LOCKED (optimistic claiming)

Ratchet uses PostgreSQL's `SKIP LOCKED` clause for lock-free job claiming:

```sql
SELECT * FROM scheduler_job_queue
WHERE status = 'PENDING'
  AND scheduled_time <= statement_timestamp()
ORDER BY priority + FLOOR(GREATEST(0, EXTRACT(EPOCH FROM (statement_timestamp() - scheduled_time)) / 60) / ?) DESC,
         scheduled_time ASC,
         job_id ASC
LIMIT ?
FOR UPDATE SKIP LOCKED;
```

Multiple Ratchet nodes can claim different jobs simultaneously without blocking each other.

### Active business-key uniqueness

Active-key uniqueness is enforced by a dedicated `scheduler_business_key_reservation` table, not by an index on `scheduler_job`. The reservation table holds one row per active business key, with `business_key` as its primary key:

```sql
CREATE TABLE IF NOT EXISTS scheduler_business_key_reservation
(
    business_key TEXT NOT NULL,
    owner_job_id uuid NOT NULL,
    owner_table  TEXT NOT NULL,
    CONSTRAINT pk_scheduler_business_key_reservation PRIMARY KEY (business_key),
    CONSTRAINT chk_bk_owner_table CHECK (owner_table IN ('QUEUE', 'RECURRING'))
);
```

A duplicate active business key fails against `pk_scheduler_business_key_reservation`. The `business_key` column on `scheduler_job` is observability-only and carries a plain (non-unique) `idx_job_business_key` index.

### Generated columns

Target class and method name are extracted from the JSONB payload as generated columns for indexing:

```sql
target_class TEXT GENERATED ALWAYS AS (payload ->> 'target') STORED
method_name  TEXT GENERATED ALWAYS AS (payload ->> 'method') STORED
```

### CHECK constraints

PostgreSQL uses `CHECK` constraints for data validation instead of MySQL's `ENUM` types. Live status is tracked on `scheduler_job_queue` (`chk_queue_status`), while `scheduler_job` records only the terminal status (`chk_terminal_status`):

```sql
-- scheduler_job (cold metadata + terminal fields)
CONSTRAINT chk_terminal_status CHECK (terminal_status IS NULL OR terminal_status IN ('SUCCEEDED', 'FAILED', 'CANCELED'))
CONSTRAINT chk_job_type CHECK (job_type IN ('SINGLE', 'RECURRING', 'BATCH_PARENT', 'BATCH_CHILD', ...))
CONSTRAINT chk_job_priority CHECK (priority BETWEEN 0 AND 4)

-- scheduler_job_queue (live claim path)
CONSTRAINT chk_queue_status CHECK (status IN ('PENDING', 'RUNNING', 'PAUSED', 'WAITING'))
```

## Performance tuning

### Connection pooling

Use PgBouncer in transaction mode for efficient connection pooling:

```ini
[pgbouncer]
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 25
```

### Shared buffers

Set to 25% of available RAM:

```sql
ALTER SYSTEM SET shared_buffers = '4GB';
```

Restart PostgreSQL after changing.

### Work memory

Increase for complex queries:

```sql
ALTER SYSTEM SET work_mem = '256MB';
SELECT pg_reload_conf();
```

### Effective cache size

Help the planner estimate cache hit rates (set to 75% of available RAM):

```sql
ALTER SYSTEM SET effective_cache_size = '12GB';
SELECT pg_reload_conf();
```

### Autovacuum tuning

Ratchet performs frequent updates and deletes on the hot `scheduler_job_queue` table. Tune autovacuum to keep up:

```sql
ALTER TABLE scheduler_job_queue SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.02
);
```

## Monitoring

### Monitor job queue

```sql
-- Live statuses (PENDING/RUNNING) are on scheduler_job_queue; FAILED is terminal and
-- survives as terminal_status on the cold scheduler_job row after the queue row is
-- deleted. UNION the two to count both in one pass.
SELECT
  COUNT(*) AS total,
  COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending,
  COUNT(CASE WHEN status = 'RUNNING' THEN 1 END) AS running,
  COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed
FROM (
  SELECT status FROM scheduler_job_queue
  UNION ALL
  SELECT terminal_status AS status FROM scheduler_job WHERE terminal_status IS NOT NULL
) all_jobs;
```

### Query performance

Enable `pg_stat_statements` and find slow queries:

```sql
SELECT query, calls, mean_exec_time, max_exec_time
FROM pg_stat_statements
WHERE query LIKE '%scheduler_%'
ORDER BY mean_exec_time DESC
LIMIT 10;
```

### Index usage

Verify indexes are being used:

```sql
SELECT schemaname, tablename, indexname, idx_scan
FROM pg_stat_user_indexes
WHERE indexname LIKE 'idx_%'
  AND tablename LIKE 'scheduler_%'
ORDER BY idx_scan ASC;
```

### Active nodes

```sql
SELECT node_id, heartbeat_ts, started_at
FROM scheduler_node
WHERE heartbeat_ts > NOW() - INTERVAL '30 seconds'
ORDER BY started_at;
```

## Maintenance

### Vacuum and analyze

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

### Monitor table bloat

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

## Backup and recovery

### Logical backup

```bash
pg_dump -Fc -v ratchet > ratchet-backup.dump
```

### Point-in-time recovery

```bash
pg_basebackup -D /backup -Ft -z
```

### Restore

```bash
pg_restore -d ratchet ratchet-backup.dump
```

## High availability

### Streaming replication

For HA, use PostgreSQL streaming replication:

```sql
-- On primary
ALTER SYSTEM SET wal_level = replica;
ALTER SYSTEM SET max_wal_senders = 5;
SELECT pg_reload_conf();
```

### Failover

Use Patroni or pg_auto_failover for automatic failover. Ratchet reconnects automatically when the connection pool detects a new primary.

## See also

- [MySQL Deployment](/deployment/mysql)
- [Database Setup](/deployment/database-setup)
- [Installation](/deployment/installation)
