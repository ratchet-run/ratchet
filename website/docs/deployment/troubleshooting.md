---
sidebar_position: 14
title: Deployment Troubleshooting
description: Diagnosing and fixing common deployment problems — schema issues, connection failures, clustering problems, and performance bottlenecks
---

# Deployment Troubleshooting

This guide covers problems that surface during deployment and operations. For general debugging techniques, see [Troubleshooting Overview](../troubleshooting/overview.md).

## Schema Issues

### Tables/collections not found

**Symptom:** `Table 'scheduler_job' doesn't exist` or similar errors at startup.

**Fix:** Apply the DDL for your store module. DDL files are in the module's `src/main/resources/ddl/` directory:

```bash
# PostgreSQL
psql -U ratchet -d myapp -f ratchet-store-postgresql/src/main/resources/ddl/postgresql-schema.sql

# MySQL
mysql -u ratchet -p myapp < ratchet-store-mysql/src/main/resources/ddl/mysql-schema.sql

# MongoDB — runs automatically via MongoCollectionInitializer.initialize()
```

Ratchet ships plain SQL files, not migrations. Apply them however your project manages DDL (Flyway, Liquibase, manual scripts, etc.).

### Missing indexes

**Symptom:** Slow job claiming, increasing poll cycle times, growing queue despite available workers.

**Diagnosis:**
```sql
-- PostgreSQL
EXPLAIN ANALYZE
SELECT * FROM scheduler_job
WHERE status = 'PENDING' AND scheduled_time <= NOW()
ORDER BY priority + FLOOR(GREATEST(0, EXTRACT(EPOCH FROM (statement_timestamp() - scheduled_time)) / 60) / 15) DESC,
         scheduled_time ASC
FOR UPDATE SKIP LOCKED LIMIT 10;
```

If you see a sequential scan instead of an index scan, the composite index is missing.

**Fix:** Re-apply the DDL or create the index manually:
```sql
-- PostgreSQL
CREATE INDEX IF NOT EXISTS idx_job_claim_cover
ON scheduler_job (job_type, scheduled_time ASC, priority DESC, job_id ASC)
WHERE status = 'PENDING';
```

### Schema version mismatch

**Symptom:** Column not found errors or constraint violations after upgrading Ratchet.

**Fix:** Compare your applied schema against the latest DDL in the new version. Ratchet does not auto-migrate — you must diff and apply changes manually or through your migration tool.

## Connection Issues

### DataSource not found

**Symptom:** `JNDI lookup failed for 'java:comp/DefaultDataSource'` or `No qualifying bean of type 'DataSource'`.

**Fix:** Ensure your application server has a DataSource configured and bound to the expected JNDI name. For WildFly:

```xml
<!-- standalone.xml -->
<datasource jndi-name="java:jboss/datasources/RatchetDS" pool-name="RatchetDS">
    <connection-url>jdbc:postgresql://localhost:5432/myapp</connection-url>
    <driver>postgresql</driver>
    <security>
        <user-name>ratchet</user-name>
        <password>ratchet</password>
    </security>
</datasource>
```

### Connection pool exhaustion

**Symptom:** `Unable to acquire JDBC Connection` or timeouts during high load.

**Cause:** Ratchet's poller and workers each hold connections during claim and execution. If your pool is smaller than the number of concurrent workers, connections can be exhausted.

**Fix:** Set your connection pool size to at least `workerThreads + pollerThreads + margin`:

```xml
<datasource ...>
    <pool>
        <min-pool-size>5</min-pool-size>
        <max-pool-size>30</max-pool-size>
    </pool>
</datasource>
```

### MongoDB replica set required

**Symptom:** `Transaction numbers are only allowed on a replica set member or mongos` at startup.

**Fix:** MongoDB requires a replica set for transactions. For development:

```bash
mongod --replSet rs0
mongosh --eval 'rs.initiate()'
```

## Clustering Problems

### Duplicate job execution

**Symptom:** The same job runs on two nodes simultaneously.

**Possible causes:**
1. **Missing `SKIP LOCKED` support** — You're on a database version that doesn't support it (MySQL < 8.0, PostgreSQL < 9.5)
2. **Job timeout too short** — The job takes longer than its timeout, so the poller reclaims it while it's still running on another node
3. **Clock skew** — Nodes have significantly different system clocks

**Fix:**
1. Upgrade to a supported database version
2. Increase `withTimeout()` on long-running jobs
3. Synchronize clocks with NTP. Check skew: `SELECT NOW()` on the DB vs `Instant.now()` on each node

### Node appears in `scheduler_node` but isn't running

**Symptom:** Stale entries in the node table for nodes that have been decommissioned.

**Fix:** Stale entries are harmless but confusing. Clean them up:

```sql
DELETE FROM scheduler_node
WHERE last_heartbeat < NOW() - INTERVAL '1 hour';
```

Or use the programmatic API:

```java
nodeStore.deleteInactiveNodesSince(Instant.now().minus(Duration.ofHours(1)));
```

### Jobs stuck in RUNNING

**Symptom:** Jobs have `status = 'RUNNING'` and `picked_by` set to a node that's no longer alive.

**Cause:** The node crashed mid-execution. The job's timeout hasn't expired yet, or there's no timeout set.

**Fix:**
1. **Prevent:** Always set a reasonable `withTimeout()` on jobs
2. **Recover:** After the timeout expires, the poller will automatically reclaim the job. To force recovery:

```sql
UPDATE scheduler_job
SET status = 'PENDING', picked_by = NULL, picked_at = NULL
WHERE status = 'RUNNING'
  AND picked_at < NOW() - INTERVAL '30 minutes';
```

:::caution
Only reset jobs if you're certain the claiming node is truly dead. Resetting a job that's still executing will cause duplicate execution.
:::

## Performance Issues

### Poll cycle taking too long

**Symptom:** `PerformanceMetricsEvent` shows poll cycles > 1 second.

**Diagnosis:** Check if the polling query is using indexes:

```sql
EXPLAIN ANALYZE
SELECT * FROM scheduler_job
WHERE status = 'PENDING' AND scheduled_time <= NOW()
ORDER BY priority + FLOOR(GREATEST(0, EXTRACT(EPOCH FROM (statement_timestamp() - scheduled_time)) / 60) / 15) DESC,
         scheduled_time ASC
FOR UPDATE SKIP LOCKED LIMIT 10;
```

**Fixes:**
1. Ensure the composite index exists (see [Missing indexes](#missing-indexes))
2. Reduce batch size — claiming fewer jobs per cycle reduces lock contention
3. Archive completed jobs to keep the table lean

### Queue growing despite available capacity

**Symptom:** Pending job count increases while worker threads are idle.

**Possible causes:**
1. **Scheduling mismatch** — Jobs have `scheduled_time` in the future
2. **Resource permits** — Jobs require a resource permit that's exhausted
3. **Paused jobs** — Jobs are in `PAUSED` status

**Diagnosis:**
```sql
SELECT status, COUNT(*) FROM scheduler_job
GROUP BY status ORDER BY status;

-- Check future-scheduled jobs
SELECT COUNT(*) FROM scheduler_job
WHERE status = 'PENDING' AND scheduled_time > NOW();
```

### High lock contention on PostgreSQL

**Symptom:** `deadlock detected` errors or long wait times on `scheduler_lock`.

**Fix:** Advisory locks in Ratchet use short TTLs. If you see contention:
1. Ensure `lock_timeout` is set reasonably: `SET lock_timeout = '5s';`
2. Check for long-running transactions holding locks
3. Increase the poll interval to reduce claim frequency

## CDI Issues

### Ratchet beans not discovered

**Symptom:** `Unsatisfied dependency for type JobSchedulerService` at deployment.

**Fix:** Ensure CDI bean discovery is enabled:
1. `beans.xml` exists in `META-INF/` or `WEB-INF/`
2. Ratchet JARs are in the deployment (not just on the classpath as external modules)
3. Bean discovery mode is `all` or `annotated` (not `none`)

### Multiple store implementations on classpath

**Symptom:** `Ambiguous dependency for type JobStore` — CDI found two store beans.

**Fix:** Include only one store module in your deployment. If you need both (e.g., for migration), mark one as `@Alternative` and don't enable it.

## See Also

- [Troubleshooting Overview](../troubleshooting/overview.md) — General debugging techniques
- [Common Issues](../troubleshooting/common-issues.md) — Frequently encountered problems
- [Clustering](./clustering.md) — Multi-node architecture and failure modes
- [Performance Tuning](./performance-tuning.md) — Optimizing for throughput
