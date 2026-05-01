---
title: MySQL Deployment
---

# MySQL Deployment

Ratchet on MySQL 8+.

## Prerequisites

- MySQL 8.0 or later
- InnoDB storage engine (required for row-level locking)
- `utf8mb4` character set with `utf8mb4_unicode_ci` collation
- `READ COMMITTED` transaction isolation level

## Schema Setup

### Apply DDL

```bash
mysql -u ratchet -p ratchet < ratchet-store-mysql/src/main/resources/ddl/mysql-schema.sql
```

Or copy into your migration tool:

```bash
cp mysql-schema.sql src/main/resources/db/migration/V1__ratchet_schema.sql
flyway migrate
```

### Verify Installation

```sql
SHOW TABLES LIKE 'scheduler_%';
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

Configure your data source for MySQL:

```xml
<!-- persistence.xml -->
<persistence-unit name="your-application-pu" transaction-type="JTA">
  <jta-data-source>java:/RatchetDS</jta-data-source>
  <mapping-file>META-INF/orm-mysql.xml</mapping-file>
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
  <class>run.ratchet.store.entity.LockEntity</class>
  <exclude-unlisted-classes>true</exclude-unlisted-classes>
  <properties>
    <property name="hibernate.dialect" value="org.hibernate.dialect.MySQL8Dialect" />
    <property name="hibernate.connection.isolation" value="2" />
  </properties>
</persistence-unit>
```

The `mapping-file` line is required for production MySQL persistence units.
MySQL stores UUIDv7 IDs as `BINARY(16)`, and the mapping file applies the
store-local `UuidByteArrayConverter` so EclipseLink, OpenJPA, and other
non-Hibernate providers bind UUID fields as 16 bytes. Hibernate's built-in UUID
handler already uses standard-byte-order `BINARY(16)`, so the converter is
idempotent there. PostgreSQL does not use this mapping file.

The MySQL store does not require a fixed persistence-unit name. By default it uses the deployment's
unnamed `@PersistenceContext`. If your application has multiple persistence units, provide a CDI
alternative for `RatchetEntityManagerProvider`:

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

Or via WildFly CLI:

```bash
/subsystem=datasources/data-source=RatchetDS:add( \
    jndi-name=java:/RatchetDS, \
    driver-name=mysql, \
    connection-url=jdbc:mysql://localhost:3306/ratchet, \
    user-name=ratchet, \
    password=secret, \
    min-pool-size=5, \
    max-pool-size=20, \
    transaction-isolation=TRANSACTION_READ_COMMITTED, \
    valid-connection-checker-class-name=org.jboss.jca.adapters.jdbc.extensions.mysql.MySQLValidConnectionChecker)
```

:::caution MySQL Isolation Level
MySQL defaults to `REPEATABLE READ`, which acquires gap locks on `SELECT ... FOR UPDATE` that block concurrent inserts into the same table. This causes lock wait timeouts under production job scheduling load. **Always** configure `READ COMMITTED` isolation via one of:

- **WildFly DataSource**: `transaction-isolation=TRANSACTION_READ_COMMITTED`
- **JDBC URL**: `?sessionVariables=transaction_isolation='READ-COMMITTED'`
- **persistence.xml**: `<property name="hibernate.connection.isolation" value="2"/>`
- **WildFly -ds.xml**: `<transaction-isolation>TRANSACTION_READ_COMMITTED</transaction-isolation>`

Ratchet checks this at startup and fails by default when the active session is not
`READ COMMITTED`. Use `RatchetOptions.builder().store(s -> s.isolationCheckMode(RatchetOptions.IsolationCheckMode.WARN))`
only as an explicit temporary opt-out.
:::

### MySQL-Specific Settings

Ratchet does not expose MySQL-only tuning flags. Use the shared scheduler settings instead:

- `RatchetOptions.polling().batchSize()`
- `RatchetOptions.polling().minDelayMs()`
- `RatchetOptions.polling().maxDelayMs()`
- `RatchetOptions.execution().maxConcurrency("SINGLE", ...)`
- `RatchetOptions.maintenance().jobRetentionDays()`

## Schema Design

### Primary Tables

| Table | Purpose |
|-------|---------|
| `scheduler_job` | Cold job metadata, payload, and terminal state |
| `scheduler_job_execution` | Per-attempt execution history with timing and errors |
| `scheduler_job_archive` | Archived completed/failed/canceled jobs |
| `scheduler_batch` | Batch progress tracking |

### MySQL-Specific Features

The MySQL schema uses several MySQL-specific features:

- **ENUM types** for `status`, `job_type`, `backoff_policy`, and `level` columns
- **BINARY(16) UUIDv7 identifiers** for every primary and foreign key that refers to a job, batch, execution, log, archive, resource permit, or workflow row
- **JSON columns** for `payload`, `params`, `job_result`, and `mdc`
- **GENERATED ALWAYS AS ... STORED** columns to extract `target_class` and `method_name` from payload JSON
- **Reservation table** for active business-key uniqueness without keeping terminal rows hot
- **Optional operator views** in `ddl/views/vw_jobs.sql` that expose binary UUIDs as hyphenated strings via `BIN_TO_UUID(col)` without MySQL's UUIDv1 swap flag

### Key Indexes

The schema includes optimized indexes for the polling query:

```sql
-- Executable claim path on scheduler_job_queue
INDEX idx_claim_executable (status, job_type, scheduled_time, priority, job_id)

-- Orphan recovery
INDEX idx_queue_orphan (status, picked_at, picked_by)

-- recurring-master scheduling
INDEX idx_job_recurring_pending (job_type, rec_status, next_fire)
```

## Performance Tuning

### InnoDB Buffer Pool

For better performance, size the InnoDB buffer pool to 70-80% of available RAM:

```ini
[mysqld]
innodb_buffer_pool_size = 8G
innodb_buffer_pool_instances = 8
```

### Redo Log Size

Larger redo logs improve write throughput:

```ini
[mysqld]
innodb_log_file_size = 1G
```

### Max Connections

Ensure MySQL allows enough connections for your connection pool:

```ini
[mysqld]
max_connections = 200
```

### Transaction Isolation

Set globally for all connections:

```ini
[mysqld]
transaction_isolation = READ-COMMITTED
```

## Monitoring

### Monitor Job Queue Depth

```sql
SELECT COUNT(*) AS pending_jobs
FROM scheduler_job
WHERE status = 'PENDING';
```

### Failed Job Trends

```sql
SELECT DATE(archived_at) AS date, COUNT(*) AS failures
FROM scheduler_job_archive
WHERE final_status = 'FAILED'
  AND archived_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY DATE(archived_at)
ORDER BY date DESC;
```

### Job Execution Performance

```sql
SELECT
  AVG(duration_ms) AS avg_duration_ms,
  MAX(duration_ms) AS max_duration_ms
FROM scheduler_job_execution
WHERE started_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)
  AND status = 'SUCCEEDED';
```

### Active Nodes

```sql
SELECT node_id, heartbeat_ts, started_at
FROM scheduler_node
WHERE heartbeat_ts > DATE_SUB(NOW(), INTERVAL 30 SECOND)
ORDER BY started_at;
```

## Maintenance

### Cleanup Old Jobs

Ratchet automatically archives completed jobs based on the `RATCHET_JOB_RETENTION_DAYS` setting. To manually clean up:

```sql
DELETE FROM scheduler_job_archive
WHERE archived_at < DATE_SUB(NOW(), INTERVAL 90 DAY);
```

### Rebuild Indexes

Periodically optimize tables for better query performance:

```sql
OPTIMIZE TABLE scheduler_job;
OPTIMIZE TABLE scheduler_job_execution;
OPTIMIZE TABLE scheduler_job_archive;
```

## Backup & Recovery

### Backup Strategy

Use MySQL's native backup tools:

```bash
mysqldump -u root -p ratchet > ratchet-backup.sql
```

Or with Percona XtraBackup for online backups:

```bash
xtrabackup --backup --target-dir=/backup
```

### Recovery

```bash
mysql -u root -p ratchet < ratchet-backup.sql
```

## See Also

- [PostgreSQL Deployment](/docs/deployment/postgresql)
- [Database Setup](/docs/deployment/database-setup)
- [Installation](/docs/deployment/installation)
