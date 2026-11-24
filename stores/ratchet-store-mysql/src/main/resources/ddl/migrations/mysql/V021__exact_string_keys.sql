-- Stop all application writers before migrating. MySQL commits each DDL statement.
-- Align resource references with their existing limit while the old comparison still applies.
-- The limit changes last so an interrupted migration can retry this step.

UPDATE scheduler_job AS ref
  JOIN scheduler_resource_limit AS resource
    ON ref.resource_name COLLATE utf8mb4_unicode_ci = resource.resource_name
SET ref.resource_name = resource.resource_name
WHERE BINARY ref.resource_name <> BINARY resource.resource_name
  AND EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'scheduler_resource_limit'
      AND column_name = 'resource_name' AND collation_name <> 'utf8mb4_bin'
  );

UPDATE scheduler_recurring_job AS ref
  JOIN scheduler_resource_limit AS resource
    ON ref.resource_name COLLATE utf8mb4_unicode_ci = resource.resource_name
SET ref.resource_name = resource.resource_name
WHERE BINARY ref.resource_name <> BINARY resource.resource_name
  AND EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'scheduler_resource_limit'
      AND column_name = 'resource_name' AND collation_name <> 'utf8mb4_bin'
  );

UPDATE scheduler_resource_waiter AS ref
  JOIN scheduler_resource_limit AS resource
    ON ref.resource_name COLLATE utf8mb4_unicode_ci = resource.resource_name
SET ref.resource_name = resource.resource_name
WHERE BINARY ref.resource_name <> BINARY resource.resource_name
  AND EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'scheduler_resource_limit'
      AND column_name = 'resource_name' AND collation_name <> 'utf8mb4_bin'
  );

-- Permit ids are derived from the original resource name. Renaming leaves an unreleasable permit.
DELETE permit FROM scheduler_resource_permit AS permit
  JOIN scheduler_resource_limit AS resource
    ON permit.resource_name COLLATE utf8mb4_unicode_ci = resource.resource_name
WHERE BINARY permit.resource_name <> BINARY resource.resource_name
  AND EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'scheduler_resource_limit'
      AND column_name = 'resource_name' AND collation_name <> 'utf8mb4_bin'
  );

ALTER TABLE scheduler_node
  MODIFY node_id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL;

ALTER TABLE scheduler_lock
  MODIFY lock_name VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
  MODIFY owner_node VARCHAR(64) COLLATE utf8mb4_bin NOT NULL;

ALTER TABLE scheduler_recurring_job
  MODIFY business_key VARCHAR(255) COLLATE utf8mb4_bin NULL,
  MODIFY resource_name VARCHAR(100) COLLATE utf8mb4_bin NULL,
  MODIFY execution_target VARCHAR(64) COLLATE utf8mb4_bin NULL;

ALTER TABLE scheduler_recurring_job_archive
  MODIFY business_key VARCHAR(255) COLLATE utf8mb4_bin NULL,
  MODIFY execution_target VARCHAR(64) COLLATE utf8mb4_bin NULL;

ALTER TABLE scheduler_job
  MODIFY idempotency_key VARCHAR(36) COLLATE utf8mb4_bin NOT NULL,
  MODIFY business_key VARCHAR(255) COLLATE utf8mb4_bin NULL,
  MODIFY resource_name VARCHAR(100) COLLATE utf8mb4_bin NULL,
  MODIFY execution_target VARCHAR(64) COLLATE utf8mb4_bin NULL;

ALTER TABLE scheduler_job_queue
  MODIFY business_key VARCHAR(255) COLLATE utf8mb4_bin NULL,
  MODIFY picked_by VARCHAR(64) COLLATE utf8mb4_bin NULL,
  MODIFY signal_key VARCHAR(255) COLLATE utf8mb4_bin NULL,
  MODIFY execution_target VARCHAR(64) COLLATE utf8mb4_bin NULL;

ALTER TABLE scheduler_business_key_reservation
  MODIFY business_key VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
  MODIFY held_by_node VARCHAR(64) COLLATE utf8mb4_bin NULL;

ALTER TABLE scheduler_job_tag
  MODIFY tag VARCHAR(64) COLLATE utf8mb4_bin NOT NULL;

ALTER TABLE scheduler_job_execution
  MODIFY node_id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL;

ALTER TABLE scheduler_job_archive
  MODIFY business_key VARCHAR(255) COLLATE utf8mb4_bin NULL,
  MODIFY archived_by VARCHAR(64) COLLATE utf8mb4_bin NULL,
  MODIFY tags VARCHAR(512) COLLATE utf8mb4_bin NULL;

ALTER TABLE scheduler_resource_permit
  MODIFY resource_name VARCHAR(100) COLLATE utf8mb4_bin NOT NULL,
  MODIFY node_id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL;

ALTER TABLE scheduler_idempotency_key
  MODIFY idempotency_key VARCHAR(36) COLLATE utf8mb4_bin NOT NULL;

ALTER TABLE scheduler_event_outbox
  MODIFY business_key VARCHAR(255) COLLATE utf8mb4_bin NULL,
  MODIFY node_id VARCHAR(64) COLLATE utf8mb4_bin NULL;

ALTER TABLE scheduler_partition_set
  MODIFY set_key VARCHAR(36) COLLATE utf8mb4_bin NOT NULL;

ALTER TABLE scheduler_resource_waiter
  MODIFY resource_name VARCHAR(100) COLLATE utf8mb4_bin NOT NULL;

ALTER TABLE scheduler_resource_limit
  MODIFY resource_name VARCHAR(100) COLLATE utf8mb4_bin NOT NULL;
