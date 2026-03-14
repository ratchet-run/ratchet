// MongoDB initialization script for Ratchet scheduler.
// Run with: mongosh <database> mongodb-init.js
//
// This is the MongoDB equivalent of the SQL DDL scripts. Collections are created
// implicitly on first write, but indexes must be created explicitly.

// ── Counters (sequence generator) ────────────────────────────────────────────
db.createCollection("counters");
db.counters.insertOne({ _id: "scheduler_job", seq: NumberLong(0) });
db.counters.insertOne({ _id: "scheduler_job_execution", seq: NumberLong(0) });
db.counters.insertOne({ _id: "scheduler_job_log", seq: NumberLong(0) });
db.counters.insertOne({ _id: "scheduler_job_archive", seq: NumberLong(0) });
db.counters.insertOne({ _id: "scheduler_workflow_condition", seq: NumberLong(0) });
db.counters.insertOne({ _id: "scheduler_dlq_alerts", seq: NumberLong(0) });
db.counters.insertOne({ _id: "scheduler_resource_permit", seq: NumberLong(0) });

// ── scheduler_job ────────────────────────────────────────────────────────────
db.scheduler_job.createIndex(
  { status: 1, priority: -1, scheduled_time: 1 },
  { name: "idx_job_poll_composite" }
);
db.scheduler_job.createIndex(
  { job_type: 1, status: 1, next_fire: 1 },
  { name: "idx_job_recurring_composite" }
);
db.scheduler_job.createIndex(
  { idempotency_key: 1 },
  { name: "idx_job_idempotency_key", unique: true }
);
db.scheduler_job.createIndex(
  { business_key: 1 },
  {
    name: "idx_job_active_business_key",
    unique: true,
    partialFilterExpression: {
      status: { $in: ["PENDING", "RUNNING", "PAUSED"] },
      business_key: { $type: "string" }
    }
  }
);
db.scheduler_job.createIndex({ tags: 1 }, { name: "idx_job_tags" });
db.scheduler_job.createIndex({ picked_by: 1 }, { name: "idx_job_picked_by" });
db.scheduler_job.createIndex({ depends_on: 1 }, { name: "idx_job_depends_on" });
db.scheduler_job.createIndex({ target_class: 1 }, { name: "idx_job_target_class" });
db.scheduler_job.createIndex({ method_name: 1 }, { name: "idx_job_method_name" });
db.scheduler_job.createIndex({ created_at: 1 }, { name: "idx_job_created_at" });
db.scheduler_job.createIndex({ updated_at: 1 }, { name: "idx_job_updated_at" });
db.scheduler_job.createIndex({ job_type: 1 }, { name: "idx_job_type" });
db.scheduler_job.createIndex({ superseded_by: 1 }, { name: "idx_job_superseded_by" });

// ── scheduler_job_execution ──────────────────────────────────────────────────
db.scheduler_job_execution.createIndex({ job_id: 1 }, { name: "idx_execution_job_id" });
db.scheduler_job_execution.createIndex(
  { node_id: 1, started_at: 1 },
  { name: "idx_execution_node_started" }
);

// ── scheduler_job_log ────────────────────────────────────────────────────────
db.scheduler_job_log.createIndex({ job_id: 1, ts: 1 }, { name: "idx_log_job_ts" });
db.scheduler_job_log.createIndex({ ts: 1 }, { name: "idx_log_ts" });

// ── scheduler_job_archive ────────────────────────────────────────────────────
db.scheduler_job_archive.createIndex({ original_job_id: 1 }, { name: "idx_archive_original_job_id" });
db.scheduler_job_archive.createIndex({ final_status: 1 }, { name: "idx_archive_final_status" });
db.scheduler_job_archive.createIndex({ archived_at: 1 }, { name: "idx_archive_archived_at" });
db.scheduler_job_archive.createIndex({ target_class: 1 }, { name: "idx_archive_target_class" });
db.scheduler_job_archive.createIndex({ business_key: 1 }, { name: "idx_archive_business_key" });
db.scheduler_job_archive.createIndex({ original_created_at: 1 }, { name: "idx_archive_original_created_at" });
db.scheduler_job_archive.createIndex({ completion_time: 1 }, { name: "idx_archive_completion_time" });
db.scheduler_job_archive.createIndex({ job_type: 1 }, { name: "idx_archive_job_type" });
db.scheduler_job_archive.createIndex({ priority: 1 }, { name: "idx_archive_priority" });

// ── scheduler_lock (TTL-based distributed locks) ─────────────────────────────
db.scheduler_lock.createIndex(
  { expires_at: 1 },
  { name: "idx_lock_ttl", expireAfterSeconds: 0 }
);

// ── scheduler_node ───────────────────────────────────────────────────────────
db.scheduler_node.createIndex({ heartbeat_ts: 1 }, { name: "idx_node_heartbeat" });

// ── scheduler_workflow_condition ──────────────────────────────────────────────
db.scheduler_workflow_condition.createIndex(
  { parent_job_id: 1, condition_priority: 1 },
  { name: "idx_wfc_parent_priority" }
);
db.scheduler_workflow_condition.createIndex({ child_job_id: 1 }, { name: "idx_wfc_child" });

// ── scheduler_dlq_alerts ─────────────────────────────────────────────────────
db.scheduler_dlq_alerts.createIndex(
  { job_id: 1, error_hash: 1 },
  { name: "idx_dlq_job_hash", unique: true }
);
db.scheduler_dlq_alerts.createIndex({ alert_sent_at: 1 }, { name: "idx_dlq_sent_at" });

// ── scheduler_resource_permit ────────────────────────────────────────────────
db.scheduler_resource_permit.createIndex({ resource_name: 1 }, { name: "idx_permit_resource" });
db.scheduler_resource_permit.createIndex({ job_id: 1 }, { name: "idx_permit_job_id" });
db.scheduler_resource_permit.createIndex({ node_id: 1 }, { name: "idx_permit_node_id" });

print("Ratchet MongoDB initialization complete.");
