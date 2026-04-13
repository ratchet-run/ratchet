package run.ratchet.store.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/** Creates MongoDB collections and indexes required by the Ratchet scheduler at startup. */
public class MongoCollectionInitializer {

  private static final Logger log = Logger.getLogger(MongoCollectionInitializer.class);

  private final MongoDatabase database;

  public MongoCollectionInitializer(MongoDatabase database) {
    this.database = database;
  }

  private static void createIndex(MongoCollection<Document> coll, Bson keys, String name) {
    createIndex(coll, keys, new IndexOptions().name(name));
  }

  private static void createIndex(MongoCollection<Document> coll, Bson keys, IndexOptions options) {
    try {
      coll.createIndex(keys, options);
    } catch (Exception e) {
      log.warnf(
          e,
          "Failed to create index %s on %s",
          options.getName(),
          coll.getNamespace().getCollectionName());
    }
  }

  public void initialize() {
    log.debug("Initializing MongoDB collections and indexes");
    createJobIndexes();
    createBatchIndexes();
    createBatchMetricsIndexes();
    createExecutionIndexes();
    createLogIndexes();
    createArchiveIndexes();
    createLockIndexes();
    createNodeIndexes();
    createWorkflowConditionIndexes();
    createDlqAlertIndexes();
    createResourceLimitIndexes();
    createResourcePermitIndexes();
  }

  private void createJobIndexes() {
    var coll = database.getCollection("scheduler_job");
    createIndex(
        coll,
        Indexes.compoundIndex(
            Indexes.ascending("status"),
            Indexes.descending("priority"),
            Indexes.ascending("scheduled_time")),
        "idx_job_poll_composite");
    createIndex(
        coll,
        Indexes.compoundIndex(
            Indexes.ascending("job_type"),
            Indexes.ascending("status"),
            Indexes.ascending("next_fire")),
        "idx_job_recurring_composite");
    createIndex(
        coll,
        Indexes.ascending("idempotency_key"),
        new IndexOptions().name("idx_job_idempotency_key").unique(true));
    createIndex(
        coll,
        Indexes.ascending("business_key"),
        new IndexOptions()
            .name("idx_job_active_business_key")
            .unique(true)
            .partialFilterExpression(
                new Document("status", new Document("$in", List.of("PENDING", "RUNNING", "PAUSED")))
                    .append("business_key", new Document("$type", "string"))));
    createIndex(coll, Indexes.ascending("tags"), "idx_job_tags");
    createIndex(coll, Indexes.ascending("picked_by"), "idx_job_picked_by");
    createIndex(coll, Indexes.ascending("depends_on"), "idx_job_depends_on");
    createIndex(coll, Indexes.ascending("target_class"), "idx_job_target_class");
    createIndex(coll, Indexes.ascending("method_name"), "idx_job_method_name");
    createIndex(coll, Indexes.ascending("created_at"), "idx_job_created_at");
    createIndex(coll, Indexes.ascending("updated_at"), "idx_job_updated_at");
    createIndex(coll, Indexes.ascending("job_type"), "idx_job_type");
    createIndex(coll, Indexes.ascending("superseded_by"), "idx_job_superseded_by");
  }

  private void createBatchIndexes() {}

  private void createBatchMetricsIndexes() {}

  private void createExecutionIndexes() {
    var coll = database.getCollection("scheduler_job_execution");
    createIndex(coll, Indexes.ascending("job_id"), "idx_execution_job_id");
    createIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending("node_id"), Indexes.ascending("started_at")),
        "idx_execution_node_started");
  }

  private void createLogIndexes() {
    var coll = database.getCollection("scheduler_job_log");
    createIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending("job_id"), Indexes.ascending("ts")),
        "idx_log_job_ts");
    createIndex(coll, Indexes.ascending("ts"), "idx_log_ts");
  }

  private void createArchiveIndexes() {
    var coll = database.getCollection("scheduler_job_archive");
    createIndex(coll, Indexes.ascending("original_job_id"), "idx_archive_original_job_id");
    createIndex(coll, Indexes.ascending("final_status"), "idx_archive_final_status");
    createIndex(coll, Indexes.ascending("archived_at"), "idx_archive_archived_at");
    createIndex(coll, Indexes.ascending("target_class"), "idx_archive_target_class");
    createIndex(coll, Indexes.ascending("business_key"), "idx_archive_business_key");
    createIndex(coll, Indexes.ascending("original_created_at"), "idx_archive_original_created_at");
    createIndex(coll, Indexes.ascending("completion_time"), "idx_archive_completion_time");
    createIndex(coll, Indexes.ascending("job_type"), "idx_archive_job_type");
    createIndex(coll, Indexes.ascending("priority"), "idx_archive_priority");
  }

  private void createLockIndexes() {
    var coll = database.getCollection("scheduler_lock");
    createIndex(
        coll,
        Indexes.ascending("expires_at"),
        new IndexOptions().name("idx_lock_ttl").expireAfter(0L, TimeUnit.SECONDS));
  }

  private void createNodeIndexes() {
    var coll = database.getCollection("scheduler_node");
    createIndex(coll, Indexes.ascending("heartbeat_ts"), "idx_node_heartbeat");
  }

  private void createWorkflowConditionIndexes() {
    var coll = database.getCollection("scheduler_workflow_condition");
    createIndex(
        coll,
        Indexes.compoundIndex(
            Indexes.ascending("parent_job_id"), Indexes.ascending("condition_priority")),
        "idx_wfc_parent_priority");
    createIndex(coll, Indexes.ascending("child_job_id"), "idx_wfc_child");
  }

  private void createDlqAlertIndexes() {
    var coll = database.getCollection("scheduler_dlq_alerts");
    createIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending("job_id"), Indexes.ascending("error_hash")),
        new IndexOptions().name("idx_dlq_job_hash").unique(true));
    createIndex(coll, Indexes.ascending("alert_sent_at"), "idx_dlq_sent_at");
  }

  private void createResourceLimitIndexes() {}

  private void createResourcePermitIndexes() {
    var coll = database.getCollection("scheduler_resource_permit");
    createIndex(coll, Indexes.ascending("resource_name"), "idx_permit_resource");
    createIndex(coll, Indexes.ascending("job_id"), "idx_permit_job_id");
    createIndex(coll, Indexes.ascending("node_id"), "idx_permit_node_id");
  }
}
