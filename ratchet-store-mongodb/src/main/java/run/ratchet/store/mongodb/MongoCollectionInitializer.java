package run.ratchet.store.mongodb;

import static run.ratchet.store.mongodb.MongoFieldNames.*;

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
class MongoCollectionInitializer {

  private static final Logger log = Logger.getLogger(MongoCollectionInitializer.class);

  private final MongoDatabase database;

  MongoCollectionInitializer(MongoDatabase database) {
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
          "Index creation error: %s on %s",
          options.getName(),
          coll.getNamespace().getCollectionName());
    }
  }

  private static void createRequiredIndex(
      MongoCollection<Document> coll, Bson keys, IndexOptions options) {
    try {
      coll.createIndex(keys, options);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Required MongoDB index "
              + options.getName()
              + " could not be created on "
              + coll.getNamespace().getCollectionName(),
          e);
    }
  }

  void initialize() {
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
            Indexes.ascending(STATUS),
            Indexes.descending(PRIORITY),
            Indexes.ascending(SCHEDULED_TIME)),
        "idx_job_poll_composite");
    createRequiredIndex(
        coll,
        Indexes.compoundIndex(
            Indexes.ascending(STATUS),
            Indexes.ascending(JOB_TYPE),
            Indexes.descending(PRIORITY),
            Indexes.ascending(SCHEDULED_TIME),
            Indexes.ascending(ID)),
        new IndexOptions().name(MongoIndexHints.JOB_CLAIM_EXEC));
    createIndex(
        coll,
        Indexes.compoundIndex(
            Indexes.ascending(JOB_TYPE), Indexes.ascending(STATUS), Indexes.ascending(NEXT_FIRE)),
        "idx_job_recurring_composite");
    createRequiredIndex(
        coll,
        Indexes.compoundIndex(
            Indexes.ascending(STATUS),
            Indexes.ascending(JOB_TYPE),
            Indexes.descending(PRIORITY),
            Indexes.ascending(NEXT_FIRE),
            Indexes.ascending(ID)),
        new IndexOptions().name(MongoIndexHints.JOB_CLAIM_RECURRING));
    createRequiredIndex(
        coll,
        Indexes.ascending(IDEMPOTENCY_KEY),
        new IndexOptions().name("idx_job_idempotency_key").unique(true));
    createRequiredIndex(
        coll,
        Indexes.ascending(BUSINESS_KEY),
        new IndexOptions()
            .name("idx_job_active_business_key")
            .unique(true)
            .partialFilterExpression(
                new Document(STATUS, new Document("$in", List.of("PENDING", "RUNNING", "PAUSED")))
                    .append(BUSINESS_KEY, new Document("$type", "string"))));
    createIndex(coll, Indexes.ascending(TAGS), "idx_job_tags");
    createIndex(coll, Indexes.ascending(PICKED_BY), "idx_job_picked_by");
    createIndex(coll, Indexes.ascending(DEPENDS_ON), "idx_job_depends_on");
    createIndex(coll, Indexes.ascending(TARGET_CLASS), "idx_job_target_class");
    createIndex(coll, Indexes.ascending(METHOD_NAME), "idx_job_method_name");
    createIndex(coll, Indexes.ascending(CREATED_AT), "idx_job_created_at");
    createIndex(coll, Indexes.ascending(UPDATED_AT), "idx_job_updated_at");
    createIndex(coll, Indexes.ascending(JOB_TYPE), "idx_job_type");
    createIndex(coll, Indexes.ascending(SUPERSEDED_BY), "idx_job_superseded_by");

    // Dashboard query indexes (query-layer hardening)
    createIndex(coll, Indexes.ascending(TRACE_CONTEXT + ".traceparent"), "idx_job_traceparent");
    createIndex(coll, Indexes.ascending(CALLER_PRINCIPAL, CREATED_AT), "idx_job_principal_created");
    createIndex(coll,
        Indexes.compoundIndex(Indexes.ascending(STATUS), Indexes.descending(CREATED_AT)),
        "idx_job_status_created");
  }

  private void createBatchIndexes() {}

  private void createBatchMetricsIndexes() {}

  private void createExecutionIndexes() {
    var coll = database.getCollection("scheduler_job_execution");
    createIndex(coll, Indexes.ascending(JOB_ID), "idx_execution_job_id");
    createIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending(NODE_ID), Indexes.ascending(STARTED_AT)),
        "idx_execution_node_started");
  }

  private void createLogIndexes() {
    var coll = database.getCollection("scheduler_job_log");
    createIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending(JOB_ID), Indexes.ascending(TS)),
        "idx_log_job_ts");
    createIndex(coll, Indexes.ascending(TS), "idx_log_ts");
  }

  private void createArchiveIndexes() {
    var coll = database.getCollection("scheduler_job_archive");
    createIndex(coll, Indexes.ascending(ORIGINAL_JOB_ID), "idx_archive_original_job_id");
    createIndex(coll, Indexes.ascending(FINAL_STATUS), "idx_archive_final_status");
    createIndex(coll, Indexes.ascending(ARCHIVED_AT), "idx_archive_archived_at");
    createIndex(coll, Indexes.ascending(TARGET_CLASS), "idx_archive_target_class");
    createIndex(coll, Indexes.ascending(BUSINESS_KEY), "idx_archive_business_key");
    createIndex(coll, Indexes.ascending(ORIGINAL_CREATED_AT), "idx_archive_original_created_at");
    createIndex(coll, Indexes.ascending(COMPLETION_TIME), "idx_archive_completion_time");
    createIndex(coll, Indexes.ascending(JOB_TYPE), "idx_archive_job_type");
    createIndex(coll, Indexes.ascending(PRIORITY), "idx_archive_priority");
  }

  private void createLockIndexes() {
    var coll = database.getCollection("scheduler_lock");
    createIndex(
        coll,
        Indexes.ascending(EXPIRES_AT),
        new IndexOptions().name("idx_lock_ttl").expireAfter(0L, TimeUnit.SECONDS));
  }

  private void createNodeIndexes() {
    var coll = database.getCollection("scheduler_node");
    createIndex(coll, Indexes.ascending(HEARTBEAT_TS), "idx_node_heartbeat");
  }

  private void createWorkflowConditionIndexes() {
    var coll = database.getCollection("scheduler_workflow_condition");
    createIndex(
        coll,
        Indexes.compoundIndex(
            Indexes.ascending(PARENT_JOB_ID), Indexes.ascending(CONDITION_PRIORITY)),
        "idx_wfc_parent_priority");
    createIndex(coll, Indexes.ascending(CHILD_JOB_ID), "idx_wfc_child");
  }

  private void createDlqAlertIndexes() {
    var coll = database.getCollection("scheduler_dlq_alerts");
    createRequiredIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending(JOB_ID), Indexes.ascending(ERROR_HASH)),
        new IndexOptions().name("idx_dlq_job_hash").unique(true));
    createIndex(coll, Indexes.ascending(ALERT_SENT_AT), "idx_dlq_sent_at");
  }

  private void createResourceLimitIndexes() {}

  private void createResourcePermitIndexes() {
    var coll = database.getCollection("scheduler_resource_permit");
    createIndex(coll, Indexes.ascending(RESOURCE_NAME), "idx_permit_resource");
    createIndex(coll, Indexes.ascending(JOB_ID), "idx_permit_job_id");
    createIndex(coll, Indexes.ascending(NODE_ID), "idx_permit_node_id");
  }
}
