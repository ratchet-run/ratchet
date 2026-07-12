/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.ne;
import static run.ratchet.store.mongodb.MongoFieldNames.*;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;
import run.ratchet.store.util.BusinessKeyReservations;

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
    } catch (MongoCommandException e) {
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
    } catch (MongoCommandException e) {
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
    createBusinessKeyReservationIndexes();
    createRecurringJobIndexes();
    backfillBusinessKeyReservations();
    createRecurringJobArchiveIndexes();
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
    createJobPropertiesIndexes();
    createJobExtensionStateIndexes();
  }

  private void createBusinessKeyReservationIndexes() {
    var coll = database.getCollection("scheduler_business_key_reservation");
    // _id is the globally unique business key. This owner lookup makes terminal/cancel cleanup
    // efficient without weakening the single cross-type serialization point.
    createRequiredIndex(
        coll, Indexes.ascending(OWNER_JOB_ID), new IndexOptions().name("idx_bk_owner"));
  }

  private void backfillBusinessKeyReservations() {
    // Existing deployments predate the shared reservation collection. Populate it before this
    // store instance becomes available so every already-active owner participates in the new
    // cross-type uniqueness invariant. Each upsert targets the shared _id and then verifies the
    // stored owner, so concurrent startup is idempotent for the same owner while a conflicting
    // owner fails startup instead of choosing a winner silently.
    removeStaleBusinessKeyReservations();
    for (Document job :
        database
            .getCollection("scheduler_job")
            .find(and(in(STATUS, MongoStoreContext.ACTIVE_STATUSES), ne(BUSINESS_KEY, null)))
            .projection(new Document(ID, 1).append(BUSINESS_KEY, 1))) {
      reserveExisting(
          job.getString(BUSINESS_KEY),
          job.get(ID, UUID.class),
          BusinessKeyReservations.OWNER_TABLE_QUEUE);
    }
    for (Document recurring :
        database
            .getCollection("scheduler_recurring_job")
            .find(ne(BUSINESS_KEY, null))
            .projection(new Document(ID, 1).append(BUSINESS_KEY, 1))) {
      reserveExisting(
          recurring.getString(BUSINESS_KEY),
          recurring.get(ID, UUID.class),
          BusinessKeyReservations.OWNER_TABLE_RECURRING);
    }
  }

  private void removeStaleBusinessKeyReservations() {
    var reservations = database.getCollection("scheduler_business_key_reservation");
    for (Document reservation : reservations.find()) {
      String businessKey = reservation.getString(ID);
      UUID ownerJobId = reservation.get(OWNER_JOB_ID, UUID.class);
      String ownerTable = reservation.getString(OWNER_TABLE);
      if (businessKey == null
          || ownerJobId == null
          || !ownerIsActive(businessKey, ownerJobId, ownerTable)) {
        reservations.deleteOne(
            and(eq(ID, businessKey), eq(OWNER_JOB_ID, ownerJobId), eq(OWNER_TABLE, ownerTable)));
      }
    }
  }

  private boolean ownerIsActive(String businessKey, UUID ownerJobId, String ownerTable) {
    if (BusinessKeyReservations.OWNER_TABLE_QUEUE.equals(ownerTable)) {
      return database
              .getCollection("scheduler_job")
              .countDocuments(
                  and(
                      eq(ID, ownerJobId),
                      eq(BUSINESS_KEY, businessKey),
                      in(STATUS, MongoStoreContext.ACTIVE_STATUSES)))
          > 0;
    }
    if (BusinessKeyReservations.OWNER_TABLE_RECURRING.equals(ownerTable)) {
      return database
              .getCollection("scheduler_recurring_job")
              .countDocuments(and(eq(ID, ownerJobId), eq(BUSINESS_KEY, businessKey)))
          > 0;
    }
    return false;
  }

  private void reserveExisting(String businessKey, UUID ownerJobId, String ownerTable) {
    if (businessKey == null || ownerJobId == null) {
      return;
    }
    Bson insert =
        new Document(
            "$setOnInsert",
            new Document(ID, businessKey)
                .append(OWNER_JOB_ID, ownerJobId)
                .append(OWNER_TABLE, ownerTable)
                .append(RESERVED_AT, new Date()));
    RuntimeException duplicateUpsert = null;
    try {
      database
          .getCollection("scheduler_business_key_reservation")
          .updateOne(eq(ID, businessKey), insert, new UpdateOptions().upsert(true));
    } catch (RuntimeException e) {
      if (!new MongoConstraintDetector().isDuplicateKey(e)) {
        throw e;
      }
      // Two nodes may concurrently backfill the same missing _id. Read the winner below: an
      // identical owner is idempotent, while a different owner is a real upgrade conflict.
      duplicateUpsert = e;
    }
    Document reserved =
        database
            .getCollection("scheduler_business_key_reservation")
            .find(eq(ID, businessKey))
            .first();
    if (reserved != null
        && ownerJobId.equals(reserved.get(OWNER_JOB_ID, UUID.class))
        && ownerTable.equals(reserved.getString(OWNER_TABLE))) {
      return;
    }
    throw new IllegalStateException(
        "Business key "
            + businessKey
            + " is active for multiple MongoDB owners; existing reservation="
            + reserved
            + ", conflicting owner="
            + ownerJobId
            + " ("
            + ownerTable
            + ")",
        duplicateUpsert);
  }

  private void createJobPropertiesIndexes() {
    var coll = database.getCollection("scheduler_job_properties");
    // Uniqueness mirror of the SQL PK (job_id, property_key); claim-correctness is not at stake
    // but upsert semantics depend on it, so it is required.
    createRequiredIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending("job_id"), Indexes.ascending("property_key")),
        new IndexOptions().name("uk_job_property").unique(true));
    // Cross-job (property_key, value) filter support for the query layer.
    createRequiredIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending("property_key"), Indexes.ascending("value")),
        new IndexOptions().name("idx_property_kv"));
  }

  private void createJobExtensionStateIndexes() {
    var coll = database.getCollection("scheduler_job_extension_state");
    // Uniqueness mirror of the SQL PK (job_id, namespace); initState duplicate detection and the
    // CAS update path both depend on one row per (job, namespace).
    createRequiredIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending("job_id"), Indexes.ascending("namespace")),
        new IndexOptions().name("uk_job_extension_namespace").unique(true));
    // Key-rotation drain check, mirroring idx_job_encryption_key_id on the job table.
    createIndex(coll, Indexes.ascending("encryption_key_id"), "idx_extension_state_key_id");
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
                new Document(
                        STATUS,
                        new Document("$in", List.of("PENDING", "RUNNING", "PAUSED", "WAITING")))
                    .append(BUSINESS_KEY, new Document("$type", "string"))));
    createIndex(coll, Indexes.ascending(TAGS), "idx_job_tags");
    createIndex(coll, Indexes.ascending(PICKED_BY), "idx_job_picked_by");
    createIndex(coll, Indexes.ascending(DEPENDS_ON), "idx_job_depends_on");
    createIndex(coll, Indexes.ascending(TARGET_CLASS), "idx_job_target_class");
    createIndex(coll, Indexes.ascending(METHOD_NAME), "idx_job_method_name");
    createIndex(coll, Indexes.ascending(CREATED_AT), "idx_job_created_at");
    createIndex(coll, Indexes.ascending(UPDATED_AT), "idx_job_updated_at");
    createIndex(coll, Indexes.ascending(TERMINATED_AT), "idx_job_terminated_at");
    createIndex(coll, Indexes.ascending(JOB_TYPE), "idx_job_type");
    createIndex(coll, Indexes.ascending(SUPERSEDED_BY), "idx_job_superseded_by");

    // Dashboard query indexes (query-layer hardening)
    createIndex(coll, Indexes.ascending(TRACE_CONTEXT + ".traceparent"), "idx_job_traceparent");
    createIndex(coll, Indexes.ascending(CALLER_PRINCIPAL, CREATED_AT), "idx_job_principal_created");
    createIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending(STATUS), Indexes.descending(CREATED_AT)),
        "idx_job_status_created");
    // Signal-waiting indexes are claim-correctness-critical: deliverSignalByKey filters on
    // signal_key+status and the timeout scanner filters on status+signal_timeout. Fail startup
    // rather than silently degrade signal delivery to a collection scan.
    createRequiredIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending(SIGNAL_KEY), Indexes.ascending(STATUS)),
        new IndexOptions().name("idx_signal_key_status"));
    createRequiredIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending(STATUS), Indexes.ascending(SIGNAL_TIMEOUT)),
        new IndexOptions().name("idx_signal_timeout_status"));
    createRequiredIndex(
        coll,
        Indexes.ascending(SIGNAL_DELIVERY_ID),
        new IndexOptions().name("idx_signal_delivery_id"));
  }

  private void createRecurringJobIndexes() {
    var coll = database.getCollection("scheduler_recurring_job");
    // Claim path: filter unpaused rows by next_fire. findOneAndUpdate provides single-document
    // atomicity, equivalent to SQL FOR UPDATE SKIP LOCKED on one row at a time.
    createRequiredIndex(
        coll,
        Indexes.compoundIndex(Indexes.ascending(IS_PAUSED), Indexes.ascending(NEXT_FIRE)),
        new IndexOptions().name(MongoIndexHints.RECURRING_JOB_CLAIM));
    // Defense in depth within the recurring collection. Cross-type uniqueness is authoritative in
    // scheduler_business_key_reservation; this local guard still rejects duplicate masters if a
    // reservation write is ever bypassed. The partial filter lets anonymous masters coexist.
    createRequiredIndex(
        coll,
        Indexes.ascending(BUSINESS_KEY),
        new IndexOptions()
            .name("uk_rec_business_key")
            .unique(true)
            .partialFilterExpression(new Document(BUSINESS_KEY, new Document("$type", "string"))));
    createIndex(coll, Indexes.ascending(TARGET_CLASS), "idx_rec_target_class");
  }

  private void createRecurringJobArchiveIndexes() {
    var coll = database.getCollection("scheduler_recurring_job_archive");
    createIndex(coll, Indexes.ascending(BUSINESS_KEY), "idx_archive_rec_business_key");
    createIndex(coll, Indexes.ascending(ARCHIVED_AT), "idx_archive_rec_archived_at");
  }

  private void createBatchIndexes() {
    var coll = database.getCollection("scheduler_batch");
    // Backs findRecoverableBatchIds, which filters by completion_processed=false on a recurring
    // timer. Without this index, recovery does a full collection scan over every batch ever
    // created.
    createIndex(coll, Indexes.ascending(COMPLETION_PROCESSED), "idx_batch_completion_processed");
  }

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
    createIndex(
        coll,
        Indexes.compoundIndex(
            Indexes.ascending(PARENT_JOB_ID),
            Indexes.ascending(CONDITION_PRIORITY),
            Indexes.ascending(DEFINITION_ORDER)),
        "idx_wfc_evaluation_order");
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
