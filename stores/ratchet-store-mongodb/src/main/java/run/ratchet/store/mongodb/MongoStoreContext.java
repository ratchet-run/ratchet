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

import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import org.bson.Document;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.ConstraintDetector;
import run.ratchet.store.context.AbstractStoreContext;

/**
 * Shared context passed into every Mongo operation class.
 *
 * <p>Owns the {@link MongoDatabase} handle and collection accessors on top of the shared {@link
 * AbstractStoreContext} scaffolding. Mongo adds a translation branch for non-transient {@link
 * MongoException}s via {@link #additionalTranslation}.
 */
final class MongoStoreContext extends AbstractStoreContext {

  static final List<String> EXECUTABLE_JOB_TYPES =
      List.of("SINGLE", "BATCH_CHILD", "CHAIN_STEP", "WORKFLOW_BRANCH");
  static final List<String> ACTIVE_STATUSES = List.of("PENDING", "RUNNING", "PAUSED", "WAITING");
  static final List<String> TERMINAL_STATUSES = List.of("SUCCEEDED", "FAILED", "CANCELED");

  private final MongoClient client;
  private final MongoDatabase database;
  private final MongoConstraintDetector constraintDetector = new MongoConstraintDetector();

  MongoStoreContext(MongoClient client, MongoDatabase database) {
    this(client, database, noopMetricsCollector(), 15);
  }

  MongoStoreContext(MongoClient client, MongoDatabase database, int priorityBoostIntervalMinutes) {
    this(client, database, noopMetricsCollector(), priorityBoostIntervalMinutes);
  }

  MongoStoreContext(
      MongoClient client,
      MongoDatabase database,
      MetricsCollector metricsCollector,
      int priorityBoostIntervalMinutes) {
    super(metricsCollector, priorityBoostIntervalMinutes);
    this.client = client;
    this.database = database;
  }

  /**
   * Start a new session bound to this context's client. Caller is responsible for closing the
   * session (try-with-resources). Used by compound operations that need multi-document atomicity
   * via {@link ClientSession#withTransaction(com.mongodb.client.TransactionBody)}.
   *
   * <p>Requires the MongoDB deployment to be a replica set or sharded cluster; standalone mongod
   * does not support sessions.
   */
  ClientSession startSession() {
    return client.startSession();
  }

  MongoDatabase database() {
    return database;
  }

  @Override
  protected String dialectMetric() {
    return "mongodb";
  }

  @Override
  protected String dialectLabel() {
    return "MongoDB";
  }

  @Override
  public ConstraintDetector constraintDetector() {
    return constraintDetector;
  }

  @Override
  protected RuntimeException additionalTranslation(String operation, RuntimeException e) {
    if (containsMongoException(e)) {
      return new IllegalStateException("MongoDB store failure during " + operation, e);
    }
    return e;
  }

  private static boolean containsMongoException(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof MongoException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  MongoCollection<Document> jobs() {
    return database.getCollection("scheduler_job");
  }

  MongoCollection<Document> batches() {
    return database.getCollection("scheduler_batch");
  }

  MongoCollection<Document> batchMetrics() {
    return database.getCollection("scheduler_batch_metrics");
  }

  MongoCollection<Document> executions() {
    return database.getCollection("scheduler_job_execution");
  }

  MongoCollection<Document> jobLogs() {
    return database.getCollection("scheduler_job_log");
  }

  MongoCollection<Document> archives() {
    return database.getCollection("scheduler_job_archive");
  }

  MongoCollection<Document> locks() {
    return database.getCollection("scheduler_lock");
  }

  MongoCollection<Document> nodes() {
    return database.getCollection("scheduler_node");
  }

  MongoCollection<Document> workflowConditions() {
    return database.getCollection("scheduler_workflow_condition");
  }

  MongoCollection<Document> dlqAlerts() {
    return database.getCollection("scheduler_dlq_alerts");
  }

  MongoCollection<Document> resourceLimits() {
    return database.getCollection("scheduler_resource_limit");
  }

  MongoCollection<Document> resourcePermits() {
    return database.getCollection("scheduler_resource_permit");
  }

  MongoCollection<Document> recurringJobs() {
    return database.getCollection("scheduler_recurring_job");
  }

  MongoCollection<Document> recurringJobArchive() {
    return database.getCollection("scheduler_recurring_job_archive");
  }

  MongoCollection<Document> jobProperties() {
    return database.getCollection("scheduler_job_properties");
  }

  MongoCollection<Document> jobExtensionState() {
    return database.getCollection("scheduler_job_extension_state");
  }
}
