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
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.bson.Document;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.util.StatusClassifier;
import run.ratchet.store.util.TransientStoreExceptions;

/**
 * Shared context passed into every Mongo operation class.
 *
 * <p>Owns the {@link MongoDatabase} handle, collection accessors, status classification helpers,
 * and transient-exception translation.
 */
final class MongoStoreContext {

  private static final String DIALECT = "mongodb";
  private static final MetricsCollector NOOP_METRICS_COLLECTOR = new NoopMetricsCollector();

  static final List<String> EXECUTABLE_JOB_TYPES =
      List.of("SINGLE", "BATCH_CHILD", "CHAIN_STEP", "WORKFLOW_BRANCH");
  static final List<String> ACTIVE_STATUSES = List.of("PENDING", "RUNNING", "PAUSED", "WAITING");
  static final List<String> TERMINAL_STATUSES = List.of("SUCCEEDED", "FAILED", "CANCELED");

  private final MongoClient client;
  private final MongoDatabase database;
  private final MetricsCollector metricsCollector;
  private final int priorityBoostIntervalMinutes;
  private final MongoConstraintDetector constraintDetector = new MongoConstraintDetector();

  MongoStoreContext(MongoClient client, MongoDatabase database) {
    this(client, database, NOOP_METRICS_COLLECTOR, 15);
  }

  MongoStoreContext(MongoClient client, MongoDatabase database, int priorityBoostIntervalMinutes) {
    this(client, database, NOOP_METRICS_COLLECTOR, priorityBoostIntervalMinutes);
  }

  MongoStoreContext(
      MongoClient client,
      MongoDatabase database,
      MetricsCollector metricsCollector,
      int priorityBoostIntervalMinutes) {
    this.client = client;
    this.database = database;
    this.metricsCollector = metricsCollector;
    this.priorityBoostIntervalMinutes = priorityBoostIntervalMinutes;
  }

  static MetricsCollector noopMetricsCollector() {
    return NOOP_METRICS_COLLECTOR;
  }

  static boolean isPollerExecutable(JobExecutionType jobType) {
    return StatusClassifier.isPollerExecutable(jobType);
  }

  static boolean isLiveStatus(JobStatus status) {
    return StatusClassifier.isLiveStatus(status);
  }

  static boolean isTerminalStatus(JobStatus status) {
    return StatusClassifier.isTerminalStatus(status);
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

  int priorityBoostIntervalMinutes() {
    return priorityBoostIntervalMinutes;
  }

  MongoConstraintDetector constraintDetector() {
    return constraintDetector;
  }

  RuntimeException translateTransientStoreException(String operation, RuntimeException e) {
    RatchetTransientStoreException wrapped =
        TransientStoreExceptions.translateOrNull("MongoDB", constraintDetector, operation, e);
    if (wrapped != null) {
      return wrapped;
    }
    if (containsMongoException(e)) {
      return new IllegalStateException("MongoDB store failure during " + operation, e);
    }
    return e;
  }

  <T> T timedStoreOperation(
      String operation, Supplier<T> action, Function<T, String> outcomeFunction) {
    long startNanos = System.nanoTime();
    try {
      T result = action.get();
      recordStoreOperation(operation, outcomeFunction.apply(result), startNanos);
      return result;
    } catch (RatchetTransientStoreException e) {
      recordStoreOperation(operation, "transient_failure", startNanos);
      throw e;
    } catch (RuntimeException e) {
      RuntimeException translated = translateTransientStoreException(operation, e);
      recordStoreOperation(
          operation,
          translated instanceof RatchetTransientStoreException ? "transient_failure" : "failure",
          startNanos);
      throw translated;
    }
  }

  private void recordStoreOperation(String operation, String outcome, long startNanos) {
    metricsCollector.storeOperation(DIALECT, operation, outcome, System.nanoTime() - startNanos);
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

  private static final class NoopMetricsCollector implements MetricsCollector {
    @Override
    public void jobStarted(UUID jobId, JobType type, JobPriority priority) {}

    @Override
    public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {}

    @Override
    public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {}

    @Override
    public void successFinalizationRetried(UUID jobId, JobType type) {}

    @Override
    public void successFinalizationMinimal(UUID jobId, JobType type) {}

    @Override
    public void successFinalizationStuck(UUID jobId, JobType type) {}

    @Override
    public void claimTransientFailure(String executionType) {}

    @Override
    public void jobsClaimed(String executionType, int claimedCount) {}

    @Override
    public void gateRejected(String executionType, String gateStatus) {}

    @Override
    public void localWakeup(String source) {}

    @Override
    public void clusterWakeupPublished(String transport, String outcome) {}

    @Override
    public void clusterWakeupReceived(String transport, String outcome) {}
  }
}
