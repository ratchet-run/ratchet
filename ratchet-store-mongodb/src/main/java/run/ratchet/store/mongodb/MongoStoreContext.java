package run.ratchet.store.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.api.JobStatus;
import java.util.List;
import org.bson.Document;

/**
 * Shared context passed into every Mongo operation class.
 *
 * <p>Owns the {@link MongoDatabase} handle, collection accessors, status classification helpers,
 * and transient-exception translation. Mirrors {@code PostgresqlStoreContext}: no metrics hook on
 * this dialect yet.
 */
final class MongoStoreContext {

  static final List<String> EXECUTABLE_JOB_TYPES =
      List.of("SINGLE", "BATCH_CHILD", "CHAIN_STEP", "WORKFLOW_BRANCH");
  static final List<String> ACTIVE_STATUSES = List.of("PENDING", "RUNNING", "PAUSED", "WAITING");
  static final List<String> TERMINAL_STATUSES = List.of("SUCCEEDED", "FAILED", "CANCELED");

  private final MongoClient client;
  private final MongoDatabase database;
  private final int priorityBoostIntervalMinutes;
  private final MongoConstraintDetector constraintDetector = new MongoConstraintDetector();

  MongoStoreContext(MongoClient client, MongoDatabase database) {
    this(client, database, 15);
  }

  MongoStoreContext(MongoClient client, MongoDatabase database, int priorityBoostIntervalMinutes) {
    this.client = client;
    this.database = database;
    this.priorityBoostIntervalMinutes = priorityBoostIntervalMinutes;
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

  static boolean isPollerExecutable(JobExecutionType jobType) {
    return jobType == JobExecutionType.SINGLE
        || jobType == JobExecutionType.BATCH_CHILD
        || jobType == JobExecutionType.CHAIN_STEP
        || jobType == JobExecutionType.WORKFLOW_BRANCH;
  }

  static boolean isLiveStatus(JobStatus status) {
    return status == JobStatus.PENDING
        || status == JobStatus.RUNNING
        || status == JobStatus.PAUSED
        || status == JobStatus.WAITING;
  }

  static boolean isTerminalStatus(JobStatus status) {
    return status == JobStatus.SUCCEEDED
        || status == JobStatus.FAILED
        || status == JobStatus.CANCELED;
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
    if (constraintDetector.isDeadlock(e) || constraintDetector.isTransientConnectionFailure(e)) {
      return new RatchetTransientStoreException(
          "Transient MongoDB store concurrency failure during " + operation, e);
    }
    return e;
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
}
