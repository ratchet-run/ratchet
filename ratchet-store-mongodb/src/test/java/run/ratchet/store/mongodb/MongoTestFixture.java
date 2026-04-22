package run.ratchet.store.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.JobStoreContractFixture;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testcontainers.containers.MongoDBContainer;

/** Shared Testcontainers-based fixture for MongoDB TCK tests. */
public class MongoTestFixture implements JobStoreContractFixture {

  private static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0").withReuse(true);

  static {
    MONGO.start();
  }

  private final MongoClient client;
  private final MongoDatabase database;
  private final MongoJobStore store;
  private final ExecutorService claimExecutor;

  public MongoTestFixture() {
    this.client = MongoClients.create(MONGO.getConnectionString());
    this.database =
        client.getDatabase("ratchet_test_" + UUID.randomUUID().toString().substring(0, 8));
    this.claimExecutor =
        Executors.newCachedThreadPool(
            r -> {
              Thread thread = new Thread(r, "ratchet-mongo-test-claim");
              thread.setDaemon(true);
              return thread;
            });
    this.store = new MongoJobStoreImpl(database, RatchetOptions.defaults(), claimExecutor);
    // @PostConstruct is CDI-only; instantiation here bypasses it, leaving collections without
    // their unique indexes. Initialize explicitly so contract tests see the same schema as a
    // production deployment.
    new MongoCollectionInitializer(database).initialize();
  }

  @Override
  public JobStore store() {
    return store;
  }

  @Override
  public JobEntity newPendingJob() {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.now());
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setPayload(new JobPayload("com.example.TestJob", "execute", "()V", false, List.of()));
    return job;
  }

  @Override
  public JobEntity newBatchParentJob() {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.now());
    job.setJobType(JobExecutionType.BATCH_PARENT);
    job.setPriority(JobPriority.NORMAL);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setPayload(new JobPayload("com.example.BatchJob", "execute", "()V", false, List.of()));
    return job;
  }

  @Override
  public void cleanupStore() {
    database.drop();
  }

  /**
   * Mongo Testcontainers use a standalone {@code mongod}, which does not expose client sessions, so
   * multi-document transactions (and therefore rollback-based test patterns) are unavailable.
   */
  @Override
  public boolean supportsTransactionalRollback() {
    return false;
  }

  /**
   * MongoJobStore signals stale writes by throwing {@link RatchetOptimisticLockException} directly
   * from {@code save()}. This override lets the TCK stale-write contract recognise the Mongo
   * store's version-mismatch signal without the test itself naming the type.
   */
  @Override
  public boolean isStaleWriteException(Throwable t) {
    return t instanceof RatchetOptimisticLockException;
  }
}
