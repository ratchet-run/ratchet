package run.ratchet.store.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.mongodb.MongoDBContainer;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.JobStoreContractFixture;

/** Shared Testcontainers-based fixture for MongoDB TCK tests. */
public class MongoTestFixture implements JobStoreContractFixture, AutoCloseable {

  // Replica-set mode is required for multi-document transactions (signal delivery, permit
  // acquisition) and retryable writes. 2-minute timeout absorbs RS bootstrap variance on busy
  // hosts; the default 60s would race the "waiting for connections" log line under contention.
  private static final MongoDBContainer MONGO =
      new MongoDBContainer("mongo:7.0")
          .withReplicaSet()
          .waitingFor(
              Wait.forLogMessage("(?i).*waiting for connections.*", 1)
                  .withStartupTimeout(Duration.ofMinutes(2)));

  // One MongoClient is shared across all 22 contract test classes. Each class previously
  // instantiated its own client (default 100-conn pool + its own SDAM monitor thread), which
  // overwhelmed the replica-set primary and caused indefinite server-selection hangs. A bounded
  // serverSelectionTimeout converts any future overload into a fast MongoTimeoutException
  // instead of a 10-minute deadlock.
  private static final MongoClient CLIENT;

  // Shared async executor for MongoJobStoreImpl claim work — daemon threads, JVM-lifetime.
  private static final ExecutorService CLAIM_EXECUTOR;
  private static final AtomicBoolean SHARED_CLOSED = new AtomicBoolean();

  static {
    MONGO.start();
    MongoClientSettings settings =
        MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(MONGO.getConnectionString()))
            .uuidRepresentation(UuidRepresentation.STANDARD)
            .applyToClusterSettings(b -> b.serverSelectionTimeout(15, TimeUnit.SECONDS))
            .applyToSocketSettings(b -> b.connectTimeout(10, TimeUnit.SECONDS))
            .build();
    CLIENT = MongoClients.create(settings);
    CLAIM_EXECUTOR =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "ratchet-mongo-test-claim");
              t.setDaemon(true);
              return t;
            });
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(MongoTestFixture::closeSharedResources, "ratchet-mongo-test-shutdown"));
  }

  private final MongoDatabase database;
  private final MongoJobStore store;
  private final AtomicBoolean closed = new AtomicBoolean();

  public MongoTestFixture() {
    this.database =
        CLIENT.getDatabase("ratchet_test_" + UUID.randomUUID().toString().substring(0, 8));
    this.store = new MongoJobStoreImpl(CLIENT, database, RatchetOptions.defaults(), CLAIM_EXECUTOR);
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

  // Wipe every collection's contents but keep their indexes. Avoids the per-method
  // database.drop() + initialize() cycle, which previously ran ~30 index creates against the
  // replica set for every single test method across 22 contract classes.
  @Override
  public void cleanupStore() {
    for (String name : database.listCollectionNames()) {
      database.getCollection(name).deleteMany(new Document());
    }
  }

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

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    // Drop the per-class database so the shared container does not accumulate state. The
    // shared CLIENT and CLAIM_EXECUTOR live for the JVM lifetime — closing them here would
    // break every contract class that runs after this one.
    database.drop();
  }

  private static void closeSharedResources() {
    if (!SHARED_CLOSED.compareAndSet(false, true)) {
      return;
    }
    try {
      CLIENT.close();
    } finally {
      try {
        CLAIM_EXECUTOR.shutdownNow();
        CLAIM_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        MONGO.close();
      }
    }
  }
}
