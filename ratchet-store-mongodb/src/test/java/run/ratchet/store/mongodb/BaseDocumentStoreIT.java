package run.ratchet.store.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.api.JobStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Base class for MongoDB store integration tests.
 *
 * <p>Manages the lifecycle of a shared Testcontainer; each test gets a fresh database and store
 * instance. Subclasses access the store via {@link #store()} and create entities via the factory
 * methods.
 */
public abstract class BaseDocumentStoreIT {

  // 2-minute startup timeout absorbs replica-set bootstrap variance on busy hosts; the default
  // 60s timeout would race the "waiting for connections" log line under contention.
  private static final MongoDBContainer MONGO =
      new MongoDBContainer("mongo:7.0")
          .withReuse(true)
          .waitingFor(
              Wait.forLogMessage("(?i).*waiting for connections.*", 1)
                  .withStartupTimeout(Duration.ofMinutes(2)));

  private MongoClient client;
  private MongoDatabase database;
  private MongoJobStore store;
  private ExecutorService claimExecutor;

  @BeforeAll
  static void startContainer() {
    if (!MONGO.isRunning()) {
      MONGO.start();
    }
  }

  @BeforeEach
  void setUp() {
    client = MongoClientFactory.create(MONGO.getConnectionString());
    database = client.getDatabase("ratchet_it_" + UUID.randomUUID().toString().substring(0, 8));
    claimExecutor = Executors.newCachedThreadPool();
    store = new MongoJobStoreImpl(client, database, RatchetOptions.defaults(), claimExecutor);
    // Store collection initialization is @PostConstruct, which only fires inside a CDI container.
    // Plain-new instantiation in test fixtures bypasses it, so the unique indexes on
    // idempotency_key / business_key that IdempotencyIT relies on never get created. Call the
    // package-private initializer explicitly here. createIndex is idempotent per MongoDB semantics.
    new MongoCollectionInitializer(database).initialize();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
    }
    if (client != null) {
      client.close();
    }
    if (claimExecutor != null) {
      claimExecutor.shutdownNow();
    }
  }

  protected MongoJobStore store() {
    return store;
  }

  protected MongoDatabase database() {
    return database;
  }

  protected JobEntity newPendingJob() {
    return newPendingJob(JobPriority.NORMAL);
  }

  protected JobEntity newPendingJob(JobPriority priority) {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.now());
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(priority);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setPayload(new JobPayload("com.example.TestJob", "execute", "()V", false, List.of()));
    return job;
  }

  protected JobEntity newBatchParentJob() {
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

  protected JobEntity newBatchChildJob() {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.now());
    job.setJobType(JobExecutionType.BATCH_CHILD);
    job.setPriority(JobPriority.NORMAL);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setPayload(new JobPayload("com.example.BatchChildJob", "execute", "()V", false, List.of()));
    return job;
  }

  protected JobEntity newChainStepJob() {
    JobEntity job = new JobEntity();
    job.setStatus(JobStatus.PENDING);
    job.setScheduledTime(Instant.now());
    job.setJobType(JobExecutionType.CHAIN_STEP);
    job.setPriority(JobPriority.NORMAL);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setPayload(new JobPayload("com.example.ChainStepJob", "execute", "()V", false, List.of()));
    return job;
  }
}
