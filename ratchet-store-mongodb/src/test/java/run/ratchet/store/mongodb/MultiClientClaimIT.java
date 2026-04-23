package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;

/**
 * Multi-client claim race test. Unlike {@link ConcurrentClaimIT}, which races multiple threads
 * through a single {@link MongoJobStore} instance, this test constructs two <em>independent</em>
 * {@link MongoJobStore} instances with their own {@link MongoClient} connections against the same
 * database. That exercises the true cross-client atomicity of {@code findOneAndUpdate(status=
 * PENDING)} — the invariant that matters when two physical nodes race for the same job.
 */
class MultiClientClaimIT {

  private static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7.0").withReuse(true);

  static {
    MONGO.start();
  }

  private MongoClient clientA;
  private MongoClient clientB;
  private MongoDatabase dbA;
  private MongoDatabase dbB;
  private MongoJobStore storeA;
  private MongoJobStore storeB;
  private ExecutorService claimExecutorA;
  private ExecutorService claimExecutorB;

  private static void race(
      MongoJobStore store,
      String nodeId,
      CountDownLatch start,
      CountDownLatch done,
      Set<Long> allClaimed,
      List<Long> duplicates) {
    try {
      start.await();
      for (int attempt = 0; attempt < 20; attempt++) {
        List<JobEntity> batch = store.claimNextBatch(20, nodeId);
        if (batch.isEmpty()) {
          break;
        }
        for (JobEntity job : batch) {
          if (!allClaimed.add(job.getId())) {
            duplicates.add(job.getId());
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      done.countDown();
    }
  }

  private static JobEntity newPendingJob() {
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

  @BeforeEach
  void setUp() {
    String dbName = "ratchet_multi_" + UUID.randomUUID().toString().substring(0, 8);
    clientA = MongoClients.create(MONGO.getConnectionString());
    clientB = MongoClients.create(MONGO.getConnectionString());
    dbA = clientA.getDatabase(dbName);
    dbB = clientB.getDatabase(dbName);
    claimExecutorA = Executors.newCachedThreadPool();
    claimExecutorB = Executors.newCachedThreadPool();
    storeA = new MongoJobStoreImpl(clientA, dbA, RatchetOptions.defaults(), claimExecutorA);
    storeB = new MongoJobStoreImpl(clientB, dbB, RatchetOptions.defaults(), claimExecutorB);
    new MongoCollectionInitializer(dbA).initialize();
  }

  @AfterEach
  void tearDown() {
    dbA.drop();
    clientA.close();
    clientB.close();
    claimExecutorA.shutdownNow();
    claimExecutorB.shutdownNow();
  }

  @Test
  void twoClients_noDuplicateClaims() throws InterruptedException {
    int jobCount = 100;
    for (int i = 0; i < jobCount; i++) {
      storeA.save(newPendingJob());
    }

    Set<Long> allClaimed = ConcurrentHashMap.newKeySet();
    List<Long> duplicates = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(2);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    pool.submit(() -> race(storeA, "node-A", start, done, allClaimed, duplicates));
    pool.submit(() -> race(storeB, "node-B", start, done, allClaimed, duplicates));

    start.countDown();
    assertTrue(done.await(30, TimeUnit.SECONDS), "both clients must finish within 30s");
    pool.shutdown();

    assertEquals(0, duplicates.size(), "no job should be claimed by both clients: " + duplicates);
    assertEquals(jobCount, allClaimed.size(), "every job should be claimed exactly once");
  }
}
