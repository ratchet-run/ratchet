package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.lang.reflect.Field;
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
import org.testcontainers.mongodb.MongoDBContainer;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.JobEntity;

/**
 * Multi-client claim race test. Unlike {@link ConcurrentClaimIT}, which races multiple threads
 * through a single {@link MongoJobStore} instance, this test constructs two <em>independent</em>
 * {@link MongoJobStore} instances with their own {@link MongoClient} connections against the same
 * database. That exercises the true cross-client atomicity of {@code findOneAndUpdate(status=
 * PENDING)} — the invariant that matters when two physical nodes race for the same job.
 */
class MultiClientClaimIT extends BaseDocumentStoreIT {

  private MongoClient clientB;
  private MongoDatabase dbB;
  private MongoJobStore storeB;

  private static void race(
      MongoJobStore store,
      String nodeId,
      CountDownLatch start,
      CountDownLatch done,
      Set<UUID> allClaimed,
      List<UUID> duplicates) {
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

  private static String mongoConnectionString() {
    try {
      Field mongoField = BaseDocumentStoreIT.class.getDeclaredField("MONGO");
      mongoField.setAccessible(true);
      MongoDBContainer mongo = (MongoDBContainer) mongoField.get(null);
      return mongo.getConnectionString();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to read BaseDocumentStoreIT Mongo container", e);
    }
  }

  private static ClaimRaceResult raceClaims(MongoJobStore storeA, MongoJobStore storeB)
      throws InterruptedException {
    Set<UUID> allClaimed = ConcurrentHashMap.newKeySet();
    List<UUID> duplicates = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(2);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      pool.submit(() -> race(storeA, "node-A", start, done, allClaimed, duplicates));
      pool.submit(() -> race(storeB, "node-B", start, done, allClaimed, duplicates));

      start.countDown();
      assertTrue(done.await(30, TimeUnit.SECONDS), "both clients must finish within 30s");
    } finally {
      pool.shutdownNow();
    }
    return new ClaimRaceResult(allClaimed, duplicates);
  }

  @BeforeEach
  void setUpSecondClient() {
    clientB = MongoClientFactory.create(mongoConnectionString());
    dbB = clientB.getDatabase(database().getName());
    storeB = new MongoJobStoreImpl(clientB, dbB, RatchetOptions.defaults());
  }

  @AfterEach
  void tearDownSecondClient() {
    if (clientB != null) {
      clientB.close();
    }
  }

  @Test
  void twoClients_noDuplicateClaims() throws InterruptedException {
    int jobCount = 100;
    for (int i = 0; i < jobCount; i++) {
      store().save(newPendingJob());
    }

    ClaimRaceResult result = raceClaims(store(), storeB);

    assertEquals(
        0,
        result.duplicates().size(),
        "no job should be claimed by both clients: " + result.duplicates());
    assertEquals(jobCount, result.allClaimed().size(), "every job should be claimed exactly once");
  }

  @Test
  void failedJob_requiresExplicitResetBeforeConcurrentClaim() throws InterruptedException {
    JobEntity failed = newPendingJob();
    failed.setStatus(JobStatus.FAILED);
    store().save(failed);

    ClaimRaceResult beforeReset = raceClaims(store(), storeB);

    assertEquals(0, beforeReset.duplicates().size(), "failed job should not be claimed");
    assertTrue(beforeReset.allClaimed().isEmpty(), "failed job must not be claimed before reset");
    assertEquals(JobStatus.FAILED, store().findById(failed.getId()).orElseThrow().getStatus());

    assertTrue(store().resetFailedToPending(failed.getId()), "failed job should reset to pending");

    ClaimRaceResult afterReset = raceClaims(store(), storeB);

    assertEquals(
        0,
        afterReset.duplicates().size(),
        "reset job should not be claimed by both clients: " + afterReset.duplicates());
    assertEquals(Set.of(failed.getId()), afterReset.allClaimed());
    assertEquals(JobStatus.RUNNING, store().findById(failed.getId()).orElseThrow().getStatus());
  }

  private record ClaimRaceResult(Set<UUID> allClaimed, List<UUID> duplicates) {}
}
