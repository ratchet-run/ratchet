package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;

class ConcurrentClaimIT extends BaseDocumentStoreIT {

  @Test
  void multipleNodes_noDoubleClaims() throws Exception {
    int jobCount = 50;
    int nodeCount = 5;

    for (int i = 0; i < jobCount; i++) {
      store().save(newPendingJob());
    }

    Set<UUID> allClaimedIds = ConcurrentHashMap.newKeySet();
    List<UUID> duplicates = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(nodeCount);

    ExecutorService executor = Executors.newFixedThreadPool(nodeCount);
    try {
      List<Future<?>> futures = new ArrayList<>(nodeCount);
      for (int n = 0; n < nodeCount; n++) {
        final String nodeId = "node-" + n;
        futures.add(
            executor.submit(
                () -> {
                  try {
                    startLatch.await();
                    List<JobEntity> claimed = store().claimNextBatch(jobCount, nodeId);
                    for (JobEntity job : claimed) {
                      if (!allClaimedIds.add(job.getId())) {
                        duplicates.add(job.getId());
                      }
                    }
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  } finally {
                    doneLatch.countDown();
                  }
                }));
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Claiming should complete within 30s");
      for (Future<?> future : futures) {
        future.get(5, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdown();
      assertTrue(
          executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should terminate cleanly");
      if (!executor.isTerminated()) {
        executor.shutdownNow();
      }
    }

    assertEquals(0, duplicates.size(), "No job should be claimed by multiple nodes");
    assertEquals(jobCount, allClaimedIds.size(), "All jobs should be claimed");
  }

  @Test
  void claimBatchLimit_respectsLimit() {
    for (int i = 0; i < 20; i++) {
      store().save(newPendingJob());
    }

    List<JobEntity> batch = store().claimNextBatch(5, "node-1");
    assertEquals(5, batch.size());

    List<JobEntity> remaining = store().claimNextBatch(100, "node-2");
    assertEquals(15, remaining.size());
  }
}
