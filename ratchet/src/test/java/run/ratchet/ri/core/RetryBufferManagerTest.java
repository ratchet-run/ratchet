package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.transaction.Transactional;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobBatchStatusStore;

@ExtendWith(MockitoExtension.class)
class RetryBufferManagerTest {

  @Mock private DeadLetterService deadLetterService;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private NodeIdentityProvider nodeIdentityProvider;

  private RetryBufferManager manager;

  private static JobEntity job(
      long id, JobExecutionType type, JobPriority priority, Instant scheduledTime) {
    JobEntity j = new JobEntity();
    j.setId(new UUID(0L, id));
    j.setJobType(type);
    j.setPriority(priority);
    j.setScheduledTime(scheduledTime);
    return j;
  }

  private static JobEntity standardJob(long id) {
    return job(id, JobExecutionType.SINGLE, JobPriority.NORMAL, Instant.now());
  }

  @BeforeEach
  void setUp() {
    manager = new RetryBufferManager(deadLetterService, jobBatchStatusStore, nodeIdentityProvider);
  }

  @Test
  void offer_underCapacity_returnsTrue() {
    assertTrue(manager.offer(standardJob(1L)));
  }

  @Test
  void transactionBoundary_onlyAppliesToPersistentFlush() throws Exception {
    assertNull(RetryBufferManager.class.getAnnotation(Transactional.class));

    Method flushOnShutdown = RetryBufferManager.class.getMethod("flushOnShutdown");
    assertNotNull(flushOnShutdown.getAnnotation(Transactional.class));
  }

  @Test
  void offer_atCapacity_returnsFalse() {
    for (int i = 0; i < RetryBufferManager.MAX_BUFFER_SIZE_PER_TYPE; i++) {
      assertTrue(manager.offer(standardJob(i)));
    }

    assertFalse(manager.offer(standardJob(9999L)));
  }

  @Test
  void totalSize_aggregatesAllJobTypes() {
    manager.offer(job(1L, JobExecutionType.SINGLE, JobPriority.NORMAL, Instant.now()));
    manager.offer(job(2L, JobExecutionType.BATCH_CHILD, JobPriority.NORMAL, Instant.now()));
    manager.offer(job(3L, JobExecutionType.CHAIN_STEP, JobPriority.NORMAL, Instant.now()));

    assertEquals(3, manager.totalSize());
  }

  @Test
  void pollFromBuffer_highPriorityBeforeNormal() {
    Instant now = Instant.now();
    manager.offer(job(1L, JobExecutionType.SINGLE, JobPriority.NORMAL, now));
    manager.offer(job(2L, JobExecutionType.SINGLE, JobPriority.HIGH, now));

    RetryBufferManager.BufferedClaim first = manager.pollFromBuffer(JobExecutionType.SINGLE);
    assertNotNull(first);
    assertEquals(new UUID(0L, 2L), first.jobId());

    RetryBufferManager.BufferedClaim second = manager.pollFromBuffer(JobExecutionType.SINGLE);
    assertNotNull(second);
    assertEquals(new UUID(0L, 1L), second.jobId());
  }

  @Test
  void pollFromBuffer_samePriority_orderedByScheduledTime() {
    Instant earlier = Instant.parse("2025-01-01T00:00:00Z");
    Instant later = Instant.parse("2025-01-01T01:00:00Z");

    manager.offer(job(1L, JobExecutionType.SINGLE, JobPriority.NORMAL, later));
    manager.offer(job(2L, JobExecutionType.SINGLE, JobPriority.NORMAL, earlier));

    RetryBufferManager.BufferedClaim first = manager.pollFromBuffer(JobExecutionType.SINGLE);
    assertNotNull(first);
    assertEquals(new UUID(0L, 2L), first.jobId(), "Earlier scheduled job should be polled first");
  }

  @Test
  void pollBatchFromBuffer_respectsLimitAndPriorityOrder() {
    Instant now = Instant.parse("2025-01-01T00:00:00Z");
    manager.offer(job(1L, JobExecutionType.SINGLE, JobPriority.NORMAL, now.plusSeconds(20)));
    manager.offer(job(2L, JobExecutionType.SINGLE, JobPriority.HIGH, now.plusSeconds(30)));
    manager.offer(job(3L, JobExecutionType.SINGLE, JobPriority.HIGH, now.plusSeconds(10)));

    List<RetryBufferManager.BufferedClaim> claims =
        manager.pollBatchFromBuffer(JobExecutionType.SINGLE, 2);

    assertEquals(2, claims.size());
    assertEquals(new UUID(0L, 3L), claims.get(0).jobId());
    assertEquals(new UUID(0L, 2L), claims.get(1).jobId());
    assertEquals(1, manager.totalSize());
  }

  @Test
  void pollBatchFromBuffer_nonPositiveLimitReturnsEmptyList() {
    manager.offer(standardJob(1L));

    assertTrue(manager.pollBatchFromBuffer(JobExecutionType.SINGLE, 0).isEmpty());
    assertTrue(manager.pollBatchFromBuffer(JobExecutionType.SINGLE, -10).isEmpty());
    assertEquals(1, manager.totalSize());
  }

  @Test
  void getBuffer_returnsUnmodifiableViewOfExistingBuffer() {
    manager.offer(standardJob(1L));

    Collection<RetryBufferManager.BufferedClaim> buffer =
        manager.getBuffer(JobExecutionType.SINGLE);

    assertEquals(1, buffer.size());
    assertThrows(
        UnsupportedOperationException.class,
        () -> buffer.add(RetryBufferManager.BufferedClaim.from(standardJob(2L))));
  }

  @Test
  void getBuffer_unknownTypeReturnsEmptyCollection() {
    assertTrue(manager.getBuffer(null).isEmpty());
  }

  @Test
  void isBufferEmpty_emptyBuffer_returnsTrue() {
    assertTrue(manager.isBufferEmpty(JobExecutionType.SINGLE));
  }

  @Test
  void isBufferEmpty_nonEmptyBuffer_returnsFalse() {
    manager.offer(standardJob(1L));
    assertFalse(manager.isBufferEmpty(JobExecutionType.SINGLE));
  }

  @Test
  void forceOffer_bypassesNormalLimit() {
    for (int i = 0; i < RetryBufferManager.MAX_BUFFER_SIZE_PER_TYPE; i++) {
      manager.offer(standardJob(i));
    }

    assertFalse(manager.offer(standardJob(9998L)));
    assertTrue(manager.forceOffer(standardJob(9999L)));
    assertEquals(RetryBufferManager.MAX_BUFFER_SIZE_PER_TYPE + 1, manager.totalSize());
  }

  @Test
  void forceOffer_atHardCap_movesToDlq() {
    for (int i = 0; i < RetryBufferManager.HARD_CAP_PER_TYPE; i++) {
      manager.forceOffer(standardJob(i));
    }

    JobEntity overflow = standardJob(99999L);
    assertFalse(manager.forceOffer(overflow));
    verify(deadLetterService).moveToDlq(eq(overflow), any(IllegalStateException.class));
  }

  @Test
  void forceOffer_atHardCap_dlqFailureKeepsClaimBuffered() {
    for (int i = 0; i < RetryBufferManager.HARD_CAP_PER_TYPE; i++) {
      manager.forceOffer(standardJob(i));
    }
    doThrow(new RuntimeException("dlq unavailable"))
        .when(deadLetterService)
        .moveToDlq(any(JobEntity.class), any(IllegalStateException.class));

    JobEntity overflow = standardJob(99999L);
    assertTrue(manager.forceOffer(overflow));

    verify(deadLetterService).moveToDlq(any(JobEntity.class), any(IllegalStateException.class));
    assertEquals(RetryBufferManager.HARD_CAP_PER_TYPE + 1, manager.totalSize());
    assertTrue(
        manager.getBuffer(JobExecutionType.SINGLE).stream()
            .anyMatch(claim -> overflow.getId().equals(claim.jobId())));
  }

  @Test
  void forceOffer_atHardCap_releasesBufferLockBeforeMovingToDlq() throws Exception {
    for (int i = 0; i < RetryBufferManager.HARD_CAP_PER_TYPE; i++) {
      manager.forceOffer(standardJob(i));
    }

    CountDownLatch dlqEntered = new CountDownLatch(1);
    CountDownLatch releaseDlq = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              dlqEntered.countDown();
              assertTrue(releaseDlq.await(5, TimeUnit.SECONDS));
              return null;
            })
        .when(deadLetterService)
        .moveToDlq(any(JobEntity.class), any(IllegalStateException.class));

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Boolean> overflowResult =
          executor.submit(() -> manager.forceOffer(standardJob(99999L)));
      assertTrue(dlqEntered.await(2, TimeUnit.SECONDS));

      Future<Boolean> offerWhileDlqIsBlocked =
          executor.submit(() -> manager.offer(standardJob(100000L)));
      assertFalse(offerWhileDlqIsBlocked.get(500, TimeUnit.MILLISECONDS));

      releaseDlq.countDown();
      assertFalse(overflowResult.get(2, TimeUnit.SECONDS));
    } finally {
      releaseDlq.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void flushOnShutdown_resetsJobsToPending() {
    manager.offer(standardJob(1L));
    manager.offer(standardJob(2L));

    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(jobBatchStatusStore.resetRunningJob(new UUID(0L, 1L), "node-1")).thenReturn(true);
    when(jobBatchStatusStore.resetRunningJob(new UUID(0L, 2L), "node-1")).thenReturn(true);

    manager.flushOnShutdown();

    verify(jobBatchStatusStore).resetRunningJob(new UUID(0L, 1L), "node-1");
    verify(jobBatchStatusStore).resetRunningJob(new UUID(0L, 2L), "node-1");
    assertTrue(manager.isBufferEmpty(JobExecutionType.SINGLE));
  }

  @Test
  void flushOnShutdown_doesNotOverwriteTerminalJobs() {
    manager.offer(standardJob(1L));
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(jobBatchStatusStore.resetRunningJob(new UUID(0L, 1L), "node-1")).thenReturn(false);

    manager.flushOnShutdown();

    verify(jobBatchStatusStore).resetRunningJob(new UUID(0L, 1L), "node-1");
    verify(jobBatchStatusStore, never()).resetRunningJob(new UUID(0L, 1L), "other-node");
    assertTrue(manager.isBufferEmpty(JobExecutionType.SINGLE));
  }

  @Test
  void flushOnShutdown_resetExceptionIsSwallowedAndBufferIsCleared() {
    manager.offer(standardJob(1L));
    manager.offer(standardJob(2L));
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    doThrow(new RuntimeException("store unavailable"))
        .when(jobBatchStatusStore)
        .resetRunningJob(new UUID(0L, 1L), "node-1");
    when(jobBatchStatusStore.resetRunningJob(new UUID(0L, 2L), "node-1")).thenReturn(true);

    manager.flushOnShutdown();

    verify(jobBatchStatusStore).resetRunningJob(new UUID(0L, 1L), "node-1");
    verify(jobBatchStatusStore).resetRunningJob(new UUID(0L, 2L), "node-1");
    assertTrue(manager.isBufferEmpty(JobExecutionType.SINGLE));
  }
}
