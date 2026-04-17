package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.api.JobPriority;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobStatusStore;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RetryBufferManagerTest {

  @Mock private DeadLetterService deadLetterService;
  @Mock private JobStatusStore jobStatusStore;
  @Mock private NodeIdentityProvider nodeIdentityProvider;

  private RetryBufferManager manager;

  @BeforeEach
  void setUp() {
    manager = new RetryBufferManager(deadLetterService, jobStatusStore, nodeIdentityProvider);
  }

  private static JobEntity job(
      long id, JobExecutionType type, JobPriority priority, Instant scheduledTime) {
    JobEntity j = new JobEntity();
    j.setId(id);
    j.setJobType(type);
    j.setPriority(priority);
    j.setScheduledTime(scheduledTime);
    return j;
  }

  private static JobEntity standardJob(long id) {
    return job(id, JobExecutionType.SINGLE, JobPriority.NORMAL, Instant.now());
  }

  @Test
  void offer_underCapacity_returnsTrue() {
    assertTrue(manager.offer(standardJob(1L)));
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
    assertEquals(2L, first.jobId());

    RetryBufferManager.BufferedClaim second = manager.pollFromBuffer(JobExecutionType.SINGLE);
    assertNotNull(second);
    assertEquals(1L, second.jobId());
  }

  @Test
  void pollFromBuffer_samePriority_orderedByScheduledTime() {
    Instant earlier = Instant.parse("2025-01-01T00:00:00Z");
    Instant later = Instant.parse("2025-01-01T01:00:00Z");

    manager.offer(job(1L, JobExecutionType.SINGLE, JobPriority.NORMAL, later));
    manager.offer(job(2L, JobExecutionType.SINGLE, JobPriority.NORMAL, earlier));

    RetryBufferManager.BufferedClaim first = manager.pollFromBuffer(JobExecutionType.SINGLE);
    assertNotNull(first);
    assertEquals(2L, first.jobId(), "Earlier scheduled job should be polled first");
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
  void flushOnShutdown_resetsJobsToPending() {
    manager.offer(standardJob(1L));
    manager.offer(standardJob(2L));

    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(jobStatusStore.resetRunningJob(1L, "node-1")).thenReturn(true);
    when(jobStatusStore.resetRunningJob(2L, "node-1")).thenReturn(true);

    manager.flushOnShutdown();

    verify(jobStatusStore).resetRunningJob(1L, "node-1");
    verify(jobStatusStore).resetRunningJob(2L, "node-1");
    assertTrue(manager.isBufferEmpty(JobExecutionType.SINGLE));
  }

  @Test
  void flushOnShutdown_doesNotOverwriteTerminalJobs() {
    manager.offer(standardJob(1L));
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(jobStatusStore.resetRunningJob(1L, "node-1")).thenReturn(false);

    manager.flushOnShutdown();

    verify(jobStatusStore).resetRunningJob(1L, "node-1");
    verify(jobStatusStore, never()).resetRunningJob(1L, "other-node");
    assertTrue(manager.isBufferEmpty(JobExecutionType.SINGLE));
  }
}
