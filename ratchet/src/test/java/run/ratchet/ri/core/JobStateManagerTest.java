package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobBatchStatusStore;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobStateManagerTest {

  private static final String NODE_ID = "node-abc";
  private static final long JOB_ID_LONG = 42L;
  private static final UUID JOB_ID = new UUID(0L, JOB_ID_LONG);

  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private NodeIdentityProvider nodeIdentityProvider;

  private JobStateManager manager;

  private static JobEntity runningJob(UUID id) {
    JobEntity job = new JobEntity();
    job.setId(id);
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy(NODE_ID);
    job.setPickedAt(Instant.now());
    return job;
  }

  @BeforeEach
  void setUp() {
    manager = new JobStateManager(jobBatchStatusStore, nodeIdentityProvider);
  }

  @Test
  void resetJobToPending_entity_casSucceeds_updatesAllFields() {
    JobEntity job = runningJob(JOB_ID);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobBatchStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(true);

    boolean result = manager.resetJobToPending(job);

    assertTrue(result);
    assertEquals(JobStatus.PENDING, job.getStatus());
    assertNull(job.getPickedBy());
    assertNull(job.getPickedAt());
  }

  @Test
  void resetJobToPending_entity_casFails_leavesFieldsUnchanged() {
    Instant pickedAt = Instant.parse("2025-06-01T10:00:00Z");
    JobEntity job = runningJob(JOB_ID);
    job.setPickedAt(pickedAt);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobBatchStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(false);

    boolean result = manager.resetJobToPending(job);

    assertFalse(result);
    assertEquals(JobStatus.RUNNING, job.getStatus());
    assertEquals(NODE_ID, job.getPickedBy());
    assertEquals(pickedAt, job.getPickedAt());
  }

  @Test
  void resetJobToPending_byId_delegatesToStoreWithNodeId() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobBatchStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(true);

    boolean result = manager.resetJobToPending(JOB_ID);

    assertTrue(result);
    verify(jobBatchStatusStore).resetRunningJob(JOB_ID, NODE_ID);
  }

  @Test
  void resetJobToPending_byId_storeReturnsFalse_returnsFalse() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobBatchStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(false);

    boolean result = manager.resetJobToPending(JOB_ID);

    assertFalse(result);
  }

  @Test
  void resetJobToPending_byId_storeThrows_returnsFalse() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobBatchStatusStore.resetRunningJob(JOB_ID, NODE_ID))
        .thenThrow(new RuntimeException("DB error"));

    boolean result = manager.resetJobToPending(JOB_ID);

    assertFalse(result);
  }

  @Test
  void resetRunningJobsForNode_delegatesToStoreWithNodeId() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobBatchStatusStore.resetRunningJobs(NODE_ID)).thenReturn(3);

    int count = manager.resetRunningJobsForNode();

    assertEquals(3, count);
    verify(jobBatchStatusStore).resetRunningJobs(NODE_ID);
  }

  @Test
  void resetRunningJobsForNode_returnsZeroWhenNoJobsReset() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobBatchStatusStore.resetRunningJobs(NODE_ID)).thenReturn(0);

    int count = manager.resetRunningJobsForNode();

    assertEquals(0, count);
  }

  @Test
  void resetJobToPending_entity_nullDependsOn_succeeds() {
    JobEntity job = runningJob(JOB_ID);
    job.setDependsOn(null);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobBatchStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(true);

    boolean result = manager.resetJobToPending(job);

    assertTrue(result);
    assertNull(job.getDependsOn());
  }

  @Test
  void resetJobToPending_entity_withDependsOn_doesNotClearIt() {
    JobEntity job = runningJob(JOB_ID);
    UUID dependency = new UUID(0L, 99L);
    job.setDependsOn(dependency);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobBatchStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(true);

    manager.resetJobToPending(job);

    assertEquals(dependency, job.getDependsOn());
  }

  @Test
  void resetJobToPending_byId_usesNodeIdFromProvider() {
    String differentNodeId = "node-xyz";
    when(nodeIdentityProvider.getNodeId()).thenReturn(differentNodeId);
    when(jobBatchStatusStore.resetRunningJob(JOB_ID, differentNodeId)).thenReturn(true);

    boolean result = manager.resetJobToPending(JOB_ID);

    assertTrue(result);
    verify(jobBatchStatusStore).resetRunningJob(JOB_ID, differentNodeId);
  }
}
