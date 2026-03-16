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
import run.ratchet.store.spi.JobStatusStore;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JobStateManagerTest {

  private static final String NODE_ID = "node-abc";
  private static final long JOB_ID = 42L;

  @Mock private JobStatusStore jobStatusStore;
  @Mock private NodeIdentityProvider nodeIdentityProvider;

  private JobStateManager manager;

  @BeforeEach
  void setUp() {
    manager = new JobStateManager(jobStatusStore, nodeIdentityProvider);
  }

  // ── resetJobToPending(JobEntity) — success ─────────────────────────────

  @Test
  void resetJobToPending_entity_casSucceeds_returnsTrue() {
    JobEntity job = runningJob(JOB_ID);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(true);

    boolean result = manager.resetJobToPending(job);

    assertTrue(result);
  }

  @Test
  void resetJobToPending_entity_casSucceeds_updatesStatusToPending() {
    JobEntity job = runningJob(JOB_ID);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(true);

    manager.resetJobToPending(job);

    assertEquals(JobStatus.PENDING, job.getStatus());
  }

  @Test
  void resetJobToPending_entity_casSucceeds_clearsPickedBy() {
    JobEntity job = runningJob(JOB_ID);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(true);

    manager.resetJobToPending(job);

    assertNull(job.getPickedBy());
  }

  @Test
  void resetJobToPending_entity_casSucceeds_clearsPickedAt() {
    JobEntity job = runningJob(JOB_ID);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(true);

    manager.resetJobToPending(job);

    assertNull(job.getPickedAt());
  }

  // ── resetJobToPending(JobEntity) — failure ─────────────────────────────

  @Test
  void resetJobToPending_entity_casFails_returnsFalse() {
    JobEntity job = runningJob(JOB_ID);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(false);

    boolean result = manager.resetJobToPending(job);

    assertFalse(result);
  }

  @Test
  void resetJobToPending_entity_casFails_statusUnchanged() {
    JobEntity job = runningJob(JOB_ID);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(false);

    manager.resetJobToPending(job);

    assertEquals(JobStatus.RUNNING, job.getStatus());
  }

  @Test
  void resetJobToPending_entity_casFails_pickedByUnchanged() {
    JobEntity job = runningJob(JOB_ID);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(false);

    manager.resetJobToPending(job);

    assertEquals(NODE_ID, job.getPickedBy());
  }

  @Test
  void resetJobToPending_entity_casFails_pickedAtUnchanged() {
    Instant pickedAt = Instant.parse("2025-06-01T10:00:00Z");
    JobEntity job = runningJob(JOB_ID);
    job.setPickedAt(pickedAt);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(false);

    manager.resetJobToPending(job);

    assertEquals(pickedAt, job.getPickedAt());
  }

  // ── resetJobToPending(Long) — delegates correctly ──────────────────────

  @Test
  void resetJobToPending_byId_delegatesToStoreWithNodeId() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(true);

    boolean result = manager.resetJobToPending(JOB_ID);

    assertTrue(result);
    verify(jobStatusStore).resetRunningJob(JOB_ID, NODE_ID);
  }

  @Test
  void resetJobToPending_byId_storeReturnsFalse_returnsFalse() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(false);

    boolean result = manager.resetJobToPending(JOB_ID);

    assertFalse(result);
  }

  @Test
  void resetJobToPending_byId_storeThrows_returnsFalse() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID))
        .thenThrow(new RuntimeException("DB error"));

    boolean result = manager.resetJobToPending(JOB_ID);

    assertFalse(result);
  }

  // ── resetRunningJobsForNode ─────────────────────────────────────────────

  @Test
  void resetRunningJobsForNode_delegatesToStoreWithNodeId() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJobs(NODE_ID)).thenReturn(3);

    int count = manager.resetRunningJobsForNode();

    assertEquals(3, count);
    verify(jobStatusStore).resetRunningJobs(NODE_ID);
  }

  @Test
  void resetRunningJobsForNode_returnsZeroWhenNoJobsReset() {
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJobs(NODE_ID)).thenReturn(0);

    int count = manager.resetRunningJobsForNode();

    assertEquals(0, count);
  }

  // ── Edge cases ─────────────────────────────────────────────────────────

  @Test
  void resetJobToPending_entity_nullDependsOn_succeeds() {
    JobEntity job = runningJob(JOB_ID);
    job.setDependsOn(null);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(true);

    boolean result = manager.resetJobToPending(job);

    assertTrue(result);
    assertNull(job.getDependsOn());
  }

  @Test
  void resetJobToPending_entity_withDependsOn_doesNotClearIt() {
    JobEntity job = runningJob(JOB_ID);
    job.setDependsOn(99L);
    when(nodeIdentityProvider.getNodeId()).thenReturn(NODE_ID);
    when(jobStatusStore.resetRunningJob(JOB_ID, NODE_ID)).thenReturn(true);

    manager.resetJobToPending(job);

    // dependsOn is not touched by the reset — only status, pickedBy, pickedAt are cleared
    assertEquals(99L, job.getDependsOn());
  }

  @Test
  void resetJobToPending_byId_usesNodeIdFromProvider() {
    String differentNodeId = "node-xyz";
    when(nodeIdentityProvider.getNodeId()).thenReturn(differentNodeId);
    when(jobStatusStore.resetRunningJob(JOB_ID, differentNodeId)).thenReturn(true);

    boolean result = manager.resetJobToPending(JOB_ID);

    assertTrue(result);
    // Verify the store was called with the node ID the provider returned, not a hard-coded value
    verify(jobStatusStore).resetRunningJob(JOB_ID, differentNodeId);
  }

  // ── Helpers ────────────────────────────────────────────────────────────

  private static JobEntity runningJob(long id) {
    JobEntity job = new JobEntity();
    job.setId(id);
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy(NODE_ID);
    job.setPickedAt(Instant.now());
    return job;
  }
}
