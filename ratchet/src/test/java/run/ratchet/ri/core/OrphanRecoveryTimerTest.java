package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.NodeStore;

@ExtendWith(MockitoExtension.class)
class OrphanRecoveryTimerTest {

  @Mock private JobBulkStore jobBulkStore;
  @Mock private NodeStore nodeStore;
  @Mock private ResourcePermitService resourcePermitService;

  private OrphanRecoveryTimer timer;

  @BeforeEach
  void setUp() {
    timer = new OrphanRecoveryTimer(jobBulkStore, nodeStore, resourcePermitService);
  }

  @Test
  void constructor_rejectsNullJobBulkStore() {
    assertThrows(
        NullPointerException.class,
        () -> new OrphanRecoveryTimer(null, nodeStore, resourcePermitService));
  }

  @Test
  void constructor_rejectsNullNodeStore() {
    assertThrows(
        NullPointerException.class,
        () -> new OrphanRecoveryTimer(jobBulkStore, null, resourcePermitService));
  }

  @Test
  void constructor_rejectsNullResourcePermitService() {
    assertThrows(
        NullPointerException.class, () -> new OrphanRecoveryTimer(jobBulkStore, nodeStore, null));
  }

  @Test
  void recoverOrphans_withStaleNodesCleansPermitsBeforeDeletingNodes() {
    NodeEntity staleNode = new NodeEntity();
    staleNode.setId("node-1");
    when(nodeStore.findInactiveNodesSince(any(Instant.class))).thenReturn(List.of(staleNode));

    timer.recoverOrphans();

    verify(resourcePermitService).cleanupOrphanedPermits(List.of("node-1"));
    verify(nodeStore).deleteInactiveNodesSince(any(Instant.class));
  }
}
