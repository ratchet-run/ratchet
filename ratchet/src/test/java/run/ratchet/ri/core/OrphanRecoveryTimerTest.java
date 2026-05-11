package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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

  @Test
  void start_cancelsExistingHandleBeforeReplacingIt() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> first = mock(ScheduledFuture.class);
    ScheduledFuture<?> second = mock(ScheduledFuture.class);
    doReturn(first, second)
        .when(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.MINUTES));

    timer.start(executor, 1);
    timer.start(executor, 1);

    verify(first).cancel(false);
    verify(second, never()).cancel(false);
  }

  @Test
  void stop_clearsHandleBeforeCancelAndIsIdempotent() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> handle = mock(ScheduledFuture.class);
    doReturn(handle)
        .when(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.MINUTES));

    timer.start(executor, 1);
    timer.stop();
    timer.stop();

    verify(handle, times(1)).cancel(false);
  }
}
