package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.LockStore;

@ExtendWith(MockitoExtension.class)
class StoreBackedStartupCoordinatorTest {

  @Mock private LockStore lockStore;
  @Mock private NodeIdentityProvider nodeIdentityProvider;

  private StoreBackedStartupCoordinator coordinator;

  @BeforeEach
  void setUp() {
    coordinator = new StoreBackedStartupCoordinator(lockStore, nodeIdentityProvider);
  }

  @Test
  void tryAcquire_usesStoreBackedLeaseWithStartupPrefix() {
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(lockStore.tryLock(
            "startup:recurring-annotation-orphan-cleanup", Duration.ofMinutes(5), "node-1"))
        .thenReturn(true);

    assertTrue(coordinator.tryAcquire("recurring-annotation-orphan-cleanup", Duration.ofMinutes(5)));

    verify(lockStore)
        .tryLock("startup:recurring-annotation-orphan-cleanup", Duration.ofMinutes(5), "node-1");
  }

  @Test
  void tryAcquire_returnsFalseWhenStoreLeaseIsUnavailable() {
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(lockStore.tryLock(
            "startup:recurring-annotation-orphan-cleanup", Duration.ofMinutes(5), "node-1"))
        .thenReturn(false);

    assertFalse(coordinator.tryAcquire("recurring-annotation-orphan-cleanup", Duration.ofMinutes(5)));
  }

  @Test
  void release_usesStoreBackedLeaseWithStartupPrefix() {
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");

    coordinator.release("recurring-annotation-orphan-cleanup");

    verify(lockStore).unlock("startup:recurring-annotation-orphan-cleanup", "node-1");
  }

  @Test
  void tryAcquire_rejectsBlankActionName() {
    assertThrows(
        IllegalArgumentException.class, () -> coordinator.tryAcquire("   ", Duration.ofMinutes(5)));
  }
}
