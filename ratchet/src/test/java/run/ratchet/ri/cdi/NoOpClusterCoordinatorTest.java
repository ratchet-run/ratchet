package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.annotation.PostConstruct;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;

class NoOpClusterCoordinatorTest {

  @Test
  void announceMethodIsWiredViaPostConstruct() throws NoSuchMethodException {
    // The CDI container invokes @PostConstruct automatically; this test verifies the annotation
    // is in place so a future refactor that removes it gets caught here rather than only when
    // operators notice the missing startup log.
    Method announce = NoOpClusterCoordinator.class.getDeclaredMethod("announce");
    assertNotNull(
        announce.getAnnotation(PostConstruct.class), "@PostConstruct must remain on announce()");
  }

  @Test
  void announceDoesNotThrow() {
    NoOpClusterCoordinator coordinator = new NoOpClusterCoordinator();
    assertDoesNotThrow(
        () -> {
          // Invoke announce() reflectively (it's package-private and we're in the same package,
          // but call via getDeclaredMethod to keep coverage parity with @PostConstruct path).
          Method announce = NoOpClusterCoordinator.class.getDeclaredMethod("announce");
          announce.setAccessible(true);
          announce.invoke(coordinator);
        });
  }

  @Test
  void noOpMethodsAreSafe() {
    NoOpClusterCoordinator coordinator = new NoOpClusterCoordinator();
    assertDoesNotThrow(
        () -> coordinator.notifyNewWork(JobPriority.HIGH, new NodeIdentity("nodeA"), null));
    assertDoesNotThrow(() -> coordinator.registerWakeupListener(hint -> {}));
    assertDoesNotThrow(coordinator::close);
  }
}
