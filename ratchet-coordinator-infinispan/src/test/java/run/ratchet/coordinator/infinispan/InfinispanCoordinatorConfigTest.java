package run.ratchet.coordinator.infinispan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class InfinispanCoordinatorConfigTest {

  @Test
  void defaultsProduceValidConfig() {
    InfinispanCoordinatorConfig c = InfinispanCoordinatorConfig.defaults();
    assertNotNull(c);
    assertEquals(InfinispanCoordinatorConfig.DEFAULT_CACHE_NAME, c.cacheName());
    assertTrue(c.cellId().isEmpty());
    assertTrue(c.wakeupTtlSeconds() > 0);
    assertTrue(c.shutdownGraceMs() > 0);
  }

  @Test
  void effectiveCacheNameAppliesCellId() {
    InfinispanCoordinatorConfig c =
        new InfinispanCoordinatorConfig("base", Optional.of("cellA"), 60L, 2, 5_000L);
    assertEquals("base_cellA", c.effectiveCacheName());
  }

  @Test
  void nonPositiveWakeupTtlRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new InfinispanCoordinatorConfig("wakeup", Optional.empty(), 0L, 2, 5_000L));
    assertThrows(
        IllegalArgumentException.class,
        () -> new InfinispanCoordinatorConfig("wakeup", Optional.empty(), -1L, 2, 5_000L));
  }

  @Test
  void nonPositiveShutdownGraceRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new InfinispanCoordinatorConfig("wakeup", Optional.empty(), 60L, 2, 0L));
  }

  @Test
  void blankCacheNameRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new InfinispanCoordinatorConfig("", Optional.empty(), 60L, 2, 5_000L));
  }

  @Test
  void nonPositiveListenerThreadsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new InfinispanCoordinatorConfig("wakeup", Optional.empty(), 60L, 0, 5_000L));
  }
}
