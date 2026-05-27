package run.ratchet.coordinator.hazelcast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class HazelcastCoordinatorConfigTest {

  @Test
  void defaultsProduceValidConfig() {
    HazelcastCoordinatorConfig c = HazelcastCoordinatorConfig.defaults();
    assertNotNull(c);
    assertEquals(HazelcastCoordinatorConfig.DEFAULT_TOPIC_NAME, c.topicName());
    assertTrue(c.cellId().isEmpty());
    assertTrue(c.shutdownGraceMs() > 0);
  }

  @Test
  void effectiveTopicNameAppliesCellId() {
    HazelcastCoordinatorConfig c =
        new HazelcastCoordinatorConfig("base", Optional.of("tenant42"), 2, 5_000L);
    assertEquals("base-tenant42", c.effectiveTopicName());
  }

  @Test
  void effectiveTopicNameOmitsCellIdSuffixWhenEmpty() {
    HazelcastCoordinatorConfig c =
        new HazelcastCoordinatorConfig("base", Optional.empty(), 2, 5_000L);
    assertEquals("base", c.effectiveTopicName());
  }

  @Test
  void blankTopicNameRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HazelcastCoordinatorConfig("", Optional.empty(), 2, 5_000L));
    assertThrows(
        IllegalArgumentException.class,
        () -> new HazelcastCoordinatorConfig("   ", Optional.empty(), 2, 5_000L));
  }

  @Test
  void nullArgumentsRejected() {
    assertThrows(
        NullPointerException.class,
        () -> new HazelcastCoordinatorConfig(null, Optional.empty(), 2, 5_000L));
    assertThrows(
        NullPointerException.class, () -> new HazelcastCoordinatorConfig("base", null, 2, 5_000L));
  }

  @Test
  void nonPositiveShutdownGraceRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HazelcastCoordinatorConfig("base", Optional.empty(), 2, 0L));
    assertThrows(
        IllegalArgumentException.class,
        () -> new HazelcastCoordinatorConfig("base", Optional.empty(), 2, -1L));
  }

  @Test
  void nonPositiveListenerThreadsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HazelcastCoordinatorConfig("base", Optional.empty(), 0, 5_000L));
  }
}
