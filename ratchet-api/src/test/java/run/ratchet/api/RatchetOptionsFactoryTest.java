package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.RatchetConfigSource;

class RatchetOptionsFactoryTest {

  @Test
  void usesEnvironmentVariableNameBeforePropertyNameWithinSource() {
    RatchetOptions options =
        optionsFrom(
            new MapRatchetConfigSource(
                Map.of("ratchet.poller.batch-size", "456"),
                Map.of("RATCHET_POLLER_BATCH_SIZE", "123")));

    assertEquals(123, options.polling().batchSize());
  }

  @Test
  void fallsBackToPropertyNameWhenEnvironmentVariableNameIsMissing() {
    RatchetOptions options =
        optionsFrom(
            new MapRatchetConfigSource(
                Map.of("ratchet.worker.use-virtual-threads", "true"), Map.of()));

    assertTrue(options.execution().useVirtualThreads());
  }

  @Test
  void usesFirstSourceThatReturnsValueBeforeLaterSources() {
    RatchetOptions options =
        optionsFrom(
            new MapRatchetConfigSource(Map.of("ratchet.poller.batch-size", "77"), Map.of()),
            new MapRatchetConfigSource(Map.of(), Map.of("RATCHET_POLLER_BATCH_SIZE", "123")));

    assertEquals(77, options.polling().batchSize());
  }

  @Test
  void preservesDefaultsWhenSourceChainHasNoValues() {
    RatchetOptions defaults = RatchetOptions.defaults();
    RatchetOptions options = optionsFrom((propertyName, environmentVariable) -> Optional.empty());

    assertEquals(defaults.polling().batchSize(), options.polling().batchSize());
    assertEquals(defaults.execution().useVirtualThreads(), options.execution().useVirtualThreads());
    assertEquals(
        defaults.timeout().signalTimeoutBatchSize(), options.timeout().signalTimeoutBatchSize());
    assertEquals(defaults.store().isolationCheckMode(), options.store().isolationCheckMode());
  }

  private static RatchetOptions optionsFrom(RatchetConfigSource... sources) {
    return RatchetOptionsFactory.from(new DefaultRatchetConfig(List.of(sources)));
  }

  private record MapRatchetConfigSource(
      Map<String, String> properties, Map<String, String> environment)
      implements RatchetConfigSource {

    @Override
    public Optional<String> get(String propertyName, String environmentVariable) {
      return value(environment, environmentVariable).or(() -> value(properties, propertyName));
    }

    private static Optional<String> value(Map<String, String> values, String key) {
      if (key == null) {
        return Optional.empty();
      }
      return Optional.ofNullable(values.get(key));
    }
  }
}
