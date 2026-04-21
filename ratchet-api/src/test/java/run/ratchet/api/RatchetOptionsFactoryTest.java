package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.spi.RatchetConfigSource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RatchetOptionsFactoryTest {

  @Test
  void buildsOptionsFromTypedSourceChain() {
    RatchetConfigSource source =
        (propertyName, environmentVariable) ->
            Optional.ofNullable(
                    Map.of(
                            "RATCHET_POLLER_BATCH_SIZE", "123",
                            "ratchet.worker.use-virtual-threads", "true",
                            "RATCHET_THREAD_POOL_SIZE_SINGLE", "7",
                            "RATCHET_TSID_NODE_ID", "42",
                            "RATCHET_ALLOW_EMPTY_CLASS_POLICY", "true",
                            "RATCHET_CB_DEFAULT_WAIT_MS", "12000",
                            "RATCHET_ISOLATION_CHECK_MODE", "warn")
                        .getOrDefault(environmentVariable, null))
                .or(
                    () ->
                        Optional.ofNullable(
                            Map.of("ratchet.worker.use-virtual-threads", "true")
                                .getOrDefault(propertyName, null)));

    RatchetOptions options = RatchetOptionsFactory.fromFallbackSources(List.of(source));

    assertEquals(123, options.polling().batchSize());
    assertTrue(options.execution().useVirtualThreads());
    assertEquals(7, options.execution().maxConcurrency("SINGLE", -1));
    assertEquals(42, options.node().explicitTsidNodeId().orElseThrow());
    assertTrue(options.security().allowEmptyClassPolicy());
    assertEquals(
        12000L, options.circuitBreaker().profile(CircuitBreakerProfile.DEFAULT).waitDurationMs());
    assertEquals(RatchetOptions.IsolationCheckMode.WARN, options.store().isolationCheckMode());
  }
}
