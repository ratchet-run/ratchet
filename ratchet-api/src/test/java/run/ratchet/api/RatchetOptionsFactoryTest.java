package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.RatchetConfigSource;

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
                            "RATCHET_SIGNAL_TIMEOUT_BATCH_SIZE", "42",
                            "RATCHET_ALLOW_EMPTY_CLASS_POLICY", "true",
                            "RATCHET_CB_DEFAULT_WAIT_MS", "12000",
                            "RATCHET_ISOLATION_CHECK_MODE", "warn")
                        .getOrDefault(environmentVariable, null))
                .or(
                    () ->
                        Optional.ofNullable(
                            Map.of("ratchet.worker.use-virtual-threads", "true")
                                .getOrDefault(propertyName, null)));

    RatchetOptions options = RatchetOptionsFactory.fromEnvironment(source);

    assertEquals(123, options.polling().batchSize());
    assertTrue(options.execution().useVirtualThreads());
    assertEquals(7, options.execution().maxConcurrency("SINGLE", -1));
    assertEquals(42, options.timeout().signalTimeoutBatchSize());
    assertTrue(options.security().allowEmptyClassPolicy());
    assertEquals(
        12000L, options.circuitBreaker().profile(CircuitBreakerProfile.DEFAULT).waitDurationMs());
    assertEquals(RatchetOptions.IsolationCheckMode.WARN, options.store().isolationCheckMode());
  }
}
