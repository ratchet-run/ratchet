package run.ratchet.micrometer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import run.ratchet.api.JobType;
import run.ratchet.api.exception.RatchetTransientStoreException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class MicrometerMetricsCollectorTest {

  @Test
  void jobFailedTagsByBoundedFamily() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerMetricsCollector collector = new MicrometerMetricsCollector(registry);

    collector.jobFailed(
        1L, JobType.SINGLE, new RuntimeException("wrapped", new TimeoutException()), 2);

    assertEquals(
        1.0,
        registry
            .get("ratchet.jobs.failed")
            .tag("type", "SINGLE")
            .tag("family", "TIMEOUT")
            .counter()
            .count());
    assertNull(
        registry
            .find("ratchet.jobs.failed")
            .tags("type", "SINGLE", "exception", "RuntimeException")
            .counter());
  }

  @Test
  void callbackFailedTagsByBoundedFamily() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerMetricsCollector collector = new MicrometerMetricsCollector(registry);

    collector.callbackFailed(
        2L,
        JobType.BATCH,
        new RuntimeException("wrapped", new RatchetTransientStoreException("transient")),
        1);

    assertEquals(
        1.0,
        registry
            .get("ratchet.callbacks.failed")
            .tag("type", "BATCH")
            .tag("family", "TRANSIENT")
            .counter()
            .count());
  }

  @Test
  void pollerBreakerStatePublishesGauge() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerMetricsCollector collector = new MicrometerMetricsCollector(registry);

    collector.pollerBreakerState("store.claim", "OPEN");

    assertEquals(
        2.0,
        registry.get("ratchet.poller.breaker.state").tag("breaker", "store.claim").gauge().value());
  }
}
