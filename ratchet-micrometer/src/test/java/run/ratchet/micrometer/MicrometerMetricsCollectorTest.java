package run.ratchet.micrometer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobType;
import run.ratchet.api.SignalDecision;
import run.ratchet.api.exception.RatchetTransientStoreException;

class MicrometerMetricsCollectorTest {

  @Test
  void jobFailedTagsByBoundedFamily() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerMetricsCollector collector = new MicrometerMetricsCollector(registry);

    collector.jobFailed(
        new UUID(0L, 1L),
        JobType.SINGLE,
        new RuntimeException("wrapped", new TimeoutException()),
        2);

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
  void jobFailedTagsValidationBusinessAndUnknownFamilies() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerMetricsCollector collector = new MicrometerMetricsCollector(registry);

    collector.jobFailed(
        new UUID(0L, 11L), JobType.SINGLE, new IllegalArgumentException("bad input"), 1);
    collector.jobFailed(
        new UUID(0L, 12L), JobType.SINGLE, new OrderRejectedException("customer rule"), 1);
    collector.jobFailed(new UUID(0L, 13L), JobType.SINGLE, new NullPointerException("bug"), 1);

    assertJobFailureFamily(registry, "VALIDATION");
    assertJobFailureFamily(registry, "BUSINESS");
    assertJobFailureFamily(registry, "UNKNOWN");
  }

  @Test
  void callbackFailedTagsByBoundedFamily() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerMetricsCollector collector = new MicrometerMetricsCollector(registry);

    collector.callbackFailed(
        new UUID(0L, 2L),
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
  void callbackFailedTagsValidationBusinessAndUnknownFamilies() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerMetricsCollector collector = new MicrometerMetricsCollector(registry);

    collector.callbackFailed(
        new UUID(0L, 21L), JobType.BATCH, new IllegalArgumentException("bad input"), 1);
    collector.callbackFailed(
        new UUID(0L, 22L), JobType.BATCH, new OrderRejectedException("customer rule"), 1);
    collector.callbackFailed(new UUID(0L, 23L), JobType.BATCH, new NullPointerException("bug"), 1);

    assertCallbackFailureFamily(registry, "VALIDATION");
    assertCallbackFailureFamily(registry, "BUSINESS");
    assertCallbackFailureFamily(registry, "UNKNOWN");
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

  @Test
  void pollerBreakerStatePublishesDocumentedNumericValues() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerMetricsCollector collector = new MicrometerMetricsCollector(registry);

    collector.pollerBreakerState("store.claim", "HALF_OPEN");
    assertEquals(
        1.0,
        registry.get("ratchet.poller.breaker.state").tag("breaker", "store.claim").gauge().value());

    collector.pollerBreakerState("store.claim", "CLOSED");
    assertEquals(
        0.0,
        registry.get("ratchet.poller.breaker.state").tag("breaker", "store.claim").gauge().value());

    collector.pollerBreakerState("store.claim", "unexpected");
    assertEquals(
        0.0,
        registry.get("ratchet.poller.breaker.state").tag("breaker", "store.claim").gauge().value());
  }

  @Test
  void signalMetricsUseBoundedTypeAndOutcomeTags() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerMetricsCollector collector = new MicrometerMetricsCollector(registry);
    UUID jobId = new UUID(0L, 3L);

    collector.signalWaiting(jobId, JobType.SINGLE, "approval");
    collector.signalDelivered(jobId, JobType.SINGLE, "approval", SignalDecision.Outcome.REJECTED);
    collector.signalTimedOut(jobId, JobType.SINGLE, "approval");
    collector.signalCancelled(jobId, JobType.SINGLE, "approval");

    assertEquals(
        1.0, registry.get("ratchet.signal.waiting").tag("type", "SINGLE").counter().count());
    assertEquals(
        1.0,
        registry
            .get("ratchet.signal.delivered")
            .tag("type", "SINGLE")
            .tag("outcome", "REJECTED")
            .counter()
            .count());
    assertEquals(
        1.0, registry.get("ratchet.signal.timed_out").tag("type", "SINGLE").counter().count());
    assertEquals(
        1.0, registry.get("ratchet.signal.cancelled").tag("type", "SINGLE").counter().count());
    assertNull(registry.find("ratchet.signal.waiting").tag("signal_key", "approval").counter());
  }

  @Test
  void stringTagsCollapseUnknownValuesByDefault() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerMetricsCollector collector = new MicrometerMetricsCollector(registry);

    collector.localWakeup("tenant-123");
    collector.storeOperation("mysql", "customer_123_lookup", "success", 1_000L);

    assertEquals(
        1.0, registry.get("ratchet.wakeup.local").tag("source", "OTHER").counter().count());
    assertNull(registry.find("ratchet.wakeup.local").tag("source", "tenant-123").counter());
    assertEquals(
        1.0,
        registry
            .get("ratchet.store.operation")
            .tag("store", "mysql")
            .tag("operation", "OTHER")
            .tag("outcome", "success")
            .timer()
            .count());
    assertNull(
        registry.find("ratchet.store.operation").tag("operation", "customer_123_lookup").timer());
  }

  @Test
  void customStringMetricTagsRequireExplicitOptIn() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerMetricTagPolicy tagPolicy =
        MicrometerMetricTagPolicy.builder()
            .allowValue("source", "tenant-tier-gold")
            .allowValue("operation", "tenant_tier_lookup")
            .allowValue("store", "mysql")
            .allowValue("outcome", "success")
            .build();
    MicrometerMetricsCollector collector = new MicrometerMetricsCollector(registry, tagPolicy);

    collector.localWakeup("tenant-tier-gold");
    collector.storeOperation("mysql", "tenant_tier_lookup", "success", 1_000L);
    collector.jobsClaimed("SINGLE", 1);

    assertEquals(
        1.0,
        registry.get("ratchet.wakeup.local").tag("source", "tenant-tier-gold").counter().count());
    assertEquals(
        1.0,
        registry
            .get("ratchet.store.operation")
            .tag("store", "mysql")
            .tag("operation", "tenant_tier_lookup")
            .tag("outcome", "success")
            .timer()
            .count());
    assertEquals(
        1.0,
        registry
            .get("ratchet.poller.claimed.jobs")
            .tag("execution_type", "SINGLE")
            .counter()
            .count());
  }

  private static void assertJobFailureFamily(SimpleMeterRegistry registry, String family) {
    assertEquals(
        1.0,
        registry
            .get("ratchet.jobs.failed")
            .tag("type", "SINGLE")
            .tag("family", family)
            .counter()
            .count());
  }

  private static void assertCallbackFailureFamily(SimpleMeterRegistry registry, String family) {
    assertEquals(
        1.0,
        registry
            .get("ratchet.callbacks.failed")
            .tag("type", "BATCH")
            .tag("family", family)
            .counter()
            .count());
  }

  private static final class OrderRejectedException extends RuntimeException {

    private OrderRejectedException(String message) {
      super(message);
    }
  }
}
