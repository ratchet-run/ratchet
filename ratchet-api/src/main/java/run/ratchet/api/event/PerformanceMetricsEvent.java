package run.ratchet.api.event;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

/**
 * Legacy placeholder for dashboard-level aggregate metrics.
 *
 * <p>The RI does not publish this event. Use the {@code MetricsCollector} SPI for scheduler
 * telemetry instead. This type is retained only for source compatibility with early API drafts.
 *
 * <p>The {@code performanceData} map is defensively copied and is immutable after construction. All
 * keys and values must be non-null and serializable by the event transport used by the application.
 *
 * @deprecated no scheduler producer exists; use {@code MetricsCollector} instead
 */
@Deprecated(since = "0.1.0", forRemoval = false)
public record PerformanceMetricsEvent(Map<String, Object> performanceData) implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  public PerformanceMetricsEvent {
    performanceData = Map.copyOf(performanceData);
  }
}
