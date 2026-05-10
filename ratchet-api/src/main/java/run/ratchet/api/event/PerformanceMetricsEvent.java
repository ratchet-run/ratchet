package run.ratchet.api.event;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

/**
 * Fired when performance metrics are ready for broadcasting to dashboard clients.
 *
 * <p>Published periodically and observed by the WebSocket layer to push real-time metrics. Does not
 * extend {@link AbstractJobSchedulerEvent} because it represents system-level aggregate metrics,
 * not a per-job lifecycle event.
 *
 * <p>The {@code performanceData} map is defensively copied and is immutable after construction. All
 * keys and values must be non-null and serializable by the event transport used by the application.
 */
public record PerformanceMetricsEvent(Map<String, Object> performanceData) implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  public PerformanceMetricsEvent {
    performanceData = Map.copyOf(performanceData);
  }
}
