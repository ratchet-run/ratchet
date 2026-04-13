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
 * @param performanceData Map containing performance metrics for the job scheduler.
 */
public record PerformanceMetricsEvent(Map<String, Object> performanceData) implements Serializable {

  @Serial private static final long serialVersionUID = 1L;
}
