package run.ratchet.api.event;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

/**
 * Event fired when performance metrics are ready for broadcasting to dashboard clients.
 *
 * <p>This event is published periodically and observed by the WebSocket layer to push real-time
 * metrics to connected clients. It enables live monitoring dashboards without requiring clients to
 * poll the server.
 *
 * <h2>Performance Data Contents:</h2>
 *
 * <p>The performance data map typically contains:
 *
 * <ul>
 *   <li><b>queueDepth:</b> Number of pending jobs in the queue
 *   <li><b>readyJobs:</b> Number of jobs ready for immediate execution
 *   <li><b>oldestJobAge:</b> Age of the oldest pending job in seconds
 *   <li><b>processingRate:</b> Jobs processed per minute
 *   <li><b>successRate:</b> Percentage of successful job completions
 *   <li><b>avgDuration:</b> Average job execution duration
 *   <li><b>threadUtilization:</b> Thread pool utilization percentage
 *   <li><b>activeThreads:</b> Number of currently active worker threads
 *   <li><b>queuedTasks:</b> Number of tasks waiting in thread pool queue
 *   <li><b>cpuUsage:</b> Current CPU utilization
 *   <li><b>memoryUsage:</b> Current memory usage in bytes
 *   <li><b>memoryPercent:</b> Memory usage as percentage of available
 * </ul>
 *
 * <p>Note: This event intentionally does NOT extend {@link AbstractJobSchedulerEvent} because it
 * represents system-level aggregate metrics, not a per-job lifecycle event. {@code
 * AbstractJobSchedulerEvent} carries per-job metadata (jobId, businessKey, jobType, priority,
 * nodeId) that does not apply to system-wide performance snapshots. The scheduler event bus accepts
 * any event type, so this record is published directly without inheriting from the job-lifecycle
 * base class.
 *
 * @param performanceData Map containing performance metrics for the job scheduler.
 */
public record PerformanceMetricsEvent(Map<String, Object> performanceData) implements Serializable {

  @Serial private static final long serialVersionUID = 1L;
}
