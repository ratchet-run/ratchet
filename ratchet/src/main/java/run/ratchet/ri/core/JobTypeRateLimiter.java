package run.ratchet.ri.core;

import run.ratchet.store.entity.JobExecutionType;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Rate limiter for job execution per job type to prevent resource exhaustion.
 *
 * <p>This rate limiter is an essential safeguard against runaway job processing that could
 * overwhelm system resources (CPU, memory, database connections, external API rate limits). By
 * limiting how many jobs of each type can execute per minute, we ensure fair resource allocation
 * and prevent cascading failures.
 *
 * <p>The rate limiter uses a sliding window approach to track job execution rates per job type.
 * Each job type has its own independent rate limit and tracking window, allowing fine-grained
 * control over different workload categories.
 *
 * <p><b>Configuration via environment variables:</b>
 *
 * <ul>
 *   <li>{@code SCHEDULER_RATE_LIMIT_SINGLE}: Max jobs per minute for SINGLE type (default:
 *       unlimited)
 *   <li>{@code SCHEDULER_RATE_LIMIT_RECURRING}: Max jobs per minute for RECURRING type (default:
 *       unlimited)
 *   <li>{@code SCHEDULER_RATE_LIMIT_BATCH_CHILD}: Max jobs per minute for BATCH_CHILD type
 *       (default: unlimited)
 *   <li>{@code SCHEDULER_RATE_LIMIT_CHAIN_STEP}: Max jobs per minute for CHAIN_STEP type (default:
 *       unlimited)
 *   <li>{@code SCHEDULER_RATE_LIMIT_BATCH_PARENT}: Max jobs per minute for BATCH_PARENT type
 *       (default: unlimited)
 *   <li>{@code SCHEDULER_RATE_LIMIT_WORKFLOW_BRANCH}: Max jobs per minute for WORKFLOW_BRANCH type
 *       (default: unlimited)
 * </ul>
 *
 * <p>A value of 0 (or not set) means unlimited - no rate limiting is applied for that job type.
 *
 * <p><b>Thread Safety:</b> This class is thread-safe and can be called from multiple threads
 * concurrently.
 *
 * @see JobExecutionType for the scheduler execution categories
 */
@ApplicationScoped
public class JobTypeRateLimiter {

  private static final Logger log = Logger.getLogger(JobTypeRateLimiter.class.getName());

  private final Map<JobExecutionType, Integer> rateLimits = new EnumMap<>(JobExecutionType.class);
  private final Map<JobExecutionType, RateWindow> rateWindows = new ConcurrentHashMap<>();

  /** Creates a new rate limiter and initializes rate limits from environment variables. */
  public JobTypeRateLimiter() {
    init();
  }

  /**
   * Gets the current execution count for a job type in the current window.
   *
   * @param jobType the job type
   * @return current count in the current minute window
   */
  public int getCurrentCount(JobExecutionType jobType) {
    RateWindow window = rateWindows.get(jobType);
    if (window == null) {
      return 0;
    }
    return window.getCurrentCount();
  }

  /**
   * Gets the configured rate limit for a job type.
   *
   * @param jobType the job type
   * @return rate limit (0 = unlimited)
   */
  public int getRateLimit(JobExecutionType jobType) {
    return rateLimits.getOrDefault(jobType, 0);
  }

  /**
   * Checks if rate limiting is enabled for a job type.
   *
   * @param jobType the job type
   * @return true if rate limiting is configured, false otherwise
   */
  public boolean isRateLimited(JobExecutionType jobType) {
    Integer limit = rateLimits.get(jobType);
    return limit != null && limit > 0;
  }

  /**
   * Checks if a job of the given type can be executed within the rate limit.
   *
   * @param jobType the job type to check
   * @return true if within rate limit, false if rate limit exceeded
   */
  public boolean tryAcquire(JobExecutionType jobType) {
    Integer maxPerMinute = rateLimits.get(jobType);

    // No rate limit configured (0 or null = unlimited)
    if (maxPerMinute == null || maxPerMinute <= 0) {
      return true;
    }

    // Get or create rate window for this job type
    RateWindow window = rateWindows.computeIfAbsent(jobType, k -> new RateWindow());

    return window.tryAcquire(maxPerMinute);
  }

  /**
   * Initializes rate limits from environment variable configuration.
   *
   * <p>Reads rate limit values from environment variables and populates the rate limits map. Rate
   * limits are logged at INFO level when enabled for visibility into the configured limits.
   */
  void init() {
    rateLimits.put(JobExecutionType.SINGLE, getRateLimitFromEnv("SCHEDULER_RATE_LIMIT_SINGLE", 0));
    rateLimits.put(
        JobExecutionType.RECURRING, getRateLimitFromEnv("SCHEDULER_RATE_LIMIT_RECURRING", 0));
    rateLimits.put(
        JobExecutionType.BATCH_CHILD, getRateLimitFromEnv("SCHEDULER_RATE_LIMIT_BATCH_CHILD", 0));
    rateLimits.put(
        JobExecutionType.CHAIN_STEP, getRateLimitFromEnv("SCHEDULER_RATE_LIMIT_CHAIN_STEP", 0));
    rateLimits.put(
        JobExecutionType.BATCH_PARENT, getRateLimitFromEnv("SCHEDULER_RATE_LIMIT_BATCH_PARENT", 0));
    rateLimits.put(
        JobExecutionType.WORKFLOW_BRANCH,
        getRateLimitFromEnv("SCHEDULER_RATE_LIMIT_WORKFLOW_BRANCH", 0));

    boolean anyConfigured = false;
    for (Map.Entry<JobExecutionType, Integer> entry : rateLimits.entrySet()) {
      if (entry.getValue() > 0) {
        log.info("Rate limit for " + entry.getKey() + ": " + entry.getValue() + " jobs/minute");
        anyConfigured = true;
      }
    }

    if (!anyConfigured) {
      log.warning(
          "No rate limits configured for job scheduler"
              + " - all job types will execute without rate limiting");
    }
  }

  private int getRateLimitFromEnv(String envVar, int defaultValue) {
    String value = System.getenv(envVar);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    try {
      int limit = Integer.parseInt(value.trim());
      return Math.max(limit, 0);
    } catch (NumberFormatException e) {
      log.warning("Invalid rate limit value for " + envVar + ": " + value + ", using unlimited");
      return defaultValue;
    }
  }

  private static class RateWindow {

    private final AtomicInteger count = new AtomicInteger(0);
    private volatile long windowStart = System.currentTimeMillis();

    int getCurrentCount() {
      long now = System.currentTimeMillis();
      if (now - windowStart >= 60000) {
        return 0;
      }
      return count.get();
    }

    synchronized boolean tryAcquire(int maxPerMinute) {
      long now = System.currentTimeMillis();

      if (now - windowStart >= 60000) {
        count.set(1);
        windowStart = now;
        return 1 <= maxPerMinute;
      }

      int current = count.incrementAndGet();
      return current <= maxPerMinute;
    }
  }
}
