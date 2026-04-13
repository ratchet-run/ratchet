package run.ratchet.ri.core;

import run.ratchet.store.entity.JobExecutionType;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.logging.Logger;

/**
 * Per-type rate limiter using a one-minute sliding window. Configured via environment variables
 * named {@code SCHEDULER_RATE_LIMIT_<TYPE>} (e.g. {@code SCHEDULER_RATE_LIMIT_SINGLE}). A value of
 * 0 or unset means unlimited.
 */
@ApplicationScoped
public class JobTypeRateLimiter {

  private static final Logger log = Logger.getLogger(JobTypeRateLimiter.class);

  private final Map<JobExecutionType, Integer> rateLimits = new EnumMap<>(JobExecutionType.class);
  private final Map<JobExecutionType, RateWindow> rateWindows = new ConcurrentHashMap<>();

  /** Creates a new rate limiter and initializes rate limits from environment variables. */
  public JobTypeRateLimiter() {
    init();
  }

  /** Returns the current execution count in the current one-minute window. */
  public int getCurrentCount(JobExecutionType jobType) {
    RateWindow window = rateWindows.get(jobType);
    if (window == null) {
      return 0;
    }
    return window.getCurrentCount();
  }

  /**
   * @return configured rate limit (0 = unlimited)
   */
  public int getRateLimit(JobExecutionType jobType) {
    return rateLimits.getOrDefault(jobType, 0);
  }

  public boolean isRateLimited(JobExecutionType jobType) {
    Integer limit = rateLimits.get(jobType);
    return limit != null && limit > 0;
  }

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

  /** Reads rate limits from environment variables. */
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
        log.infof("Rate limit for %s: %s jobs/minute", entry.getKey(), entry.getValue());
        anyConfigured = true;
      }
    }

    if (!anyConfigured) {
      log.debug("No rate limits configured — all job types unlimited");
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
      log.warnf("Invalid rate limit value for %s: %s, using unlimited", envVar, value);
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
