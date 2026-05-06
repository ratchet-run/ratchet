package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.entity.JobExecutionType;

/** Per-type rate limiter using a one-minute sliding window. A value of 0 means unlimited. */
@ApplicationScoped
public class JobTypeRateLimiter {

  private static final Logger log = Logger.getLogger(JobTypeRateLimiter.class);

  private final Map<JobExecutionType, Integer> rateLimits = new EnumMap<>(JobExecutionType.class);
  private final Map<JobExecutionType, RateWindow> rateWindows = new ConcurrentHashMap<>();
  private final RatchetOptions options;

  public JobTypeRateLimiter() {
    this(RatchetOptions.defaults());
  }

  @Inject
  public JobTypeRateLimiter(RatchetOptions options) {
    this.options = options;
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

    if (maxPerMinute == null || maxPerMinute <= 0) {
      return true;
    }

    RateWindow window = rateWindows.computeIfAbsent(jobType, k -> new RateWindow());

    return window.tryAcquire(maxPerMinute);
  }

  void init() {
    rateLimits.clear();
    for (JobExecutionType type : JobExecutionType.values()) {
      rateLimits.put(type, options.execution().rateLimitPerMinute(type.name()));
    }

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
