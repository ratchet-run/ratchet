package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
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
  private final LongSupplier clockMillis;

  public JobTypeRateLimiter() {
    this(RatchetOptions.defaults());
  }

  @Inject
  public JobTypeRateLimiter(RatchetOptions options) {
    this(options, System::currentTimeMillis);
  }

  JobTypeRateLimiter(RatchetOptions options, LongSupplier clockMillis) {
    this.options = options;
    this.clockMillis = clockMillis;
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

    RateWindow window = rateWindows.computeIfAbsent(jobType, k -> new RateWindow(clockMillis));

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
    private final LongSupplier clockMillis;
    private volatile long windowStart;

    private RateWindow(LongSupplier clockMillis) {
      this.clockMillis = clockMillis;
      this.windowStart = clockMillis.getAsLong();
    }

    int getCurrentCount() {
      long now = clockMillis.getAsLong();
      if (now - windowStart >= 60000) {
        return 0;
      }
      return count.get();
    }

    synchronized boolean tryAcquire(int maxPerMinute) {
      long now = clockMillis.getAsLong();

      if (now - windowStart >= 60000) {
        count.set(1);
        windowStart = now;
        return 1 <= maxPerMinute;
      }

      if (count.get() >= maxPerMinute) {
        return false;
      }
      count.incrementAndGet();
      return true;
    }
  }
}
