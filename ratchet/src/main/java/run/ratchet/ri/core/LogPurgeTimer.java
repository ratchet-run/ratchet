package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.spi.JobLogStore;

/**
 * Periodically purges old job execution logs to prevent unbounded table growth. Uses a singleton
 * lease to ensure only one node in the cluster executes the purge.
 */
@ApplicationScoped
public class LogPurgeTimer {

  private static final Logger log = Logger.getLogger(LogPurgeTimer.class);
  private static final String LEASE_NAME = "logPurger";
  private static final Duration LEASE_TTL = Duration.ofMinutes(10);

  private final JobLogStore jobLogStore;
  private final SingletonLeaseService singletonLeaseService;
  private final ExecutorProvider executorProvider;
  private final Clock clock;

  private Duration retentionPeriod;
  private Cron cron;
  private ZoneId zone;
  private volatile boolean initialized;

  protected LogPurgeTimer() {
    this.jobLogStore = null;
    this.singletonLeaseService = null;
    this.executorProvider = null;
    this.clock = null;
  }

  public LogPurgeTimer(
      JobLogStore jobLogStore,
      SingletonLeaseService singletonLeaseService,
      ExecutorProvider executorProvider) {
    this(jobLogStore, singletonLeaseService, executorProvider, Clock.systemUTC());
  }

  @Inject
  public LogPurgeTimer(
      JobLogStore jobLogStore,
      SingletonLeaseService singletonLeaseService,
      ExecutorProvider executorProvider,
      Clock clock) {
    this.jobLogStore = jobLogStore;
    this.singletonLeaseService = singletonLeaseService;
    this.executorProvider = executorProvider;
    this.clock = clock;
  }

  public void init(long retentionDays, Cron cronExpression) {
    if (jobLogStore == null || singletonLeaseService == null || executorProvider == null) {
      throw new IllegalStateException("LogPurgeTimer dependencies are not initialized");
    }

    this.retentionPeriod = Duration.ofDays(retentionDays);
    this.cron = Objects.requireNonNull(cronExpression, "cronExpression");
    this.zone = ZoneId.systemDefault();
    this.initialized = true;

    scheduleNext();

    log.infof("Log purge timer scheduled (retention=%s days)", retentionDays);
  }

  public void stop() {
    // Cron-based scheduling uses one-shot delays; nothing to cancel between runs
  }

  void run() {
    if (!initialized) {
      log.debug("Log purge timer is not initialized, skipping run");
      return;
    }

    try {
      purge();
    } finally {
      scheduleNext();
    }
  }

  private void purge() {
    try {
      Optional<SingletonLease> lease = singletonLeaseService.tryAcquire(LEASE_NAME, LEASE_TTL);
      if (lease.isEmpty()) {
        log.debug("Log purge skipped - singleton lease held by another node");
        return;
      }

      try (SingletonLease ignored = lease.get()) {
        Instant cutoff = effective().instant().minus(retentionPeriod);
        int deleted = jobLogStore.purgeLogsOlderThan(cutoff);
        if (deleted > 0) {
          log.infof("Purged %s log rows older than %s", deleted, cutoff);
        }
      }
    } catch (Exception e) {
      log.error("Log purge failed", e);
    }
  }

  private void scheduleNext() {
    if (!initialized) {
      return;
    }

    Instant now = effective().instant();
    Optional<Instant> next =
        ExecutionTime.forCron(cron).nextExecution(now.atZone(zone)).map(ZonedDateTime::toInstant);

    next.ifPresent(
        instant ->
            executorProvider
                .getScheduledExecutor()
                .schedule(
                    this::run, Duration.between(now, instant).toMillis(), TimeUnit.MILLISECONDS));
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}
