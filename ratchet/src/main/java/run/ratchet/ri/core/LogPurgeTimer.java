package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.spi.JobLogStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;

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

  private Duration retentionPeriod;
  private Cron cron;
  private ZoneId zone;

  protected LogPurgeTimer() {
    this.jobLogStore = null;
    this.singletonLeaseService = null;
    this.executorProvider = null;
  }

  @Inject
  public LogPurgeTimer(
      JobLogStore jobLogStore,
      SingletonLeaseService singletonLeaseService,
      ExecutorProvider executorProvider) {
    this.jobLogStore = jobLogStore;
    this.singletonLeaseService = singletonLeaseService;
    this.executorProvider = executorProvider;
  }

  public void init(long retentionDays, Cron cronExpression) {
    this.retentionPeriod = Duration.ofDays(retentionDays);
    this.cron = cronExpression;
    this.zone = ZoneId.systemDefault();

    scheduleNext();

    log.infof("Log purge timer scheduled (retention=%s days)", retentionDays);
  }

  public void stop() {
    // Cron-based scheduling uses one-shot delays; nothing to cancel between runs
  }

  void run() {
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
        Instant cutoff = Instant.now().minus(retentionPeriod);
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
    Instant now = Instant.now();
    Optional<Instant> next =
        ExecutionTime.forCron(cron).nextExecution(now.atZone(zone)).map(ZonedDateTime::toInstant);

    next.ifPresent(
        instant ->
            executorProvider
                .getScheduledExecutor()
                .schedule(
                    this::run, Duration.between(now, instant).toMillis(), TimeUnit.MILLISECONDS));
  }
}
