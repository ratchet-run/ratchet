package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.JobLogStore;
import run.ratchet.store.spi.LockStore;
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
 * Periodically purges old job execution logs to prevent unbounded table growth.
 *
 * <p>Schedule is configurable via the {@code LOG_PURGER_CRON} environment variable (Quartz format,
 * default: {@code 0 30 2 * * ?}). Uses a distributed lock to ensure only one node in the cluster
 * executes the purge.
 *
 * <p><b>For high-volume deployments</b> (above ~10M log rows), combine purging with time-range
 * partitioning of {@code scheduler_job_log}. Dropping a partition is O(1) and reclaims space
 * immediately, whereas {@code DELETE} compounds index bloat and vacuum cost. See {@code
 * docs/ops/partitioning.md} for MySQL and PostgreSQL recipes.
 *
 * @see JobLogStore#purgeLogsOlderThan(Instant)
 */
@ApplicationScoped
public class LogPurgeTimer {

  private static final Logger log = Logger.getLogger(LogPurgeTimer.class);
  private static final String LOCK_NAME = "logPurger";

  private final JobLogStore jobLogStore;
  private final LockStore lockStore;
  private final NodeIdentityProvider nodeIdentityProvider;
  private final ExecutorProvider executorProvider;

  private Duration retentionPeriod;
  private Cron cron;
  private ZoneId zone;

  // Required by CDI proxy
  protected LogPurgeTimer() {
    this.jobLogStore = null;
    this.lockStore = null;
    this.nodeIdentityProvider = null;
    this.executorProvider = null;
  }

  @Inject
  public LogPurgeTimer(
      JobLogStore jobLogStore,
      LockStore lockStore,
      NodeIdentityProvider nodeIdentityProvider,
      ExecutorProvider executorProvider) {
    this.jobLogStore = jobLogStore;
    this.lockStore = lockStore;
    this.nodeIdentityProvider = nodeIdentityProvider;
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
      if (!lockStore.tryLock(LOCK_NAME, Duration.ofMinutes(10), nodeIdentityProvider.getNodeId())) {
        log.debug("Log purge skipped - lock held by another node");
        return;
      }

      Instant cutoff = Instant.now().minus(retentionPeriod);
      int deleted = jobLogStore.purgeLogsOlderThan(cutoff);
      if (deleted > 0) {
        log.infof("Purged %s log rows older than %s", deleted, cutoff);
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
