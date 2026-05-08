package run.ratchet.ri.core;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.JobCrudStore;

/**
 * Polls for due recurring masters and delegates to {@link RecurringJobExecutor} to spawn children.
 * Uses a singleton lease so only one node processes recurring jobs at a time.
 */
@ApplicationScoped
public class RecurringScheduler {

  public static final CronParser PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

  private static final Logger log = Logger.getLogger(RecurringScheduler.class);
  private static final String LEASE_NAME = "recurringScheduler";
  private static final Duration LEASE_TTL = Duration.ofMinutes(5);

  private final AtomicBoolean started = new AtomicBoolean();
  private final Object scheduleLock = new Object();
  private final ExecutorProvider executorProvider;
  private final JobCrudStore jobCrudStore;
  private final SingletonLeaseService singletonLeaseService;
  private final NodeIdentityProvider nodeIdentityProvider;
  private final RecurringJobExecutor recurringJobExecutor;
  private final PollerScheduler pollerScheduler;
  private final Clock clock;

  private volatile int batchLimit = 20;
  private volatile long minPollMs = 1000;
  private volatile long maxPollMs = 60000;
  private volatile long currentDelayMs;

  @SuppressWarnings("java:S3077")
  private volatile Future<?> handle;

  @SuppressWarnings("java:S3077")
  private volatile ScheduledExecutorService executor;

  protected RecurringScheduler() {
    this.executorProvider = null;
    this.jobCrudStore = null;
    this.singletonLeaseService = null;
    this.nodeIdentityProvider = null;
    this.recurringJobExecutor = null;
    this.pollerScheduler = null;
    this.clock = null;
  }

  @Inject
  public RecurringScheduler(
      ExecutorProvider executorProvider,
      JobCrudStore jobCrudStore,
      SingletonLeaseService singletonLeaseService,
      NodeIdentityProvider nodeIdentityProvider,
      RecurringJobExecutor recurringJobExecutor,
      PollerScheduler pollerScheduler,
      Clock clock) {
    this.executorProvider = executorProvider;
    this.jobCrudStore = jobCrudStore;
    this.singletonLeaseService = singletonLeaseService;
    this.nodeIdentityProvider = nodeIdentityProvider;
    this.recurringJobExecutor = recurringJobExecutor;
    this.pollerScheduler = pollerScheduler;
    this.clock = clock;
  }

  public long getCurrentDelayMs() {
    return currentDelayMs;
  }

  public void configure(long minPollMs, long maxPollMs, int batchLimit) {
    this.minPollMs = minPollMs;
    this.maxPollMs = maxPollMs;
    this.batchLimit = batchLimit;
  }

  public void init() {
    if (!started.compareAndSet(false, true)) {
      log.warn("RecurringScheduler already initialized; skipping re-run");
      return;
    }

    executor = executorProvider.getScheduledExecutor();
    currentDelayMs = minPollMs;
    scheduleNext(minPollMs);
    log.infof("RecurringScheduler started (minPoll=%s ms, maxPoll=%s ms)", minPollMs, maxPollMs);
  }

  /** Forces an immediate poll cycle. */
  public void kick() {
    if (!started.get()) {
      return;
    }
    synchronized (scheduleLock) {
      if (!started.get()) {
        return;
      }
      Future<?> current = handle;
      if (current != null) {
        current.cancel(false);
        handle = null;
      }
      scheduleNextLocked(minPollMs);
    }
    log.debug("RecurringScheduler kicked — immediate scan scheduled");
  }

  public void stop() {
    started.set(false);
    synchronized (scheduleLock) {
      if (handle != null) {
        handle.cancel(false);
      }
      handle = null;
    }
  }

  @SuppressWarnings("java:S1181")
  void run() {
    if (!started.get()) {
      return;
    }

    ScheduledFuture<?> renewalTask = null;
    Optional<SingletonLease> lease = Optional.empty();
    try {
      lease = singletonLeaseService.tryAcquire(LEASE_NAME, LEASE_TTL);
      if (lease.isEmpty()) {
        scheduleNext(minPollMs);
        return;
      }

      SingletonLease acquiredLease = lease.get();
      renewalTask =
          executor.scheduleAtFixedRate(() -> renewLease(acquiredLease), 2, 2, TimeUnit.MINUTES);

      int processedCount =
          recurringJobExecutor.process(batchLimit, nodeIdentityProvider.getNodeId());

      if (processedCount > 0) {
        pollerScheduler.wakeup();
      }

      long nextDelay = calculateNextDelay(processedCount);
      scheduleNext(nextDelay);
    } catch (Throwable ex) {
      if (!started.get()) {
        return;
      }
      // CDI context gone (e.g. Arquillian undeploy) — stop permanently, next deploy starts fresh
      if (SchedulerUtils.isCdiContextGone(ex)) {
        started.set(false);
        log.info("RecurringScheduler detected inactive CDI context — stopping permanently");
        return;
      }
      log.error("RecurringScheduler failed", ex);
      try {
        scheduleNext(minPollMs);
      } catch (Exception e) {
        log.debug("Cannot reschedule recurring scan — scheduler will restart on next deploy", e);
      }
    } finally {
      if (renewalTask != null && !renewalTask.isCancelled()) {
        renewalTask.cancel(false);
      }
      lease.ifPresent(SingletonLease::close);
    }
  }

  private long calculateNextDelay(int processedCount) {
    if (processedCount > 0) {
      return minPollMs;
    }

    Optional<Instant> earliestNextFire = jobCrudStore.findEarliestRecurringNextFire();

    if (earliestNextFire.isEmpty()) {
      return maxPollMs;
    }

    long msUntilNextFire =
        Duration.between(Instant.now(effective()), earliestNextFire.get()).toMillis();

    long targetDelay = Math.max(msUntilNextFire - 500, minPollMs);
    return Math.min(targetDelay, maxPollMs);
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }

  private void renewLease(SingletonLease lease) {
    try {
      if (!lease.renew(LEASE_TTL)) {
        log.warnf("RecurringScheduler could not renew singleton lease %s", lease.name());
      }
    } catch (Exception e) {
      log.warnf(e, "RecurringScheduler lease renewal failed for %s", lease.name());
    }
  }

  private void scheduleNext(long delayMs) {
    synchronized (scheduleLock) {
      scheduleNextLocked(delayMs);
    }
  }

  private void scheduleNextLocked(long delayMs) {
    if (!started.get()) {
      return;
    }
    currentDelayMs = delayMs;
    handle = executor.schedule(this::run, delayMs, TimeUnit.MILLISECONDS);
  }
}
