package run.ratchet.ri.core;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.LockStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;

/**
 * Manages the lifecycle of recurring jobs by monitoring cron-based schedules and spawning
 * individual job instances at appropriate times. This service ensures recurring jobs execute
 * according to their schedules even across system restarts and node failures.
 *
 * <p>The RecurringScheduler implements a master-child pattern where:
 *
 * <ul>
 *   <li><b>Master Jobs:</b> RECURRING type jobs that define the schedule and template
 *   <li><b>Child Jobs:</b> SINGLE type jobs spawned for each scheduled execution
 *   <li><b>Catch-up Mode:</b> Enqueues missed executions if the system was down
 *   <li><b>Next Fire Calculation:</b> Updates master with next execution time
 * </ul>
 *
 * <p>Key features:
 *
 * <ul>
 *   <li><b>Cron Expression Support:</b> Uses Quartz-compatible cron syntax
 *   <li><b>Timezone Awareness:</b> Each job can have its own timezone
 *   <li><b>Leader Election:</b> Uses distributed locking to ensure only one node processes
 *       recurring jobs
 *   <li><b>Graceful Termination:</b> Jobs without valid future executions are automatically
 *       canceled
 * </ul>
 *
 * @see RecurringJobExecutor for transactional job processing
 * @see LockStore for distributed coordination
 */
@ApplicationScoped
public class RecurringScheduler {

  /** Shared cron expression parser configured for Quartz-compatible syntax. */
  public static final CronParser PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

  private static final Logger log = Logger.getLogger(RecurringScheduler.class);

  /** Distributed lock name used for leader election. */
  private static final String LOCK_NAME = "recurringScheduler";

  private final AtomicBoolean started = new AtomicBoolean();

  /** Provider for executor services. */
  private final ExecutorProvider executorProvider;

  /** Store for querying and updating job entities. */
  private final JobCrudStore jobCrudStore;

  /** Store for distributed lock acquisition and renewal. */
  private final LockStore lockStore;

  /** Provides the unique node identifier for lock ownership attribution. */
  private final NodeIdentityProvider nodeIdentityProvider;

  /** Handles the transactional processing of recurring jobs. */
  private final RecurringJobExecutor recurringJobExecutor;

  /** The poller scheduler to wake when children are spawned. */
  private final PollerScheduler pollerScheduler;

  /** Maximum number of recurring jobs to process per scan cycle. */
  private volatile int batchLimit = 20;

  /** Minimum delay between recurring job scans in milliseconds. */
  private volatile long minPollMs = 1000;

  /** Maximum delay between recurring job scans in milliseconds. */
  private volatile long maxPollMs = 60000;

  /** Current delay between scans, adjusted dynamically based on next-fire times. */
  private volatile long currentDelayMs;

  /** Handle to the currently scheduled scan task for cancellation during shutdown. */
  @SuppressWarnings("java:S3077")
  private volatile Future<?> handle;

  /** Cached executor reference resolved once during {@link #init()}, avoiding CDI proxy lookups. */
  @SuppressWarnings("java:S3077")
  private volatile ScheduledExecutorService executor;

  // Required by CDI proxy
  protected RecurringScheduler() {
    this.executorProvider = null;
    this.jobCrudStore = null;
    this.lockStore = null;
    this.nodeIdentityProvider = null;
    this.recurringJobExecutor = null;
    this.pollerScheduler = null;
  }

  @Inject
  public RecurringScheduler(
      ExecutorProvider executorProvider,
      JobCrudStore jobCrudStore,
      LockStore lockStore,
      NodeIdentityProvider nodeIdentityProvider,
      RecurringJobExecutor recurringJobExecutor,
      PollerScheduler pollerScheduler) {
    this.executorProvider = executorProvider;
    this.jobCrudStore = jobCrudStore;
    this.lockStore = lockStore;
    this.nodeIdentityProvider = nodeIdentityProvider;
    this.recurringJobExecutor = recurringJobExecutor;
    this.pollerScheduler = pollerScheduler;
  }

  public long getCurrentDelayMs() {
    return currentDelayMs;
  }

  /**
   * Configures polling parameters. Must be called before {@link #init()}.
   *
   * @param minPollMs minimum scan frequency
   * @param maxPollMs maximum delay between scans
   * @param batchLimit max jobs processed per scan
   */
  public void configure(long minPollMs, long maxPollMs, int batchLimit) {
    this.minPollMs = minPollMs;
    this.maxPollMs = maxPollMs;
    this.batchLimit = batchLimit;
  }

  /** Initializes the recurring scheduler with adaptive polling. */
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

  /**
   * Triggers an immediate scan cycle when a new recurring job is submitted. This cancels any
   * pending long-delay scan and replaces it with an immediate one, ensuring newly submitted
   * recurring jobs are picked up promptly rather than waiting for the next scheduled poll.
   */
  public void kick() {
    if (!started.get()) {
      return;
    }
    Future<?> current = handle;
    if (current != null) {
      current.cancel(false);
    }
    scheduleNext(minPollMs);
    log.debug("RecurringScheduler kicked — immediate scan scheduled");
  }

  /** Stops the recurring scheduler. */
  public void stop() {
    started.set(false);
    if (handle != null) {
      handle.cancel(true);
      handle = null;
    }
  }

  /** Executes a single recurring job scan cycle. */
  @SuppressWarnings("java:S1181")
  void run() {
    if (!started.get()) {
      return;
    }

    ScheduledFuture<?> renewalTask = null;
    try {
      // leader election via DB lock (5-minute TTL)
      if (!lockStore.tryLock(LOCK_NAME, Duration.ofMinutes(5), nodeIdentityProvider.getNodeId())) {
        scheduleNext(minPollMs);
        return;
      }

      // Start lock renewal task
      renewalTask =
          executor.scheduleAtFixedRate(
              () ->
                  lockStore.renewLock(
                      LOCK_NAME, Duration.ofMinutes(5), nodeIdentityProvider.getNodeId()),
              2,
              2,
              TimeUnit.MINUTES);

      int processedCount =
          recurringJobExecutor.process(batchLimit, nodeIdentityProvider.getNodeId());

      // Wake the poller so it picks up spawned children immediately
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
      if (isCdiContextGone(ex)) {
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
      try {
        lockStore.unlock(LOCK_NAME, nodeIdentityProvider.getNodeId());
      } catch (Exception e) {
        log.debug("Failed to release recurring scheduler lock", e);
      }
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

    long msUntilNextFire = Duration.between(Instant.now(), earliestNextFire.get()).toMillis();

    long targetDelay = Math.max(msUntilNextFire - 500, minPollMs);
    return Math.min(targetDelay, maxPollMs);
  }

  /** Checks whether the throwable indicates the CDI application context has been torn down. */
  private static boolean isCdiContextGone(Throwable t) {
    Throwable current = t;
    while (current != null) {
      String name = current.getClass().getName();
      if (name.contains("ContextNotActiveException") || name.contains("ContextNotAliveException")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private void scheduleNext(long delayMs) {
    if (!started.get()) {
      return;
    }
    currentDelayMs = delayMs;
    handle = executor.schedule(this::run, delayMs, TimeUnit.MILLISECONDS);
  }
}
