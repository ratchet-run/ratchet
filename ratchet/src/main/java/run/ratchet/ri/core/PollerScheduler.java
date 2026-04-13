package run.ratchet.ri.core;

import run.ratchet.spi.ExecutorProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;

/**
 * Owns the scheduling infrastructure for the job poller: executor lifecycle, dynamic poll delays,
 * and wakeup signals. Delegates each poll cycle to {@link Poller#tick()}.
 *
 * @see Poller
 */
@ApplicationScoped
public class PollerScheduler {

  private static final Logger log = Logger.getLogger(PollerScheduler.class);

  private final AtomicBoolean started = new AtomicBoolean();
  private final ExecutorProvider executorProvider;
  private final Poller poller;

  @SuppressWarnings("java:S3077")
  private volatile Future<?> handle;

  /**
   * Cached executor reference resolved once during {@link #start()}, avoiding CDI proxy lookups.
   */
  @SuppressWarnings("java:S3077")
  private volatile ScheduledExecutorService executor;

  protected PollerScheduler() {
    this.executorProvider = null;
    this.poller = null;
  }

  @Inject
  public PollerScheduler(ExecutorProvider executorProvider, Poller poller) {
    this.executorProvider = executorProvider;
    this.poller = poller;
  }

  public void start() {
    if (!started.compareAndSet(false, true)) {
      log.warn("PollerScheduler already started; skipping re-start");
      return;
    }

    executor = executorProvider.getScheduledExecutor();
    log.info("PollerScheduler starting");
    scheduleNext(0);
    log.info("PollerScheduler started");
  }

  public void stop() {
    if (!started.compareAndSet(true, false)) {
      return;
    }

    log.info("PollerScheduler stopping");
    cancelCurrentSchedule();
    log.info("PollerScheduler stopped");
  }

  /**
   * Wakes up the poller to immediately check for available jobs.
   *
   * <p>Called when a job notification is received from the cluster, indicating that new work is
   * available.
   */
  public void wakeup() {
    if (!started.get()) {
      return;
    }

    poller.onWakeup();

    cancelCurrentSchedule();
    scheduleNext(0);

    log.debug("PollerScheduler wakeup triggered - immediate poll scheduled");
  }

  void scheduleNext(long delayMs) {
    if (!started.get()) {
      return;
    }

    handle = executor.schedule(this::executePollCycle, delayMs, TimeUnit.MILLISECONDS);
  }

  private void cancelCurrentSchedule() {
    Future<?> currentHandle = handle;
    if (currentHandle != null && !currentHandle.isDone()) {
      currentHandle.cancel(false);
    }
    handle = null;
  }

  @SuppressWarnings("java:S1181")
  private void executePollCycle() {
    if (!started.get()) {
      return;
    }

    try {
      long nextDelayMs = poller.tick();
      scheduleNext(nextDelayMs);
    } catch (Throwable t) {
      if (!started.get()) {
        return;
      }
      // CDI context gone (e.g. Arquillian undeploy) — stop permanently, next deploy starts fresh
      if (SchedulerUtils.isCdiContextGone(t)) {
        started.set(false);
        log.info("Poll cycle detected inactive CDI context — stopping permanently");
        return;
      }
      log.error("Poll cycle failed", t);
      try {
        scheduleNext(5000);
      } catch (Exception e) {
        log.debug("Cannot reschedule poll cycle — scheduler will restart on next deploy", e);
      }
    }
  }
}
