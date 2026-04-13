package run.ratchet.ri.core;

import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;

/**
 * Manages start/stop lifecycle of the job scheduler subsystem. Idempotent: repeated {@link #start}
 * or {@link #shutdown} calls are safe.
 */
public class SchedulerLifecycleManager {

  private static final Logger log = Logger.getLogger(SchedulerLifecycleManager.class);

  private final AtomicBoolean started = new AtomicBoolean(false);

  @SuppressWarnings("java:S3077")
  private volatile Runnable startCallback;

  @SuppressWarnings("java:S3077")
  private volatile Runnable shutdownCallback;

  public void configure(Runnable startCallback, Runnable shutdownCallback) {
    this.startCallback = startCallback;
    this.shutdownCallback = shutdownCallback;
  }

  public boolean isStarted() {
    return started.get();
  }

  /** Gracefully shuts down the job scheduler subsystem. */
  public void shutdown() {
    if (!started.compareAndSet(true, false)) {
      return;
    }

    log.info("Shutting down job scheduler subsystem...");

    if (shutdownCallback != null) {
      shutdownCallback.run();
    }

    log.info("Job scheduler subsystem shut down");
  }

  /** Must be called after database migrations. Idempotent. */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      log.warn("Scheduler already started; skipping re-start");
      return;
    }

    log.info("Starting job scheduler subsystem...");

    if (startCallback != null) {
      startCallback.run();
    }

    log.info("Job scheduler subsystem started");
  }
}
