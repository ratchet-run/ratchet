package run.ratchet.testsuite.tck;

import run.ratchet.api.JobSchedulerService;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.tck.api.RatchetTckProbe;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TestClock;
import run.ratchet.testsuite.app.TestCleanupStrategy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Optional;

/**
 * Reference-implementation bridge for the public-API TCK. Wires:
 *
 * <ul>
 *   <li>{@code scheduler()} → CDI-injected {@link JobSchedulerService}.
 *   <li>{@code probe()} → {@link ListenerProbe}, which subscribes via {@code
 *       addEventListener(Consumer&lt;Object&gt;)}.
 *   <li>{@code clock()} → {@link Optional#empty()}; the RI is wall-clock-driven today, so {@link
 *       run.ratchet.tck.api.AbstractDelayedSchedulingContract} skips via JUnit assumption.
 *       The RI subclass also carries an explicit {@code @Disabled} so the skip is visible in test
 *       reports rather than silently swallowed.
 *   <li>{@code clear()} → drain-controller-pause + non-destructive {@link
 *       JobExecutorService#awaitIdle(Duration)} + store truncate via {@link TestCleanupStrategy} +
 *       probe reset + drain-controller-resume.
 * </ul>
 */
@ApplicationScoped
public class RiRatchetTckRuntime implements RatchetTckRuntime {

  /** Bound on {@link JobExecutorService#awaitIdle(Duration)} during a clear(). */
  private static final Duration CLEAR_DRAIN_TIMEOUT = Duration.ofSeconds(30);

  @Inject private JobSchedulerService scheduler;
  @Inject private ListenerProbe probe;
  @Inject private DrainController drainController;
  @Inject private JobExecutorService executor;
  @Inject private TestCleanupStrategy cleanupStrategy;

  @Override
  public JobSchedulerService scheduler() {
    return scheduler;
  }

  @Override
  public RatchetTckProbe probe() {
    return probe;
  }

  @Override
  public Optional<TestClock> clock() {
    return Optional.empty();
  }

  @Override
  public void clear() {
    drainController.setDraining(true);
    try {
      boolean idle = executor.awaitIdle(CLEAR_DRAIN_TIMEOUT);
      if (!idle) {
        throw new IllegalStateException(
            "RiRatchetTckRuntime.clear(): executor did not become idle within "
                + CLEAR_DRAIN_TIMEOUT
                + " — implementation drain is buggy or a worker is stuck");
      }
      cleanupStrategy.truncateAll();
      probe.reset();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("clear() interrupted", e);
    } finally {
      drainController.setDraining(false);
    }
  }
}
