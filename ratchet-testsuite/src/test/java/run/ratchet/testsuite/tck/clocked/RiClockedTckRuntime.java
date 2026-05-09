package run.ratchet.testsuite.tck.clocked;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.tck.api.RatchetTckProbe;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TestClock;
import run.ratchet.testsuite.tck.ListenerProbe;
import run.ratchet.testsuite.tck.RiRatchetTckRuntime;

/**
 * RI-side {@link RatchetTckRuntime} variant that exposes a {@link TestClock} and reset semantics
 * for the {@code InMemoryJobStore}. Only used by {@code RiDelayedSchedulingIT} — other Ri*ITs
 * continue to use {@code RiRatchetTckRuntime} against the production MySQL store.
 */
@ApplicationScoped
public class RiClockedTckRuntime implements RatchetTckRuntime {

  @Inject private JobSchedulerService scheduler;
  @Inject private ListenerProbe probe;
  @Inject private DrainController drainController;
  @Inject private JobExecutorService executor;
  @Inject private TestClock testClock;
  @Inject private InMemoryJobStore inMemoryJobStore;

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
    return Optional.of(testClock);
  }

  @Override
  public void clear() {
    RiRatchetTckRuntime.clearRuntime(
        "RiClockedTckRuntime", drainController, executor, inMemoryJobStore::reset, probe);
  }
}
