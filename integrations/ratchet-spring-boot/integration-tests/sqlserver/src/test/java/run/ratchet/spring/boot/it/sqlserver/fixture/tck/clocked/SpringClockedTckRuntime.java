/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.spring.boot.it.sqlserver.fixture.tck.clocked;

import java.util.Objects;
import java.util.Optional;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.tck.api.ListenerProbe;
import run.ratchet.tck.api.RatchetTckProbe;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.RatchetTckRuntimeSupport;
import run.ratchet.tck.api.TestClock;
import run.ratchet.tck.store.clocked.InMemoryJobStore;

/** TCK runtime backed by the shared in-memory store and stepping clock. */
public final class SpringClockedTckRuntime implements RatchetTckRuntime {

  private final JobSchedulerService scheduler;
  private final ListenerProbe probe;
  private final DrainController drainController;
  private final JobExecutorService executor;
  private final TestClock clock;
  private final InMemoryJobStore store;

  public SpringClockedTckRuntime(
      JobSchedulerService scheduler,
      ListenerProbe probe,
      DrainController drainController,
      JobExecutorService executor,
      TestClock clock,
      InMemoryJobStore store) {
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.probe = Objects.requireNonNull(probe, "probe");
    this.drainController = Objects.requireNonNull(drainController, "drainController");
    this.executor = Objects.requireNonNull(executor, "executor");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.store = Objects.requireNonNull(store, "store");
  }

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
    return Optional.of(clock);
  }

  @Override
  public void clear() {
    RatchetTckRuntimeSupport.clearRuntime(
        "SpringClockedTckRuntime",
        drainController::setDraining,
        executor::awaitIdle,
        store::reset,
        probe::reset);
  }
}
