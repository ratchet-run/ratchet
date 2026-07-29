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
package run.ratchet.testsuite.tck.clocked;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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
    RatchetTckRuntimeSupport.clearRuntime(
        "RiClockedTckRuntime",
        drainController::setDraining,
        executor::awaitIdle,
        inMemoryJobStore::reset,
        probe::reset);
  }
}
