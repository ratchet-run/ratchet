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
package run.ratchet.spring.boot.it.mysql.fixture.tck;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.tck.api.ListenerProbe;
import run.ratchet.tck.api.RatchetTckProbe;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.RatchetTckRuntimeSupport;
import run.ratchet.tck.api.TestClock;

/** Spring {@link RatchetTckRuntime} bridge for the public API contracts. */
public final class SpringRatchetTckRuntime implements RatchetTckRuntime {

  private final JobSchedulerService scheduler;
  private final ListenerProbe probe;
  private final DrainController drainController;
  private final JobExecutorService executor;
  private final SpringTckStoreCleaner storeCleaner;
  private final RatchetOptions options;

  public SpringRatchetTckRuntime(
      JobSchedulerService scheduler,
      ListenerProbe probe,
      DrainController drainController,
      JobExecutorService executor,
      SpringTckStoreCleaner storeCleaner,
      RatchetOptions options) {
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.probe = Objects.requireNonNull(probe, "probe");
    this.drainController = Objects.requireNonNull(drainController, "drainController");
    this.executor = Objects.requireNonNull(executor, "executor");
    this.storeCleaner = Objects.requireNonNull(storeCleaner, "storeCleaner");
    this.options = Objects.requireNonNull(options, "options");
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
    return Optional.empty();
  }

  @Override
  public OptionalLong maxPayloadBytes() {
    return OptionalLong.of(Math.multiplyExact((long) options.payload().maxPayloadKb(), 1024L));
  }

  @Override
  public boolean supportsCallerTransactionRollback() {
    return true;
  }

  @Override
  public void clear() {
    RatchetTckRuntimeSupport.clearRuntime(
        "SpringRatchetTckRuntime",
        drainController::setDraining,
        executor::awaitIdle,
        storeCleaner::truncateAll,
        probe::reset);
  }
}
