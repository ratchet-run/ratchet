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
package run.ratchet.quarkus.it.tck.clocked;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Optional;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.quarkus.it.tck.QuarkusRatchetTckProbe;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.tck.api.RatchetTckProbe;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TestClock;

/**
 * Quarkus {@link RatchetTckRuntime} variant backed by an in-memory store and controllable clock.
 */
@Alternative
@ApplicationScoped
public class QuarkusClockedTckRuntime implements RatchetTckRuntime {

  private static final Duration CLEAR_DRAIN_TIMEOUT = Duration.ofSeconds(30);

  @Inject JobSchedulerService scheduler;
  @Inject QuarkusRatchetTckProbe probe;
  @Inject DrainController drainController;
  @Inject JobExecutorService executor;
  @Inject TestClock testClock;
  @Inject InMemoryJobStore inMemoryJobStore;

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
    drainController.setDraining(true);
    try {
      boolean idle = executor.awaitIdle(CLEAR_DRAIN_TIMEOUT);
      if (!idle) {
        throw new IllegalStateException(
            "QuarkusClockedTckRuntime.clear(): executor did not become idle within "
                + CLEAR_DRAIN_TIMEOUT);
      }
      inMemoryJobStore.reset();
      probe.reset();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("clear() interrupted", e);
    } finally {
      drainController.setDraining(false);
    }
  }
}
