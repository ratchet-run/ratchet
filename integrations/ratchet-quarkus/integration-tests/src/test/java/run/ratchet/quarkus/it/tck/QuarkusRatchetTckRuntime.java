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
package run.ratchet.quarkus.it.tck;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.tck.api.RatchetTckProbe;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TestClock;

/** Quarkus {@link RatchetTckRuntime} bridge for the public-API TCK contracts. */
@ApplicationScoped
public class QuarkusRatchetTckRuntime implements RatchetTckRuntime {

  private static final Duration CLEAR_DRAIN_TIMEOUT = Duration.ofSeconds(30);

  @Inject JobSchedulerService scheduler;
  @Inject QuarkusRatchetTckProbe probe;
  @Inject DrainController drainController;
  @Inject JobExecutorService executor;
  @Inject QuarkusTckStoreCleaner storeCleaner;
  @Inject RatchetOptions options;

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
    drainController.setDraining(true);
    try {
      boolean idle = executor.awaitIdle(CLEAR_DRAIN_TIMEOUT);
      if (!idle) {
        throw new IllegalStateException(
            "QuarkusRatchetTckRuntime.clear(): executor did not become idle within "
                + CLEAR_DRAIN_TIMEOUT);
      }
      storeCleaner.truncateAll();
      probe.reset();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("clear() interrupted", e);
    } finally {
      drainController.setDraining(false);
    }
  }
}
