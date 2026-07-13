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
package run.ratchet.testsuite.tck;

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
import run.ratchet.testsuite.app.TestCleanupStrategy;

/**
 * Reference-implementation bridge for the public-API TCK. Wires:
 *
 * <ul>
 *   <li>{@code scheduler()} → CDI-injected {@link JobSchedulerService}.
 *   <li>{@code probe()} → {@link ListenerProbe}, which subscribes via {@code
 *       addEventListener(Consumer&lt;Object&gt;)}.
 *   <li>{@code clock()} → {@link Optional#empty()}; the RI is wall-clock-driven today, so {@link
 *       run.ratchet.tck.api.AbstractDelayedSchedulingContract} skips via JUnit assumption.
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
  @Inject private RatchetOptions options;

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
    return !"mongodb".equals(System.getProperty("ratchet.test.db.type", ""));
  }

  @Override
  public void clear() {
    clearRuntime(
        "RiRatchetTckRuntime", drainController, executor, cleanupStrategy::truncateAll, probe);
  }

  public static void clearRuntime(
      String runtimeName,
      DrainController drainController,
      JobExecutorService executor,
      Runnable resetStore,
      ListenerProbe probe) {
    drainController.setDraining(true);
    try {
      boolean idle = executor.awaitIdle(CLEAR_DRAIN_TIMEOUT);
      if (!idle) {
        throw new IllegalStateException(
            runtimeName
                + ".clear(): executor did not become idle within "
                + CLEAR_DRAIN_TIMEOUT
                + " — implementation drain is buggy or a worker is stuck");
      }
      resetStore.run();
      probe.reset();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("clear() interrupted", e);
    } finally {
      drainController.setDraining(false);
    }
  }
}
