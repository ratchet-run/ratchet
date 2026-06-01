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
package run.ratchet.spi;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.Incubating;

/**
 * Supplies the thread pools used for job execution and scheduling.
 *
 * <p>Each accessor must return a stable executor instance for the provider lifecycle. Consumers may
 * retain returned references for cancellation, delayed scheduling, and lifecycle coordination.
 *
 * <p>Executor ownership is implementation-specific. Managed-runtime providers must not shut down
 * container-owned executors, and callers must not invoke shutdown methods on executors they did not
 * create. Standalone providers that create their own pools are responsible for disposing them
 * during application shutdown.
 */
@Incubating
public interface ExecutorProvider {

  /**
   * Returns the executor used for job payload execution. This is the platform pool — the same
   * executor returned by {@link #getJobExecutor(String)} for {@link ExecutorTargets#PLATFORM}.
   *
   * @return job executor; never {@code null}
   */
  ExecutorService getJobExecutor();

  /**
   * Returns the executor that backs a named execution-target pool, if one is configured.
   *
   * <p>{@link ExecutorTargets#PLATFORM} always resolves to the same executor as {@link
   * #getJobExecutor()}. {@link ExecutorTargets#VIRTUAL} resolves only when a virtual executor is
   * configured. Any other (or unconfigured) name returns empty; callers route the fallback rather
   * than the provider hiding it.
   *
   * @param target reserved execution-target name; see {@link ExecutorTargets}
   * @return the executor for {@code target}, or empty when no pool is configured under that name
   */
  default Optional<ExecutorService> getJobExecutor(String target) {
    return ExecutorTargets.PLATFORM.equals(target)
        ? Optional.of(getJobExecutor())
        : Optional.empty();
  }

  /**
   * Returns the scheduler used for timers, watchdogs, and delayed maintenance work.
   *
   * @return scheduled executor; never {@code null}
   */
  ScheduledExecutorService getScheduledExecutor();
}
