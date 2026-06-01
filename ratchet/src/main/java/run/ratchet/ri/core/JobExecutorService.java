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
package run.ratchet.ri.core;

import java.time.Duration;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;

/**
 * SPI for executing jobs on the configured executor. Default implementation: {@link
 * run.ratchet.ri.core.internal.DefaultJobExecutorService}.
 *
 * @apiNote Framework SPI consumed by ri.core.JobSubmissionService / JobExecutionCoordinator and by
 *     ratchet-testsuite integration tests. Applications must not implement this interface.
 */
public interface JobExecutorService {

  ExecutionResult execute(JobEntity job, String poolName);

  ExecutionResult execute(JobClaimDto claim, String poolName);

  /**
   * Non-destructive drain: blocks until all currently-executing jobs reach a terminal state, or
   * until {@code timeout} elapses. Does NOT cancel running tasks. Pair with {@link
   * DrainController#setDraining(boolean)} on entry to prevent new work from arriving.
   *
   * @return {@code true} if the executor became idle within the timeout; {@code false} otherwise
   */
  boolean awaitIdle(Duration timeout) throws InterruptedException;

  int shutdownActiveExecutions();
}
