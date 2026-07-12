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
package run.ratchet.tck.api;

import java.util.Optional;
import java.util.OptionalLong;
import run.ratchet.api.JobSchedulerService;

/**
 * Implementation-supplied bridge between the Ratchet API TCK and a concrete Ratchet runtime. A
 * passing TCK run earns the implementation the "Ratchet API Compatible" label.
 *
 * <p>Implementations provide:
 *
 * <ul>
 *   <li>a live {@link JobSchedulerService} under test,
 *   <li>a {@link RatchetTckProbe} that observes lifecycle events for that scheduler,
 *   <li>optionally a {@link TestClock} when the implementation can drive its scheduler from a
 *       controllable clock,
 *   <li>a non-destructive {@link #clear() drain} so tests are isolated from each other.
 * </ul>
 *
 * <p>The runtime is passed to seed-contract base classes (see the {@code Abstract*Contract} types
 * in this package) via an abstract {@code runtime()} method on the subclass.
 */
public interface RatchetTckRuntime {

  /** The scheduler under test. The same instance is returned across calls within one test. */
  JobSchedulerService scheduler();

  /**
   * Probe observing the scheduler. The same instance is returned across calls within one test. MUST
   * be wired before the first job is submitted in a test (typically in {@code @BeforeEach} or
   * eagerly at construction).
   */
  RatchetTckProbe probe();

  /**
   * Optional deterministic clock. Returns empty when the implementation's scheduler is driven by
   * wall-clock time. Contracts that require deterministic time call {@code
   * Assumptions.assumeTrue(runtime.clock().isPresent())} to skip on wall-clock implementations.
   */
  Optional<TestClock> clock();

  /**
   * Configured maximum serialized job-payload size in UTF-8 bytes. Implementations that expose a
   * limit return it so {@link AbstractPayloadSizeContract} can exercise rejection behavior.
   * Implementations without a configurable limit return empty and skip that optional contract.
   */
  default OptionalLong maxPayloadBytes() {
    return OptionalLong.empty();
  }

  /**
   * Drains the scheduler so each test starts from a clean baseline. The contract is intentionally
   * detailed because under-specified drains let probe state from one test poison another.
   *
   * <h2>{@code clear()} contract — implementations MUST satisfy all seven rules</h2>
   *
   * <ol>
   *   <li><b>Quiesce-then-join, non-destructive.</b> {@code clear()} MUST NOT return until every
   *       currently-executing worker has reached a terminal state (completed / failed / cancelled).
   *       Implementations that run workers on a pool MUST cancel + join before returning. {@code
   *       clear()} MUST NOT shut the scheduler down — the bean stays alive for subsequent tests.
   *       The required primitive shape is "pause new submissions" + "{@code awaitIdle(timeout)}" +
   *       "resume".
   *   <li><b>Pending retries.</b> Jobs with retry attempts scheduled MUST be force-cancelled during
   *       {@code clear()}. {@code clear()} MUST NOT wait for pending retry timers to fire.
   *   <li><b>Workflow children.</b> Jobs spawned by a parent mid-drain MUST also be drained before
   *       {@code clear()} returns. The implementation MUST quiesce the scheduler against
   *       <em>new</em> submissions before joining workers, or loop drain-then-check until the
   *       executor is idle.
   *   <li><b>Recurring jobs.</b> Any recurring jobs registered before {@code clear()} MUST be
   *       paused or de-scheduled during the drain so their next tick cannot leak into the following
   *       test. Tests that need recurring jobs re-register them in {@code @BeforeEach}.
   *   <li><b>Probe reset.</b> After {@code clear()} returns, {@code probe().events(anyHandle)} MUST
   *       return an empty list for every handle issued before the call.
   *   <li><b>Late-event drop.</b> Probe events arriving after {@code clear()} from a prior test's
   *       job MUST be dropped (handle no longer tracked) rather than enqueued against a fresh
   *       handle. Auto-fired recurring jobs that have no caller-issued handle are covered by rule 4
   *       — if any leak through, the implementation's {@code clear()} is buggy.
   *   <li><b>Test integration.</b> Seed contracts call {@code clear()} in {@code @AfterEach}.
   *       Implementations whose tests run several contracts in sequence within one container
   *       lifecycle MUST tolerate repeated invocation.
   * </ol>
   *
   * <p>The implementation is free to bound this drain with an internal timeout to prevent an
   * unbounded await in the face of a stuck worker; on timeout it MUST throw rather than silently
   * leak state into the next test. A reasonable default is {@link Duration#ofSeconds(30)}.
   */
  void clear();
}
