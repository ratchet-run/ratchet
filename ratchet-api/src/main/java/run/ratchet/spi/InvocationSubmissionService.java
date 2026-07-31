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

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;
import run.ratchet.api.Incubating;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.WorkflowCondition;

/**
 * Submission seam for trusted extensions that construct {@link JobInvocation}s directly instead of
 * serializing lambda calls.
 *
 * <p>This SPI is <b>trusted by convention</b>, not a security barrier: anything that can inject it
 * can persist an arbitrary invocation, exactly as {@link JobInvocationResolver} consumers can
 * today. Extensions that accept external input (block names, workflow JSON) must validate and
 * authorize <em>before</em> constructing the invocation they hand to this seam; the framework still
 * applies payload validation and the deployment's class policy at persistence time, and the worker
 * re-checks at dispatch time.
 *
 * <p>All submissions converge on the same creation path as the public {@code JobSchedulerService}
 * builders — same validation, idempotency, tags, queue rows, batch metadata, workflow condition
 * rows, and wakeup behavior. There is no separate persistence path.
 */
@Incubating
public interface InvocationSubmissionService {

  /**
   * Starts a builder for a job executing the given invocation as soon as possible.
   *
   * @param invocation pre-resolved invocation; never {@code null}
   * @return builder for options, chaining, and submission; never {@code null}
   */
  InvocationJobBuilder enqueueInvocation(JobInvocation invocation);

  /**
   * Starts a builder for a job executing the given invocation after a delay.
   *
   * @param delay scheduling delay; never {@code null}
   * @param invocation pre-resolved invocation; never {@code null}
   * @return builder for options, chaining, and submission; never {@code null}
   */
  InvocationJobBuilder scheduleInvocation(Duration delay, JobInvocation invocation);

  /**
   * Starts a builder for a recurring job executing the given invocation on a cron schedule.
   *
   * @param cron Quartz cron expression; never {@code null}
   * @param zone timezone used to evaluate the cron expression; never {@code null}
   * @param invocation pre-resolved invocation; never {@code null}
   * @return builder for recurring options and submission; never {@code null}
   */
  RecurringJobBuilder scheduleRecurringInvocation(
      String cron, ZoneId zone, JobInvocation invocation);

  /**
   * Starts a builder for a batch whose children are produced by per-item invocation factories.
   *
   * @param name batch name for diagnostics; never {@code null}
   * @return batch builder; never {@code null}
   */
  InvocationBatchBuilder enqueueInvocationBatch(String name);

  /**
   * Starts a builder for a streaming batch whose per-item children come from an invocation factory.
   *
   * @param name batch name for diagnostics; never {@code null}
   * @return streaming batch builder; never {@code null}
   */
  <T extends Serializable> InvocationStreamingBatchBuilder<T> invocationStreamingBatch(String name);

  /**
   * Wraps a pre-resolved invocation as a {@code CUSTOM} workflow condition.
   *
   * <p>The invocation's target method is dispatched reflectively at evaluation time with the
   * parent's {@code JobResult} supplied as the trailing argument (the same coercion the
   * lambda-predicate path uses) and must return {@code boolean}. No lambda serialization is
   * involved; the invocation persists as the condition's expression payload and is encrypted under
   * the parent's predicate surface when payload encryption applies.
   *
   * @param invocation predicate invocation; never {@code null}
   * @return condition composable into branches exactly like the lambda-based factories
   */
  WorkflowCondition invocationCondition(JobInvocation invocation);
}
