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

import java.time.Duration;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.WorkflowCondition;

/**
 * Fluent builder for jobs created from pre-resolved {@link JobInvocation}s — the invocation-typed
 * mirror of {@link JobBuilder}, accepting a {@code JobInvocation} everywhere the public builder
 * accepts a serializable lambda.
 *
 * <p>Obtained from {@link InvocationSubmissionService}. Branch composition reuses {@link
 * WorkflowCondition} directly, mirroring the lambda side (there is no separate branch builder).
 * Context-consuming success/failure callbacks are intentionally absent: they take {@code
 * JobContext}-typed lambdas, not invocations, and trusted extensions compose follow-up work with
 * {@link #then} / {@link #branch} instead.
 *
 * <p>Persistence semantics — validation, class policy, idempotency, tags, wakeup — are identical to
 * the lambda builder; both converge on the same creation path.
 */
@Incubating
public interface InvocationJobBuilder {

  /** Chains a next step executed after this job completes. */
  InvocationJobBuilder then(JobInvocation next);

  /** Chains a next step executed only when this job succeeds. */
  InvocationJobBuilder thenOnSuccess(JobInvocation next);

  /** Chains a next step executed only when this job fails. */
  InvocationJobBuilder thenOnFailure(JobInvocation next);

  /** Adds a conditional branch evaluated against this job's outcome. */
  InvocationJobBuilder branch(WorkflowCondition condition, JobInvocation next, String description);

  /** Adds a conditional branch without a description. */
  InvocationJobBuilder when(WorkflowCondition condition, JobInvocation next);

  /** Schedules the job for immediate execution, overriding any delay. */
  InvocationJobBuilder immediate();

  /** Parks the job WAITING for an external signal before it becomes executable. */
  InvocationJobBuilder awaitSignal(String signalKey, Duration timeout);

  /** Sets the idempotency key. */
  InvocationJobBuilder withIdempotencyKey(String key);

  /**
   * Sets the business key under the portable contract documented by {@link
   * JobBuilder#withBusinessKey(String)}.
   */
  InvocationJobBuilder withBusinessKey(String key);

  /** Gates execution on a named resource permit. */
  InvocationJobBuilder withResource(String resourceName);

  /** Requests execution on the virtual-thread executor target. */
  InvocationJobBuilder virtual();

  /** Requests execution on the platform-thread executor target. */
  InvocationJobBuilder platform();

  /** Sets the retry backoff policy. */
  InvocationJobBuilder withBackoff(BackoffPolicy policy, Duration param);

  /** Sets the maximum retry count. */
  InvocationJobBuilder withMaxRetries(int retries);

  /** Adds one cleartext key/value parameter. */
  InvocationJobBuilder withParam(String key, String value);

  /** Sets the priority. */
  InvocationJobBuilder withPriority(JobPriority priority);

  /** Adds tags. */
  InvocationJobBuilder withTags(String... tags);

  /** Sets the execution timeout. */
  InvocationJobBuilder withTimeout(Duration timeout);

  /** Opts this job into payload encryption at rest. */
  InvocationJobBuilder withEncryptedPayload();

  /**
   * Persists the job (and any chained steps and branches) through the standard creation path.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   */
  JobHandle submit();
}
