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
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.BatchBuilder;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobHandle;
import run.ratchet.api.WorkflowCondition;

/**
 * Fluent builder for batches whose children are pre-resolved {@link JobInvocation}s — the
 * invocation-typed mirror of {@link BatchBuilder}.
 *
 * <p>Obtained from {@link InvocationSubmissionService#enqueueInvocationBatch}. Child construction
 * uses a per-item invocation factory instead of a serializable consumer; batch parent semantics
 * (progress counters, completion processing, branch evaluation) are identical to the lambda
 * builder.
 */
@Incubating
public interface InvocationBatchBuilder {

  /**
   * Enqueues one child job per item, each persisting the invocation the factory produces for it.
   *
   * @param items batch items; never {@code null}
   * @param invocationFactory produces the persisted invocation for one item; never {@code null},
   *     must not return {@code null}
   */
  <T extends Serializable> InvocationBatchBuilder forEach(
      Collection<T> items, Function<T, JobInvocation> invocationFactory);

  /** Adds a conditional branch evaluated against the batch outcome. */
  InvocationBatchBuilder thenBranch(
      WorkflowCondition condition, JobInvocation next, String description);

  /** Chains a next step executed when every child succeeds. */
  InvocationBatchBuilder thenOnBatchSuccess(JobInvocation next);

  /** Chains a next step executed when at least one child fails. */
  InvocationBatchBuilder thenOnBatchFailure(JobInvocation next);

  /** Requests execution on the virtual-thread executor target. */
  InvocationBatchBuilder virtual();

  /** Requests execution on the platform-thread executor target. */
  InvocationBatchBuilder platform();

  /** Sets the retry backoff policy and base delay for every invocation child. */
  default InvocationBatchBuilder withBackoff(BackoffPolicy policy, Duration param) {
    Objects.requireNonNull(policy, "policy");
    Objects.requireNonNull(param, "param");
    throw new UnsupportedOperationException(
        "Invocation batch child retry backoff is not supported");
  }

  /** Sets the maximum retry count for every invocation child. */
  default InvocationBatchBuilder withMaxRetries(int retries) {
    if (retries < 0) {
      throw new IllegalArgumentException("retries must be at least 0");
    }
    throw new UnsupportedOperationException("Invocation batch child retries are not supported");
  }

  /**
   * Persists the batch parent and all children through the standard creation path.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   */
  JobHandle submit();
}
