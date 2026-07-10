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
import java.util.function.Function;
import java.util.stream.Stream;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobHandle;
import run.ratchet.api.StreamingBatchBuilder;
import run.ratchet.api.WorkflowCondition;

/**
 * Fluent builder for streaming batches whose per-item children are pre-resolved {@link
 * JobInvocation}s — the invocation-typed mirror of {@link StreamingBatchBuilder}.
 *
 * <p>Chunking, chunk-boundary persistence, and replay semantics are shared with the lambda builder;
 * only per-item child construction differs (an invocation factory instead of a serializable
 * consumer). A chunk whose bulk insert fails emits {@code BatchChunkFailureEvent} and aborts the
 * submission.
 */
@Incubating
public interface InvocationStreamingBatchBuilder<T extends Serializable> {

  /** Supplies the item stream; consumed once at {@link #start()}. */
  InvocationStreamingBatchBuilder<T> fromStream(Stream<T> stream);

  /**
   * Sets the per-item invocation factory.
   *
   * @param invocationFactory produces the persisted invocation for one item; never {@code null},
   *     must not return {@code null}
   */
  InvocationStreamingBatchBuilder<T> process(Function<T, JobInvocation> invocationFactory);

  /** Sets the chunk size (default 100, minimum 1). */
  InvocationStreamingBatchBuilder<T> withChunkSize(int size);

  /** Requests execution on the virtual-thread executor target. */
  InvocationStreamingBatchBuilder<T> virtual();

  /** Requests execution on the platform-thread executor target. */
  InvocationStreamingBatchBuilder<T> platform();

  /** Adds a conditional branch evaluated against the batch outcome. */
  InvocationStreamingBatchBuilder<T> thenBranch(
      WorkflowCondition condition, JobInvocation next, String description);

  /** Chains a next step executed when every child succeeds. */
  InvocationStreamingBatchBuilder<T> thenOnBatchSuccess(JobInvocation next);

  /** Chains a next step executed when at least one child fails. */
  InvocationStreamingBatchBuilder<T> thenOnBatchFailure(JobInvocation next);

  /**
   * Consumes the stream, persisting children chunk by chunk through the standard creation path.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}.
   */
  JobHandle start();
}
