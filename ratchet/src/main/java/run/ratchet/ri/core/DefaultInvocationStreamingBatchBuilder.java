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

import java.io.Serializable;
import java.time.Duration;
import java.util.function.Function;
import java.util.stream.Stream;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.WorkflowBranch;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.spi.InvocationStreamingBatchBuilder;
import run.ratchet.spi.JobInvocation;

/**
 * Invocation-typed facade over {@link InvocationStreamingState}: branch targets ride through {@link
 * InvocationAdapter}, and submission converges on the shared streaming chunk loop in {@code
 * DefaultJobCreationService}.
 */
final class DefaultInvocationStreamingBatchBuilder<T extends Serializable>
    implements InvocationStreamingBatchBuilder<T> {

  private final InvocationStreamingState<T> state;

  DefaultInvocationStreamingBatchBuilder(String name, StreamingBatchSubmitter submitter) {
    this.state = new InvocationStreamingState<>(name, submitter);
  }

  @Override
  public InvocationStreamingBatchBuilder<T> fromStream(Stream<T> stream) {
    state.fromStream(stream);
    return this;
  }

  @Override
  public InvocationStreamingBatchBuilder<T> process(Function<T, JobInvocation> invocationFactory) {
    state.setInvocationFactory(invocationFactory);
    return this;
  }

  @Override
  public InvocationStreamingBatchBuilder<T> withChunkSize(int size) {
    state.withChunkSize(size);
    return this;
  }

  @Override
  public InvocationStreamingBatchBuilder<T> virtual() {
    state.virtual();
    return this;
  }

  @Override
  public InvocationStreamingBatchBuilder<T> platform() {
    state.platform();
    return this;
  }

  @Override
  public InvocationStreamingBatchBuilder<T> withBackoff(BackoffPolicy policy, Duration param) {
    state.withBackoff(policy, param);
    return this;
  }

  @Override
  public InvocationStreamingBatchBuilder<T> withMaxRetries(int retries) {
    state.withMaxRetries(retries);
    return this;
  }

  @Override
  public InvocationStreamingBatchBuilder<T> thenBranch(
      WorkflowCondition condition, JobInvocation next, String description) {
    state.addBranch(new WorkflowBranch(condition, new InvocationAdapter(next), description));
    return this;
  }

  @Override
  public InvocationStreamingBatchBuilder<T> thenOnBatchSuccess(JobInvocation next) {
    state.thenOnBatchSuccess(new InvocationAdapter(next));
    return this;
  }

  @Override
  public InvocationStreamingBatchBuilder<T> thenOnBatchFailure(JobInvocation next) {
    state.thenOnBatchFailure(new InvocationAdapter(next));
    return this;
  }

  @Override
  public JobHandle start() {
    return state.start();
  }
}
