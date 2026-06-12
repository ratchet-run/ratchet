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
import java.util.Collection;
import java.util.function.Function;
import run.ratchet.api.JobHandle;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.spi.InvocationBatchBuilder;
import run.ratchet.spi.JobInvocation;

/**
 * Invocation-typed facade over {@link DefaultBatchBuilder}: per-item invocations are converted to
 * child payloads by the delegate, and branch targets ride through {@link InvocationAdapter}, so
 * batch persistence stays on the single creation path.
 */
final class DefaultInvocationBatchBuilder implements InvocationBatchBuilder {

  private final DefaultBatchBuilder delegate;

  DefaultInvocationBatchBuilder(DefaultBatchBuilder delegate) {
    this.delegate = delegate;
  }

  @Override
  public <T extends Serializable> InvocationBatchBuilder forEach(
      Collection<T> items, Function<T, JobInvocation> invocationFactory) {
    delegate.forEachInvocation(items, invocationFactory);
    return this;
  }

  @Override
  public InvocationBatchBuilder thenBranch(
      WorkflowCondition condition, JobInvocation next, String description) {
    delegate.thenBranch(condition, new InvocationAdapter(next), description);
    return this;
  }

  @Override
  public InvocationBatchBuilder thenOnBatchSuccess(JobInvocation next) {
    delegate.thenOnBatchSuccess(new InvocationAdapter(next));
    return this;
  }

  @Override
  public InvocationBatchBuilder thenOnBatchFailure(JobInvocation next) {
    delegate.thenOnBatchFailure(new InvocationAdapter(next));
    return this;
  }

  @Override
  public InvocationBatchBuilder virtual() {
    delegate.virtual();
    return this;
  }

  @Override
  public InvocationBatchBuilder platform() {
    delegate.platform();
    return this;
  }

  @Override
  public JobHandle submit() {
    return delegate.submit();
  }
}
