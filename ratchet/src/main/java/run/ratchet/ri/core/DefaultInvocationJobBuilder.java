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
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.spi.InvocationJobBuilder;
import run.ratchet.spi.JobInvocation;

/**
 * Invocation-typed facade over {@link DefaultJobBuilder}: every {@link JobInvocation} is wrapped in
 * an {@link InvocationAdapter} so the existing builder state, chain handling, branch handling, and
 * the single creation path in {@code DefaultJobCreationService} apply unchanged.
 */
final class DefaultInvocationJobBuilder implements InvocationJobBuilder {

  private final JobBuilder delegate;

  DefaultInvocationJobBuilder(JobBuilder delegate) {
    this.delegate = delegate;
  }

  @Override
  public InvocationJobBuilder then(JobInvocation next) {
    delegate.then(new InvocationAdapter(next));
    return this;
  }

  @Override
  public InvocationJobBuilder thenOnSuccess(JobInvocation next) {
    delegate.thenOnSuccess(new InvocationAdapter(next));
    return this;
  }

  @Override
  public InvocationJobBuilder thenOnFailure(JobInvocation next) {
    delegate.thenOnFailure(new InvocationAdapter(next));
    return this;
  }

  @Override
  public InvocationJobBuilder branch(
      WorkflowCondition condition, JobInvocation next, String description) {
    delegate.branch(condition, new InvocationAdapter(next), description);
    return this;
  }

  @Override
  public InvocationJobBuilder when(WorkflowCondition condition, JobInvocation next) {
    delegate.branch(condition, new InvocationAdapter(next), null);
    return this;
  }

  @Override
  public InvocationJobBuilder immediate() {
    delegate.immediate();
    return this;
  }

  @Override
  public InvocationJobBuilder awaitSignal(String signalKey, Duration timeout) {
    delegate.awaitSignal(signalKey, timeout);
    return this;
  }

  @Override
  public InvocationJobBuilder withIdempotencyKey(String key) {
    delegate.withIdempotencyKey(key);
    return this;
  }

  @Override
  public InvocationJobBuilder withBusinessKey(String key) {
    delegate.withBusinessKey(key);
    return this;
  }

  @Override
  public InvocationJobBuilder withResource(String resourceName) {
    delegate.withResource(resourceName);
    return this;
  }

  @Override
  public InvocationJobBuilder virtual() {
    delegate.virtual();
    return this;
  }

  @Override
  public InvocationJobBuilder platform() {
    delegate.platform();
    return this;
  }

  @Override
  public InvocationJobBuilder withBackoff(BackoffPolicy policy, Duration param) {
    delegate.withBackoff(policy, param);
    return this;
  }

  @Override
  public InvocationJobBuilder withMaxRetries(int retries) {
    delegate.withMaxRetries(retries);
    return this;
  }

  @Override
  public InvocationJobBuilder withParam(String key, String value) {
    delegate.withParam(key, value);
    return this;
  }

  @Override
  public InvocationJobBuilder withPriority(JobPriority priority) {
    delegate.withPriority(priority);
    return this;
  }

  @Override
  public InvocationJobBuilder withTags(String... tags) {
    delegate.withTags(tags);
    return this;
  }

  @Override
  public InvocationJobBuilder withTimeout(Duration timeout) {
    delegate.withTimeout(timeout);
    return this;
  }

  @Override
  public InvocationJobBuilder withEncryptedPayload() {
    delegate.withEncryptedPayload();
    return this;
  }

  @Override
  public JobHandle submit() {
    return delegate.submit();
  }
}
