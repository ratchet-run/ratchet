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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.spi.InvocationBatchBuilder;
import run.ratchet.spi.InvocationJobBuilder;
import run.ratchet.spi.InvocationStreamingBatchBuilder;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.JobInvocation;
import run.ratchet.spi.JobInvocationResolver;

/**
 * Reference implementation of {@link InvocationSubmissionService}: thin facades over the existing
 * builders, converging on {@link DefaultJobCreationService} with no separate persistence path.
 */
@ApplicationScoped
public class DefaultInvocationSubmissionService implements InvocationSubmissionService {

  private final DefaultJobCreationService jobCreationService;
  private final JobInvocationResolver jobInvocationResolver;

  /** No-arg constructor required by CDI normal-scope proxying. Not for direct use. */
  protected DefaultInvocationSubmissionService() {
    this.jobCreationService = null;
    this.jobInvocationResolver = null;
  }

  @Inject
  DefaultInvocationSubmissionService(
      DefaultJobCreationService jobCreationService, JobInvocationResolver jobInvocationResolver) {
    this.jobCreationService = jobCreationService;
    this.jobInvocationResolver = jobInvocationResolver;
  }

  @Override
  public InvocationJobBuilder enqueueInvocation(JobInvocation invocation) {
    Objects.requireNonNull(invocation, "invocation must not be null");
    return new DefaultInvocationJobBuilder(
        DefaultJobBuilder.create(
            jobCreationService, new InvocationAdapter(invocation), Duration.ZERO));
  }

  @Override
  public InvocationJobBuilder scheduleInvocation(Duration delay, JobInvocation invocation) {
    Objects.requireNonNull(delay, "delay must not be null");
    Objects.requireNonNull(invocation, "invocation must not be null");
    return new DefaultInvocationJobBuilder(
        DefaultJobBuilder.create(jobCreationService, new InvocationAdapter(invocation), delay));
  }

  @Override
  public InvocationBatchBuilder enqueueInvocationBatch(String name) {
    Objects.requireNonNull(name, "name must not be null");
    return new DefaultInvocationBatchBuilder(
        new DefaultBatchBuilder(name, jobCreationService, jobInvocationResolver));
  }

  @Override
  public <T extends Serializable> InvocationStreamingBatchBuilder<T> invocationStreamingBatch(
      String name) {
    Objects.requireNonNull(name, "name must not be null");
    return new DefaultInvocationStreamingBatchBuilder<>(name, jobCreationService);
  }

  @Override
  public WorkflowCondition invocationCondition(JobInvocation invocation) {
    Objects.requireNonNull(invocation, "invocation must not be null");
    return new WorkflowCondition(WorkflowCondition.ConditionType.CUSTOM, invocation);
  }
}
