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
package run.ratchet.micronaut.internal.tx;

import io.micronaut.core.annotation.Creator;
import java.time.Clock;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.ri.core.DefaultJobCreationService;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.CallerPrincipalResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

/**
 * Micronaut proxy carrier for {@link DefaultJobCreationService}; see {@link
 * RatchetTransactionBoundary} and the package documentation for why.
 */
public class MicronautDefaultJobCreationService extends DefaultJobCreationService {
  @Creator
  public MicronautDefaultJobCreationService() {
    super();
  }

  public MicronautDefaultJobCreationService(
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      JobBulkStore jobBulkStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      RecurringJobStore recurringJobStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler,
      JobInvocationResolver jobInvocationResolver,
      JobPayloadInputValidator payloadValidator,
      CallerPrincipalProvider callerPrincipalProvider,
      TracingCollector tracingCollector,
      JobAuthorizationPolicy authorizationPolicy,
      ClassPolicy classPolicy,
      InternalEventPublisher eventPublisher,
      MetricsCollector metricsCollector,
      Clock clock,
      boolean signalCapabilityAvailable,
      boolean resourcePermitCapabilityAvailable,
      CallerPrincipalResolver callerPrincipalResolver,
      AfterCommitRegistrar afterCommitRegistrar,
      ErrorSanitizer errorSanitizer) {
    super(
        jobBatchStatusStore,
        jobTerminalStore,
        jobCrudStore,
        jobBulkStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        recurringJobStore,
        wakeupService,
        recurringScheduler,
        jobInvocationResolver,
        payloadValidator,
        callerPrincipalProvider,
        tracingCollector,
        authorizationPolicy,
        classPolicy,
        eventPublisher,
        metricsCollector,
        clock,
        signalCapabilityAvailable,
        resourcePermitCapabilityAvailable,
        callerPrincipalResolver,
        afterCommitRegistrar,
        errorSanitizer);
  }
}
