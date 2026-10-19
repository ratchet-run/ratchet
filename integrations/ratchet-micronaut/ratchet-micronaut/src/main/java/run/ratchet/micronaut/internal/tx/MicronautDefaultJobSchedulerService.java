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
import run.ratchet.api.RatchetOptions;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.ri.core.DefaultJobCreationService;
import run.ratchet.ri.core.DefaultJobSchedulerService;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

/**
 * Micronaut proxy carrier for {@link DefaultJobSchedulerService}; see {@link
 * RatchetTransactionBoundary} and the package documentation for why.
 */
public class MicronautDefaultJobSchedulerService extends DefaultJobSchedulerService {
  @Creator
  public MicronautDefaultJobSchedulerService() {
    super();
  }

  public MicronautDefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobBatchStatusStore jobBatchStatusStore,
      JobPauseStore jobPauseStore,
      JobRetryStore jobRetryStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      RecurringJobStore recurringJobStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler,
      JobInvocationResolver jobInvocationResolver,
      DefaultJobCreationService jobCreationService,
      CallerPrincipalProvider callerPrincipalProvider,
      JobAuthorizationPolicy authorizationPolicy,
      SignalStore signalStore,
      PayloadSerializer payloadSerializer,
      MetricsCollector metricsCollector,
      Clock clock,
      RatchetOptions options,
      AfterCommitRegistrar afterCommitRegistrar) {
    super(
        eventPublisher,
        jobBatchStatusStore,
        jobPauseStore,
        jobRetryStore,
        jobTerminalStore,
        jobCrudStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        recurringJobStore,
        wakeupService,
        recurringScheduler,
        jobInvocationResolver,
        jobCreationService,
        callerPrincipalProvider,
        authorizationPolicy,
        signalStore,
        payloadSerializer,
        metricsCollector,
        clock,
        options,
        afterCommitRegistrar);
  }
}
