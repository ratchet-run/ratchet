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
import jakarta.transaction.Transactional;
import java.time.Clock;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.ri.core.BatchCompletionTransaction;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;

/**
 * Micronaut proxy carrier for {@link BatchService}; see {@link RatchetTransactionBoundary} and the
 * package documentation for why.
 */
@Transactional
public class MicronautBatchService extends BatchService {
  @Creator
  public MicronautBatchService() {
    super();
  }

  public MicronautBatchService(
      BatchStore batchStore,
      JobCrudStore jobCrudStore,
      JobBatchStatusStore jobBatchStatusStore,
      JobTerminalStore jobTerminalStore,
      MetricsCollector metricsCollector,
      InternalEventPublisher eventPublisher,
      DeadLetterService deadLetterService,
      WorkflowScheduler workflowScheduler,
      ClassPolicy classPolicy,
      BeanResolver beanResolver,
      Clock clock,
      AfterCommitRegistrar afterCommitRegistrar,
      BatchCompletionTransaction batchCompletionTransaction,
      PayloadSerializer payloadSerializer) {
    super(
        batchStore,
        jobCrudStore,
        jobBatchStatusStore,
        jobTerminalStore,
        metricsCollector,
        eventPublisher,
        deadLetterService,
        workflowScheduler,
        classPolicy,
        beanResolver,
        clock,
        afterCommitRegistrar,
        batchCompletionTransaction,
        payloadSerializer);
  }
}
