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
import jakarta.transaction.Transactional.TxType;
import java.time.Instant;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.ri.core.BatchCompletionTransaction;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.TransactionalCompletion;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.DurableTerminalEvents;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobTerminalStore;

/**
 * Micronaut proxy carrier for {@link PostExecutionHandler}; see {@link RatchetTransactionBoundary}
 * and the package documentation for why.
 */
@Transactional(value = TxType.REQUIRES_NEW, rollbackOn = Exception.class)
public class MicronautPostExecutionHandler extends PostExecutionHandler {
  @Creator
  public MicronautPostExecutionHandler() {
    super();
  }

  public MicronautPostExecutionHandler(
      BatchService batchService,
      WorkflowScheduler workflowScheduler,
      DeadLetterService deadLetterService,
      PollerScheduler pollerScheduler,
      JobTerminalStore jobTerminalStore,
      AfterCommitRegistrar afterCommitRegistrar,
      BatchCompletionTransaction completionTransaction,
      DurableTerminalEvents durableEvents) {
    super(
        batchService,
        workflowScheduler,
        deadLetterService,
        pollerScheduler,
        jobTerminalStore,
        afterCommitRegistrar,
        completionTransaction,
        durableEvents);
  }

  @Override
  @Transactional(value = TxType.MANDATORY, rollbackOn = Exception.class)
  public TransactionalCompletion completeSuccessInCurrentTransaction(
      JobEntity source,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      long durationMs,
      long queueWaitMs) {
    return super.completeSuccessInCurrentTransaction(
        source, resultJson, resultType, start, end, durationMs, queueWaitMs);
  }

  @Override
  @Transactional(TxType.NOT_SUPPORTED)
  public void runFollowups(TransactionalCompletion completion) {
    super.runFollowups(completion);
  }
}
