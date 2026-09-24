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
package run.ratchet.quarkus.it.tck;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import java.util.concurrent.atomic.AtomicReference;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.store.dto.BatchProgress;

/** Runs the production completion event publisher inside a real container transaction. */
@ApplicationScoped
public class CompletionEventBoundaryProbe {
  @Inject WorkflowScheduler workflow;
  @Inject TransactionSynchronizationRegistry transactions;

  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public void emit(Object event, Runnable beforeCommit, boolean rollback) {
    workflow.publishTerminalEvent(event);
    beforeCommit.run();
    if (rollback) throw new IllegalStateException("injected outer transaction rollback");
  }

  /**
   * Invokes the CDI-proxied completion handler from a real transaction's after-completion phase.
   */
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public AtomicReference<Throwable> invokeAfterCompletion(
      BatchService batchService, BatchProgress progress) {
    AtomicReference<Throwable> observed = new AtomicReference<>();
    transactions.registerInterposedSynchronization(
        new Synchronization() {
          @Override
          public void beforeCompletion() {}

          @Override
          public void afterCompletion(int status) {
            try {
              batchService.afterChildCompletion(progress);
            } catch (Throwable failure) {
              observed.set(failure);
            }
          }
        });
    return observed;
  }
}
