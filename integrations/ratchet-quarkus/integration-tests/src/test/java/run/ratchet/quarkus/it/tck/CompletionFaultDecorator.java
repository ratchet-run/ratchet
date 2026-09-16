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

import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.dto.JobCompletionResult;
import run.ratchet.store.spi.JobTerminalStore;

/** Delegates to the real SQL/Mongo transaction, including the deliberate invalid batch mutation. */
@IfBuildProperty(name = "ratchet.it.completion-faults", stringValue = "true")
@Decorator
@Priority(1)
public abstract class CompletionFaultDecorator implements JobTerminalStore {
  @Inject @Delegate @Any JobTerminalStore delegate;
  @Inject CompletionFaults faults;

  @Override
  public JobCompletionResult commitCompletion(JobCompletionPlan plan) {
    // The prior call and its outer engine transaction have unwound before this retry begins.
    if (faults.pauseNextRetry.compareAndSet(true, false)) {
      faults.rolledBack.countDown();
      try {
        if (!faults.resume.await(20, TimeUnit.SECONDS))
          throw new AssertionError("test did not release completion retry");
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError(interrupted);
      }
    }
    if (plan.completedBatch() != null && faults.rejectBatchParents.get()) {
      faults.rejectedParents.incrementAndGet();
      throw new RatchetTransientStoreException("injected parent completion outage");
    }
    if (!plan.dependencies().isEmpty()
        && faults.corruptNextChainCompletion.compareAndSet(true, false)) {
      faults.failedPlan = plan;
      JobCompletionPlan invalid =
          new JobCompletionPlan(
              plan.jobId(),
              plan.expectedStatus(),
              plan.terminalStatus(),
              plan.resultJson(),
              plan.resultType(),
              plan.errorMessage(),
              plan.attempts(),
              plan.start(),
              plan.end(),
              plan.durationMs(),
              plan.queueWaitMs(),
              UUID.randomUUID(),
              null,
              plan.dependencies());
      try {
        delegate.commitCompletion(invalid);
        throw new AssertionError("Missing batch must reject completion");
      } catch (RuntimeException expected) {
        faults.pauseNextRetry.set(true);
        throw new RatchetTransientStoreException(
            "injected completion transaction failure", expected);
      }
    }
    return delegate.commitCompletion(plan);
  }
}
