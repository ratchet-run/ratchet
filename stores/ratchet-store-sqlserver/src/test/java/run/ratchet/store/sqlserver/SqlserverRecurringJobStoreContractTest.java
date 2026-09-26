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
package run.ratchet.store.sqlserver;

import java.util.List;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.RecurringExecutionPlan;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.tck.store.AbstractRecurringJobStoreContract;
import run.ratchet.tck.store.JobStoreContractFixture;

class SqlserverRecurringJobStoreContractTest extends AbstractRecurringJobStoreContract {

  private final SqlserverTestFixture fixture = new SqlserverTestFixture();

  @Override
  protected RecurringJobStore recurringStore() {
    return (RecurringJobStore) fixture.store();
  }

  @Override
  protected JobStoreContractFixture jobFixture() {
    return fixture;
  }

  @Override
  protected TagStore tagStore() {
    return (TagStore) fixture.store();
  }

  @Override
  protected JobPayload noopPayload() {
    return new JobPayload("run.ratchet.tck.store.NoopTask", "run", "()V", true, List.of());
  }

  @Override
  protected void cleanupRecurringStore() {
    fixture.cleanupStore();
  }

  @Override
  protected void commitRecurringPlansWithLaterFailure(List<RecurringExecutionPlan> plans) {
    var second = plans.get(1);
    // SQL inserts children before advancing masters. Make those inserts valid, then fail
    // at the second master's archive statement after the first master's UPDATE completed.
    var validPlans =
        List.of(
            plans.get(0), new RecurringExecutionPlan(second.claim(), List.of(), second.nextFire()));
    fixture.failBeforeRecurringArchiveAfterAdvance(
        () -> recurringStore().commitRecurringExecutions(validPlans));
  }
}
