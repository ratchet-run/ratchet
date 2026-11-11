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
package run.ratchet.store.mysql;

import java.util.UUID;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractEventOutboxStoreContract;

class MysqlEventOutboxStoreContractTest extends AbstractEventOutboxStoreContract {

  private final Fixture fixture = new Fixture();

  @Override
  public JobStore store() {
    return fixture.store();
  }

  @Override
  public JobEntity newPendingJob() {
    return fixture.newPendingJob();
  }

  @Override
  public JobEntity newBatchParentJob() {
    return fixture.newBatchParentJob();
  }

  @Override
  public void cleanupStore() {
    fixture.cleanupStore();
  }

  @Override
  protected void corruptOutboxEventType(UUID eventId) {
    fixture.corruptOutboxEventType(eventId);
  }

  private static final class Fixture extends MysqlTestFixture {
    void corruptOutboxEventType(UUID eventId) {
      String id = "UNHEX('" + eventId.toString().replace("-", "") + "')";
      executeNativeSql(
          "UPDATE scheduler_event_outbox SET event_type = 'UNKNOWN_EVENT' WHERE event_id = " + id);
    }
  }
}
