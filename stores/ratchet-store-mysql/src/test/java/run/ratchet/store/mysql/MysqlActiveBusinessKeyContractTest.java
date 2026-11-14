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

import java.sql.SQLException;
import java.util.UUID;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractActiveBusinessKeyContract;

class MysqlActiveBusinessKeyContractTest extends AbstractActiveBusinessKeyContract {

  private final MysqlTestFixture fixture = new MysqlTestFixture();

  @Override
  protected void insertReservationWithoutOwner(
      String businessKey, UUID ownerId, String ownerTable) {
    try (var connection = fixture.openConnection()) {
      connection.setAutoCommit(false);
      // language=MySQL
      try (var insert =
          connection.prepareStatement(
              """
          INSERT INTO scheduler_business_key_reservation (business_key, owner_job_id, owner_table, reserved_at)
          VALUES (?, ?, ?, CURRENT_TIMESTAMP)
          """)) {
        insert.setString(1, businessKey);
        insert.setBytes(2, UuidByteArrayConverter.toBytes(ownerId));
        insert.setString(3, ownerTable);
        insert.executeUpdate();
      }
      connection.commit();
    } catch (SQLException e) {
      throw new IllegalStateException("Cannot insert reservation without owner", e);
    }
  }

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
}
