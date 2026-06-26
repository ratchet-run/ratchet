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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractResourcePermitStoreContract;

class SqlserverResourcePermitStoreContractTest extends AbstractResourcePermitStoreContract {

  private final SqlserverTestFixture fixture = new SqlserverTestFixture();

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

  @Test
  void tryAcquirePermit_usesCapacityGatedInsert() throws IOException {
    String source =
        Files.readString(
            Path.of("src/main/java/run/ratchet/store/sqlserver/SqlserverAuxiliaryOperations.java"));
    int start = source.indexOf("public boolean tryAcquirePermit");
    int end = source.indexOf("public void releasePermit");
    assertTrue(start >= 0, "tryAcquirePermit method should exist in source");
    assertTrue(end > start, "releasePermit should appear after tryAcquirePermit in source");
    String method = source.substring(start, end);

    assertTrue(
        method.contains("INSERT INTO scheduler_resource_permit")
            && method.contains(") < max_concurrent")
            && !method.contains("String countSql"),
        "tryAcquirePermit must gate permit insertion with the capacity check");
  }
}
