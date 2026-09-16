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
package run.ratchet.store.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Exercises the new-table and repeat-application paths with a pre-upgrade job. */
class OracleIdempotencyMigrationTest {
  @Test
  void backfillsExistingJobAndKeepsItsOriginalOwnerOnRepeat() throws Exception {
    var fixture = new OracleTestFixture();
    try {
      fixture.cleanupStore();
      var job = fixture.store().create(fixture.newPendingJob());
      String migration;
      try (var input =
          getClass().getResourceAsStream("/ddl/migrations/V007__permanent_idempotency_keys.sql")) {
        migration =
            new String(
                java.util.Objects.requireNonNull(input).readAllBytes(), StandardCharsets.UTF_8);
      }
      try (var connection = fixture.openConnection();
          var statement = connection.createStatement()) {
        statement.execute("DROP TABLE scheduler_idempotency_key");
        statement.execute(migration);
        statement.execute(migration);
      }
      assertEquals(
          java.util.Optional.of(job.getId()),
          fixture.store().findOriginalJobIdByIdempotencyKey(job.getIdempotencyKey()));
      fixture.store().delete(job.getId());
      assertEquals(
          java.util.Optional.of(job.getId()),
          fixture.store().findOriginalJobIdByIdempotencyKey(job.getIdempotencyKey()));
    } finally {
      fixture.cleanupStore();
    }
  }
}
