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
package run.ratchet.consumer.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.consumer.ProcessScenarios;

class SqlRecoveryIT {
  @Test
  void verifiesApplicationProcess() throws Exception {
    try (var database = SqlDatabase.start()) {
      SqlApplication.provision(database);
      var options = ProcessScenarios.options(database.properties());
      options.put("micronaut.environments", database.store());
      String id = ProcessScenarios.recovery("sql-consumer", options);
      try (var connection = SqlApplication.connection(database);
          var statement =
              connection.prepareStatement(
                  "select terminal_status from scheduler_job where " + database.jobIdPredicate())) {
        statement.setObject(1, database.queryId(UUID.fromString(id)));
        try (var rows = statement.executeQuery()) {
          assertThat(rows.next()).isTrue();
          assertThat(rows.getString(1)).isEqualTo("SUCCEEDED");
          assertThat(rows.next()).isFalse();
        }
      }
    }
  }
}
