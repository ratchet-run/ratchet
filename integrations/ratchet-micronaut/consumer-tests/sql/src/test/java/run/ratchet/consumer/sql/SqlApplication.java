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

import io.micronaut.context.ApplicationContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/** Ordinary consumer bootstrap; the host owns provisioning of its own tables. */
public final class SqlApplication {
  private SqlApplication() {}

  public static ApplicationContext start(SqlDatabase database, String... overrides) {
    provision(database);
    Map<String, Object> properties = new HashMap<>(database.properties());
    for (String override : overrides) {
      String[] pair = override.split("=", 2);
      properties.put(pair[0], pair[1]);
    }
    ApplicationContext context =
        ApplicationContext.builder().environments(database.store()).properties(properties).build();
    try {
      return context.start();
    } catch (RuntimeException failure) {
      try {
        context.close();
      } catch (RuntimeException cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  public static void provision(SqlDatabase database) {
    String resource = "/schema-" + database.store() + ".sql";
    try (var input = SqlApplication.class.getResourceAsStream(resource)) {
      if (input == null) throw new IllegalStateException("Missing " + resource);
      String script = new String(input.readAllBytes(), StandardCharsets.UTF_8);
      String separator = database.store().equals("oracle") ? "(?m)^/\\s*$" : ";";
      try (var connection = connection(database);
          var statement = connection.createStatement()) {
        for (String sql : script.split(separator))
          if (!sql.isBlank()) statement.execute(sql.trim());
      }
    } catch (IOException | SQLException failure) {
      throw new IllegalStateException(resource, failure);
    }
  }

  public static Connection connection(SqlDatabase database) throws SQLException {
    var properties = database.properties();
    return DriverManager.getConnection(
        (String) properties.get("datasources.default.url"),
        (String) properties.get("datasources.default.username"),
        (String) properties.get("datasources.default.password"));
  }

  public static void reset(SqlDatabase database) {
    try (var connection = connection(database);
        var statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      for (String table : TABLES) statement.executeUpdate("DELETE FROM " + table);
      connection.commit();
    } catch (SQLException failure) {
      throw new IllegalStateException("Reset consumer database", failure);
    }
  }

  private static final String[] TABLES = {
    "scheduler_idempotency_key",
    "scheduler_business_key_reservation",
    "scheduler_job_queue",
    "scheduler_job_tag",
    "scheduler_job_log",
    "scheduler_job_execution",
    "scheduler_resource_permit",
    "scheduler_workflow_condition",
    "scheduler_batch_metrics",
    "scheduler_job_archive",
    "scheduler_job_properties",
    "scheduler_job_extension_state",
    "scheduler_job",
    "scheduler_recurring_job_archive",
    "scheduler_recurring_job",
    "scheduler_batch",
    "scheduler_resource_limit",
    "scheduler_lock",
    "scheduler_node"
  };
}
