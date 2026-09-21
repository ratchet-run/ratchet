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
package example.runtime.sql;

import static org.assertj.core.api.Assertions.*;

import example.ratchet.ConsumerApplication;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.store.postgresql.PostgresqlSchemaMigrationDialect;

class ExternalMigrationsRuntimeIT {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"flyway", "liquibase"})
  void externalToolRunsBeforeRatchetValidationAndWorkers(String tool) throws Exception {
    try (var database = SqlDatabase.start()) {
      var arguments = migrationArguments(database, tool, false);
      for (int startup = 0; startup < 2; startup++) {
        try (var context =
            new SpringApplicationBuilder(ConsumerApplication.class)
                .properties(RuntimeSupport.properties(database))
                .run(arguments)) {
          var jdbc = context.getBean(JdbcTemplate.class);
          assertThat(
                  jdbc.queryForObject(
                      "select value_text from external_migration_proof", String.class))
              .isEqualTo("ready");
          assertThat(
                  jdbc.queryForObject(
                      "select count(*) from information_schema.tables where table_name = 'ratchet_schema_version'",
                      Integer.class))
              .isZero();
          RuntimeSupport.status(
              context,
              context.getBean(JobSchedulerService.class).enqueue(RuntimeSupport::noop).submit(),
              JobStatus.SUCCEEDED);
          String ledger = tool.equals("flyway") ? "flyway_schema_history" : "databasechangelog";
          assertThat(jdbc.queryForObject("select count(*) from " + ledger, Integer.class))
              .isGreaterThan(0);
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"flyway", "liquibase"})
  void migrationFailurePreventsWorkerStartupAndReleasesRuntime(String tool) throws Exception {
    try (var database = SqlDatabase.start()) {
      var arguments = migrationArguments(database, tool, true);
      assertThatThrownBy(
              () ->
                  new SpringApplicationBuilder(ConsumerApplication.class)
                      .properties(RuntimeSupport.properties(database))
                      .run(arguments))
          .hasStackTraceContaining("runtime_intentional_migration_failure");
      var jdbc = new JdbcTemplate(RuntimeSupport.dataSource(database));
      assertThat(
              jdbc.queryForObject(
                  "select count(*) from information_schema.tables where table_name in ('scheduler_node', 'scheduler_job')",
                  Integer.class))
          .isZero();
      try (var recovered = RuntimeSupport.start(database)) {
        RuntimeSupport.status(
            recovered,
            recovered.getBean(JobSchedulerService.class).enqueue(RuntimeSupport::noop).submit(),
            JobStatus.SUCCEEDED);
      }
    }
  }

  private String[] migrationArguments(SqlDatabase database, String tool, boolean broken)
      throws Exception {
    var migrations =
        new SchemaMigrator(
                RuntimeSupport.dataSource(database), new PostgresqlSchemaMigrationDialect())
            .discoverMigrations();
    StringBuilder sql = new StringBuilder();
    if (tool.equals("liquibase"))
      sql.append("--liquibase formatted sql\n--changeset consumer:1 splitStatements:false\n");
    if (broken) sql.append("select * from runtime_intentional_migration_failure;\n");
    else {
      for (var migration : migrations) sql.append(migration.sql()).append("\n");
      sql.append(
          "drop table ratchet_schema_version;\ncreate table external_migration_proof (value_text varchar(20));\ninsert into external_migration_proof values ('ready');\n");
    }
    var settings = new LinkedHashMap<String, String>();
    settings.put("ratchet.schema.auto-migrate", "false");
    settings.put("spring." + tool + ".enabled", "true");
    if (tool.equals("flyway")) {
      Files.writeString(directory.resolve("V1__external.sql"), sql);
      settings.put("spring.flyway.locations", "filesystem:" + directory);
    } else {
      Path script = directory.resolve("external.sql");
      Files.writeString(script, sql);
      settings.put("spring.liquibase.change-log", "file:" + script);
    }
    return settings.entrySet().stream()
        .map(e -> "--" + e.getKey() + "=" + e.getValue())
        .toArray(String[]::new);
  }
}
