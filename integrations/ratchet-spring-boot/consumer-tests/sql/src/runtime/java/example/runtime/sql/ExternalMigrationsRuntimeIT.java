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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.consumer.sql.SqlDatabase;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.store.postgresql.PostgresqlSchemaMigrationDialect;

class ExternalMigrationsRuntimeIT {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"flyway", "liquibase", "sql"})
  void externalToolRunsBeforeRatchetValidationAndWorkers(String tool) throws Exception {
    try (var database = SqlDatabase.start()) {
      var arguments = migrationArguments(database, tool, false, true);
      for (int startup = 0; startup < 2; startup++) {
        var startupArguments = new ArrayList<>(List.of(arguments));
        // Plain SQL scripts have no migration ledger; provision once, then only validate.
        if (tool.equals("sql") && startup > 0) {
          startupArguments.replaceAll(
              argument ->
                  argument.startsWith("--spring.sql.init.mode=")
                      ? "--spring.sql.init.mode=never"
                      : argument);
        }
        try (var context = application(database).run(startupArguments.toArray(String[]::new))) {
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
          if (!tool.equals("sql")) {
            String ledger = tool.equals("flyway") ? "flyway_schema_history" : "databasechangelog";
            assertThat(jdbc.queryForObject("select count(*) from " + ledger, Integer.class))
                .isGreaterThan(0);
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"flyway", "liquibase", "sql"})
  void migrationFailurePreventsWorkerStartupAndReleasesRuntime(String tool) throws Exception {
    try (var database = SqlDatabase.start()) {
      var arguments = migrationArguments(database, tool, true, false);
      assertThatThrownBy(() -> application(database).run(arguments))
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

  private String[] migrationArguments(
      SqlDatabase database, String tool, boolean broken, boolean deferExternalMigrations)
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
          "drop table ratchet_schema_version;\n"
              + "create table consumer_record (id varchar(80) primary key, state varchar(40));\n"
              + "create table consumer_uuid_record (id uuid primary key, label varchar(80) not null, "
              + "occurred_at timestamp(6) with time zone not null);\n"
              + "create table external_migration_proof (value_text varchar(20));\n"
              + "insert into external_migration_proof values ('ready');\n");
    }
    var settings = new LinkedHashMap<String, String>();
    settings.put("ratchet.schema.auto-migrate", "false");
    if (deferExternalMigrations && !tool.equals("sql")) {
      // The initializer is requested before singleton creation, so these real external migrations
      // must be restored ahead of Ratchet validation and Hibernate under Boot's defer mode.
      settings.put("spring.jpa.defer-datasource-initialization", "true");
    }
    if (tool.equals("flyway")) {
      settings.put("spring.flyway.enabled", "true");
      Files.writeString(directory.resolve("V1__external.sql"), sql);
      settings.put("spring.flyway.locations", "filesystem:" + directory);
    } else {
      Path script = directory.resolve("external.sql");
      Files.writeString(script, sql);
      if (tool.equals("liquibase")) {
        settings.put("spring.liquibase.enabled", "true");
        settings.put("spring.liquibase.change-log", "file:" + script);
      } else {
        settings.put("spring.sql.init.mode", "always");
        settings.put(
            "spring.sql.init.schema-locations",
            "file:" + script + ",classpath:schema-postgresql.sql");
        settings.put("spring.sql.init.separator", ScriptUtils.EOF_STATEMENT_SEPARATOR);
      }
    }
    return settings.entrySet().stream()
        .map(e -> "--" + e.getKey() + "=" + e.getValue())
        .toArray(String[]::new);
  }

  private static SpringApplicationBuilder application(SqlDatabase database) {
    return new SpringApplicationBuilder(ConsumerApplication.class)
        .contextFactory(
            type ->
                new AnnotationConfigApplicationContext() {
                  @Override
                  protected void finishBeanFactoryInitialization(
                      ConfigurableListableBeanFactory beanFactory) {
                    // After dependency post-processing, demand validation before the JPA factory
                    // or another singleton can trigger external schema initialization first.
                    beanFactory.getBean("ratchetJpaSchemaInitializer");
                    super.finishBeanFactoryInitialization(beanFactory);
                  }
                })
        .properties(RuntimeSupport.properties(database));
  }
}
