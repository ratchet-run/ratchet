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

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import run.ratchet.store.migration.SchemaMigrationDialect;
import run.ratchet.store.migration.SchemaMigrator;

/** Installs only the shipped V001, then lets the application process perform the upgrade. */
class SqlNativeMigrationIT {
  @Test
  void upgradesReleasedSchemaAndPreservesExistingData(@TempDir Path directory) throws Exception {
    try (var database = SqlDatabase.start()) {
      var properties = database.properties();
      var dataSource =
          new DriverManagerDataSource(
              (String) properties.get("spring.datasource.url"),
              (String) properties.get("spring.datasource.username"),
              (String) properties.get("spring.datasource.password"));
      String vendor =
          switch (database.store()) {
            case "postgresql" -> "Postgresql";
            case "mysql" -> "Mysql";
            case "oracle" -> "Oracle";
            case "sqlserver" -> "Sqlserver";
            default -> throw new IllegalStateException(database.store());
          };
      var dialect =
          (SchemaMigrationDialect)
              Class.forName(
                      "run.ratchet.store."
                          + database.store()
                          + "."
                          + vendor
                          + "SchemaMigrationDialect")
                  .getConstructor()
                  .newInstance();
      var migrations = new SchemaMigrator(dataSource, dialect).discoverMigrations();
      var first = migrations.get(0);
      Path old = Files.createDirectory(directory.resolve("released"));
      String filename = first.resourceName().substring(first.resourceName().lastIndexOf('/') + 1);
      Files.writeString(old.resolve(filename), first.sql());
      Files.writeString(old.resolve("index.txt"), filename + "\n");
      ClassLoader original = Thread.currentThread().getContextClassLoader();
      try (var loader = new URLClassLoader(new URL[] {directory.toUri().toURL()}, original)) {
        Thread.currentThread().setContextClassLoader(loader);
        assertThat(new SchemaMigrator(dataSource, dialect, "released").migrate().appliedCount())
            .isEqualTo(1);
      } finally {
        Thread.currentThread().setContextClassLoader(original);
      }
      try (var connection = dataSource.getConnection();
          var statement = connection.createStatement()) {
        statement.executeUpdate(
            "create table consumer_upgrade_sentinel (id varchar(40) primary key, value_text varchar(40))");
        statement.executeUpdate(
            "insert into consumer_upgrade_sentinel (id, value_text) values ('before-upgrade', 'preserved')");
        try (var rows = statement.executeQuery("select count(*) from ratchet_schema_version")) {
          assertThat(rows.next()).isTrue();
          assertThat(rows.getInt(1)).isEqualTo(1);
        }
      }
      try (var connection = dataSource.getConnection();
          var statement = connection.createStatement()) {
        statement.executeUpdate(
            "insert into scheduler_resource_limit (resource_name, max_concurrent, description) values ('upgrade-preserved', 3, 'existing Ratchet state')");
      }
      var arguments =
          new ArrayList<>(
              run.ratchet.consumer.ConsumerProcess.command(
                  "sql-consumer", "--consumer.verify=true"));
      properties.forEach((key, value) -> arguments.add("--" + key + "=" + value));
      // A second launch verifies migration idempotence, including the existing version ledger.
      for (int run = 1; run <= 2; run++) {
        Path output = Path.of("target", "migration-" + run + ".log");
        Process process =
            new ProcessBuilder(arguments)
                .redirectErrorStream(true)
                .redirectOutput(output.toFile())
                .start();
        try {
          assertThat(process.waitFor(150, TimeUnit.SECONDS))
              .withFailMessage("%s", Files.readString(output))
              .isTrue();
          assertThat(process.exitValue()).withFailMessage("%s", Files.readString(output)).isZero();
          assertThat(Files.readString(output)).contains("RATCHET_CONSUMER_VERIFIED");
        } finally {
          process.destroyForcibly();
          process.waitFor(10, TimeUnit.SECONDS);
        }
        try (var connection = dataSource.getConnection();
            var statement = connection.createStatement()) {
          try (var rows =
              statement.executeQuery(
                  "select version, checksum from ratchet_schema_version order by version")) {
            for (var migration : migrations) {
              assertThat(rows.next()).isTrue();
              assertThat(rows.getString(1)).isEqualTo(migration.version());
              assertThat(rows.getString(2)).isEqualTo(migration.checksum());
            }
            assertThat(rows.next()).isFalse();
          }
          try (var rows =
              statement.executeQuery(
                  "select max_concurrent, description from scheduler_resource_limit where resource_name = 'upgrade-preserved'")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isEqualTo(3);
            assertThat(rows.getString(2)).isEqualTo("existing Ratchet state");
            assertThat(rows.next()).isFalse();
          }
          try (var rows =
              statement.executeQuery(
                  "select value_text from consumer_upgrade_sentinel where id = 'before-upgrade'")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString(1)).isEqualTo("preserved");
          }
        }
      }
    }
  }
}
