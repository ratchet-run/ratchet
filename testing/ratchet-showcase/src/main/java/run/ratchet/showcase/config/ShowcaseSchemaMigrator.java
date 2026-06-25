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
package run.ratchet.showcase.config;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Locale;
import java.util.logging.Logger;
import javax.sql.DataSource;
import run.ratchet.store.migration.SchemaMigrationDialect;
import run.ratchet.store.migration.SchemaMigrator;

public final class ShowcaseSchemaMigrator {

  private ShowcaseSchemaMigrator() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      throw new IllegalArgumentException(
          "Usage: ShowcaseSchemaMigrator <postgresql|mysql> <jdbcUrl> <user> <password>");
    }

    String dialect = args[0].toLowerCase(Locale.ROOT);
    String jdbcUrl = args[1];
    String user = args[2];
    String password = args[3];

    SchemaMigrationDialect migrationDialect = dialectFor(dialect);
    SchemaMigrator.MigrationResult result =
        new SchemaMigrator(new DriverManagerDataSource(jdbcUrl, user, password), migrationDialect)
            .migrate();
    System.out.printf(
        "Ratchet schema migration complete: applied=%d skipped=%d%n",
        result.appliedCount(), result.skippedCount());
  }

  private static SchemaMigrationDialect dialectFor(String dialect)
      throws ReflectiveOperationException {
    // The active Maven profile bundles exactly one SQL store, so resolve its dialect reflectively
    // rather than statically referencing a store module that may not be on the classpath.
    String dialectClass =
        switch (dialect) {
          case "postgresql" -> {
            Class.forName("org.postgresql.Driver");
            yield "run.ratchet.store.postgresql.PostgresqlSchemaMigrationDialect";
          }
          case "mysql" -> {
            Class.forName("com.mysql.cj.jdbc.Driver");
            yield "run.ratchet.store.mysql.MysqlSchemaMigrationDialect";
          }
          default ->
              throw new IllegalArgumentException("Unsupported SQL showcase database: " + dialect);
        };
    return (SchemaMigrationDialect)
        Class.forName(dialectClass).getDeclaredConstructor().newInstance();
  }

  private static final class DriverManagerDataSource implements DataSource {

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private PrintWriter logWriter;
    private int loginTimeout;

    private DriverManagerDataSource(String jdbcUrl, String user, String password) {
      this.jdbcUrl = jdbcUrl;
      this.user = user;
      this.password = password;
    }

    @Override
    public Connection getConnection() throws SQLException {
      return DriverManager.getConnection(jdbcUrl, user, password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return DriverManager.getConnection(jdbcUrl, username, password);
    }

    @Override
    public PrintWriter getLogWriter() {
      return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
      this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) {
      this.loginTimeout = seconds;
      DriverManager.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() {
      return loginTimeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      if (iface.isInstance(this)) {
        return iface.cast(this);
      }
      throw new SQLException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return iface.isInstance(this);
    }
  }
}
