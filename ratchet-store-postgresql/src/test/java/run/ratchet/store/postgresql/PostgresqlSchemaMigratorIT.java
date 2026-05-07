package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.tck.store.JdbcDriverDataSource;

/**
 * Runs {@link SchemaMigrator} against a virgin PostgreSQL Testcontainer (no init script). Verifies
 * that the migrator brings the schema up from empty, is idempotent on re-run, and converges under
 * {@code pg_advisory_lock} when two threads race.
 */
class PostgresqlSchemaMigratorIT {

  @SuppressWarnings({"resource", "rawtypes"})
  private static final PostgreSQLContainer CONTAINER =
      new PostgreSQLContainer("postgres:16")
          .withDatabaseName("ratchet_migrator_it")
          .withUsername("ratchet")
          .withPassword("ratchet")
          .withUrlParam("stringtype", "unspecified");

  @BeforeAll
  static void start() {
    CONTAINER.start();
  }

  @AfterAll
  static void stop() {
    CONTAINER.stop();
  }

  private static DataSource dataSource() {
    return new JdbcDriverDataSource(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  private static int countSchemaVersionRows() throws SQLException {
    try (Connection c = newJdbcConnection();
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM ratchet_schema_version")) {
      assertTrue(rs.next());
      return rs.getInt(1);
    }
  }

  private static boolean tableExists(String name) throws SQLException {
    try (Connection c = newJdbcConnection();
        ResultSet rs = c.getMetaData().getTables(null, null, name, new String[] {"TABLE"})) {
      return rs.next();
    }
  }

  private static Connection newJdbcConnection() throws SQLException {
    return DriverManager.getConnection(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  private static void resetDatabase() throws SQLException {
    try (Connection c = newJdbcConnection();
        Statement s = c.createStatement()) {
      s.execute("DROP SCHEMA public CASCADE");
      s.execute("CREATE SCHEMA public");
    }
  }

  @Test
  void migratesVirginSchemaThenIsIdempotentOnRerun() throws Exception {
    resetDatabase();

    SchemaMigrator.MigrationResult first = new SchemaMigrator(dataSource(), "postgresql").migrate();
    assertTrue(first.appliedCount() > 0, "expected at least one migration applied");
    assertEquals(0, first.skippedCount());
    assertTrue(tableExists("scheduler_job_queue"), "core table should exist after migration");

    int rowsAfterFirst = countSchemaVersionRows();
    assertEquals(first.appliedCount(), rowsAfterFirst);

    SchemaMigrator.MigrationResult second =
        new SchemaMigrator(dataSource(), "postgresql").migrate();
    assertEquals(0, second.appliedCount());
    assertEquals(rowsAfterFirst, second.skippedCount());
    assertEquals(rowsAfterFirst, countSchemaVersionRows());
  }

  @Test
  void parallelMigratorsConvergeUnderAdvisoryLock() throws Exception {
    resetDatabase();

    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch go = new CountDownLatch(1);
    Callable<SchemaMigrator.MigrationResult> task =
        () -> {
          ready.countDown();
          go.await();
          return new SchemaMigrator(dataSource(), "postgresql").migrate();
        };
    ExecutorService pool = Executors.newFixedThreadPool(2);
    List<Future<SchemaMigrator.MigrationResult>> futures = new ArrayList<>();
    try {
      futures.add(pool.submit(task));
      futures.add(pool.submit(task));
      assertTrue(ready.await(15, TimeUnit.SECONDS));
      go.countDown();
    } finally {
      pool.shutdown();
    }

    int totalApplied = 0;
    int totalSkipped = 0;
    for (Future<SchemaMigrator.MigrationResult> future : futures) {
      SchemaMigrator.MigrationResult result = future.get(120, TimeUnit.SECONDS);
      totalApplied += result.appliedCount();
      totalSkipped += result.skippedCount();
    }

    int finalRows = countSchemaVersionRows();
    assertEquals(finalRows, totalApplied, "applied total must equal final version-row count");
    assertEquals(finalRows, totalSkipped, "skipped total must equal final version-row count");
  }
}
