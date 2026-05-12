package run.ratchet.store.migration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.SchedulerLifecycleHook;

/**
 * Runs {@link SchemaMigrator} during {@code beforeStart} when {@code ratchet.schema.auto-migrate}
 * is enabled.
 *
 * <p>Default behavior is OFF — Ratchet ships DDL as plain SQL files and assumes operators apply
 * them through external tooling (Flyway, Liquibase, manual scripts, container init scripts).
 * Setting {@code RATCHET_SCHEMA_AUTO_MIGRATE=true} flips on a Quartz/Spring-Batch-dev style
 * "just-works" bootstrap suitable for development, CI, and embedded deployments.
 *
 * <p>Resolution rules:
 *
 * <ul>
 *   <li>Disabled (default): logs an informational notice and returns.
 *   <li>Enabled with no {@link DataSource} CDI bean: throws {@link SchemaInitializationException}
 *       (deployment fails fast). Application must produce a {@code DataSource} via
 *       {@code @Resource} or {@code @Produces}.
 *   <li>Enabled with no explicit dialect: probes {@code DatabaseMetaData#getDatabaseProductName}
 *       through {@link SchemaMigrator#dialectFromMetadata(Connection)} — only MySQL, MariaDB, and
 *       PostgreSQL are accepted; everything else (including CockroachDB) requires {@code
 *       RATCHET_SCHEMA_MIGRATION_DIALECT}.
 * </ul>
 *
 * <p>Scope is JDBC-only by contract. MongoDB initializes its collections and indexes
 * unconditionally inside {@code MongoJobStoreImpl} because named indexes are referenced by claim
 * queries (correctness-critical, not operational).
 */
@ApplicationScoped
public class SchemaMigrationLifecycleHook implements SchedulerLifecycleHook {

  private static final Logger log = Logger.getLogger(SchemaMigrationLifecycleHook.class);

  private final RatchetOptions options;
  private final Instance<DataSource> dataSourceLookup;

  protected SchemaMigrationLifecycleHook() {
    this.options = null;
    this.dataSourceLookup = null;
  }

  @Inject
  public SchemaMigrationLifecycleHook(
      RatchetOptions options, Instance<DataSource> dataSourceLookup) {
    this.options = options;
    this.dataSourceLookup = dataSourceLookup;
  }

  @Override
  public void beforeStart() {
    RatchetOptions.SchemaOptions schemaOptions = options.schema();
    if (!schemaOptions.autoMigrate()) {
      log.info(
          "Ratchet schema auto-migration disabled (default). Manage ratchet_* and scheduler_*"
              + " tables externally, or set ratchet.schema.auto-migrate=true to enable.");
      return;
    }

    if (dataSourceLookup == null || dataSourceLookup.isUnsatisfied()) {
      throw new SchemaInitializationException(
          "ratchet.schema.auto-migrate=true but no javax.sql.DataSource CDI bean is available."
              + " Produce a DataSource via @Resource or @Produces in your application, or set"
              + " ratchet.schema.auto-migrate=false to disable auto-migration.");
    }
    if (dataSourceLookup.isAmbiguous()) {
      throw new SchemaInitializationException(
          "ratchet.schema.auto-migrate=true but multiple javax.sql.DataSource CDI beans are"
              + " available. Disambiguate the Ratchet DataSource with a CDI qualifier or expose a"
              + " single unqualified producer.");
    }

    DataSource dataSource = dataSourceLookup.get();
    String dialect = resolveDialect(dataSource, schemaOptions);
    String prefix = schemaOptions.migrationPrefix();
    log.infof(
        "Ratchet schema auto-migration enabled (dialect=%s, prefix=%s); applying pending"
            + " migrations.",
        dialect, prefix);
    try {
      SchemaMigrator.MigrationResult result =
          new SchemaMigrator(dataSource, dialect, prefix).migrate();
      log.infof(
          "Ratchet schema migration complete: applied=%d skipped=%d.",
          result.appliedCount(), result.skippedCount());
    } catch (SchemaInitializationException e) {
      throw e;
    } catch (RuntimeException | SQLException | java.io.IOException e) {
      throw new SchemaInitializationException(
          "Ratchet schema auto-migration failed: " + exceptionSummary(e), e);
    }
  }

  private static String exceptionSummary(Exception e) {
    String message = e.getMessage();
    if (message == null || message.isBlank()) {
      return e.getClass().getSimpleName();
    }
    return e.getClass().getSimpleName() + ": " + message;
  }

  private String resolveDialect(DataSource dataSource, RatchetOptions.SchemaOptions schemaOptions) {
    String configured = schemaOptions.migrationDialect();
    if (configured != null && !configured.isBlank()) {
      return configured.trim();
    }
    try (Connection connection = dataSource.getConnection()) {
      return SchemaMigrator.dialectFromMetadata(connection);
    } catch (SQLException e) {
      throw new SchemaInitializationException(
          "Could not probe DataSource metadata to resolve migration dialect; set"
              + " ratchet.schema.migration-dialect explicitly. Cause: "
              + exceptionSummary(e),
          e);
    }
  }
}
