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
package run.ratchet.store.migration;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
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
 * <p>The dialect is supplied by the deployed store, never inferred by this hook: each SQL store
 * publishes a {@link SchemaMigrationDialect} CDI bean, and the hook consumes whatever is present.
 *
 * <p>Resolution rules:
 *
 * <ul>
 *   <li>Disabled (default): logs an informational notice and returns.
 *   <li>Enabled with no {@link DataSource} CDI bean: throws {@link SchemaInitializationException}
 *       (deployment fails fast). Application must produce a {@code DataSource} via
 *       {@code @Resource} or {@code @Produces}.
 *   <li>Enabled with no {@link SchemaMigrationDialect} bean: throws — the deployed store does not
 *       support managed migration.
 *   <li>Enabled with exactly one dialect bean: uses it.
 *   <li>Enabled with several dialect beans: requires {@code RATCHET_SCHEMA_MIGRATION_DIALECT} to
 *       select one by {@link SchemaMigrationDialect#id() id}.
 * </ul>
 *
 * <p>Scope is JDBC-only by contract. MongoDB initializes its collections and indexes
 * unconditionally inside {@code MongoJobStoreImpl} because named indexes are referenced by claim
 * queries (correctness-critical, not operational), and ships no {@link SchemaMigrationDialect}.
 */
@ApplicationScoped
@Priority(Interceptor.Priority.LIBRARY_BEFORE)
public class SchemaMigrationLifecycleHook implements SchedulerLifecycleHook {

  private static final Logger log = Logger.getLogger(SchemaMigrationLifecycleHook.class);

  private final RatchetOptions options;
  private final Instance<DataSource> dataSourceLookup;
  private final Instance<SchemaMigrationDialect> dialectLookup;

  protected SchemaMigrationLifecycleHook() {
    this.options = null;
    this.dataSourceLookup = null;
    this.dialectLookup = null;
  }

  @Inject
  public SchemaMigrationLifecycleHook(
      RatchetOptions options,
      Instance<DataSource> dataSourceLookup,
      Instance<SchemaMigrationDialect> dialectLookup) {
    this.options = options;
    this.dataSourceLookup = dataSourceLookup;
    this.dialectLookup = dialectLookup;
  }

  @Override
  public void beforeStart() {
    RatchetOptions.SchemaOptions schemaOptions =
        Objects.requireNonNull(options, "options must be injected").schema();
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
    SchemaMigrationDialect dialect = resolveDialect(schemaOptions);
    String prefix = schemaOptions.migrationPrefix();
    log.infof(
        "Ratchet schema auto-migration enabled (dialect=%s, prefix=%s); applying pending"
            + " migrations.",
        dialect.id(), prefix);
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

  /**
   * Selects the {@link SchemaMigrationDialect} for the deployed store. The store declares it; the
   * hook never infers a dialect from the JDBC product name.
   */
  private SchemaMigrationDialect resolveDialect(RatchetOptions.SchemaOptions schemaOptions) {
    if (dialectLookup == null || dialectLookup.isUnsatisfied()) {
      throw new SchemaInitializationException(
          "ratchet.schema.auto-migrate=true but the deployed store provides no"
              + " SchemaMigrationDialect. Managed migration is JDBC-only and requires a SQL store"
              + " module (MySQL, PostgreSQL, Oracle, or SQL Server). Apply the bundled DDL externally and set"
              + " ratchet.schema.auto-migrate=false if the deployed store does not support managed"
              + " migration.");
    }

    String configured = schemaOptions.migrationDialect();
    if (configured != null && !configured.isBlank()) {
      String wanted = configured.trim().toLowerCase(Locale.ROOT);
      for (SchemaMigrationDialect candidate : dialectLookup) {
        if (candidate.id().equals(wanted)) {
          return candidate;
        }
      }
      throw new SchemaInitializationException(
          "ratchet.schema.migration-dialect="
              + configured
              + " but no deployed SchemaMigrationDialect advertises that id. Available: "
              + availableIds()
              + ".");
    }

    if (dialectLookup.isAmbiguous()) {
      throw new SchemaInitializationException(
          "Multiple SchemaMigrationDialect beans are available ("
              + availableIds()
              + "); set ratchet.schema.migration-dialect (RATCHET_SCHEMA_MIGRATION_DIALECT) to"
              + " select one.");
    }

    return dialectLookup.get();
  }

  private String availableIds() {
    List<String> ids = new ArrayList<>();
    for (SchemaMigrationDialect candidate : dialectLookup) {
      ids.add(candidate.id());
    }
    Collections.sort(ids);
    return String.join(", ", ids);
  }
}
