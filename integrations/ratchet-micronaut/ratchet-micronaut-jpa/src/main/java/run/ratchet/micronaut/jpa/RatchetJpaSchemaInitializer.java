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
package run.ratchet.micronaut.jpa;

import java.io.IOException;
import java.sql.SQLException;
import javax.sql.DataSource;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.store.migration.SchemaMigrator;

/** Prepares schema before Hibernate validation and before the store becomes available. */
public final class RatchetJpaSchemaInitializer implements SchedulerLifecycleHook {
  private final DataSource dataSource;
  private final SqlStoreVendor vendor;
  private final RatchetOptions options;
  private boolean initialized;

  RatchetJpaSchemaInitializer(
      DataSource dataSource, SqlStoreVendor vendor, RatchetOptions options) {
    this.dataSource = dataSource;
    this.vendor = vendor;
    this.options = options;
  }

  public synchronized void initialize() {
    if (initialized) return;
    try {
      SchemaMigrator migrator =
          new SchemaMigrator(
              dataSource, vendor.migrationDialect(), options.schema().migrationPrefix());
      if (options.schema().autoMigrate()) migrator.migrate();
      else migrator.validate();
      initialized = true;
    } catch (IOException | SQLException failure) {
      throw new IllegalStateException(
          "Ratchet schema "
              + (options.schema().autoMigrate() ? "migration" : "validation")
              + " failed",
          failure);
    }
  }

  @Override
  public void beforeStart() {
    initialize();
  }
}
