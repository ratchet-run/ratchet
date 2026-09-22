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
package run.ratchet.spring.boot.autoconfigure.jpa;

import java.io.IOException;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import run.ratchet.api.RatchetOptions;
import run.ratchet.store.migration.SchemaMigrator;

/** Applies or verifies the selected SQL schema before the entity-manager factory is created. */
@DependsOnDatabaseInitialization
final class RatchetJpaSchemaInitializer implements InitializingBean {

  private final DataSource dataSource;
  private final SqlStoreVendor vendor;
  private final RatchetOptions options;

  RatchetJpaSchemaInitializer(
      DataSource dataSource, SqlStoreVendor vendor, RatchetOptions options) {
    this.dataSource = dataSource;
    this.vendor = vendor;
    this.options = options;
  }

  @Override
  public void afterPropertiesSet() {
    try {
      SchemaMigrator migrator =
          new SchemaMigrator(
              dataSource, vendor.migrationDialect(), options.schema().migrationPrefix());
      if (options.schema().autoMigrate()) {
        migrator.migrate();
      } else {
        migrator.validate();
      }
    } catch (IOException | SQLException e) {
      throw new IllegalStateException("Ratchet schema " + action() + " failed", e);
    }
  }

  private String action() {
    return options.schema().autoMigrate() ? "migration" : "validation";
  }
}
