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
package run.ratchet.spring.boot.it.mysql;

import org.junit.jupiter.api.extension.ExtensionContext;
import run.ratchet.spring.boot.it.sharedtck.fixture.tck.SqlStoreTckBinding;

/** MySQL-specific container and property binding for the shared Spring TCK. */
public class MysqlTckBinding extends SqlStoreTckBinding {

  private final MysqlContainerExtension containerExtension = new MysqlContainerExtension();

  @Override
  public void beforeAll(ExtensionContext context) {
    containerExtension.beforeAll(context);
  }

  @Override
  public String[] mainContextProperties() {
    return new String[] {
      "spring.datasource.url=" + containerExtension.currentJdbcUrl(),
      "spring.datasource.username=" + containerExtension.currentUsername(),
      "spring.datasource.password=" + containerExtension.currentPassword(),
      "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
      "spring.datasource.hikari.transaction-isolation=TRANSACTION_READ_COMMITTED",
      "spring.jpa.hibernate.ddl-auto=none",
      "spring.jpa.open-in-view=false",
      "spring.jpa.show-sql=false",
      "ratchet.class-policy.allowed-packages=" + allowedPackagesProperty(),
      "ratchet.lifecycle.drain-timeout=PT30S",
      "logging.level.org.hibernate=WARN"
    };
  }

  @Override
  public String applicationPackage() {
    return "run.ratchet.spring.boot.it.mysql";
  }

  @Override
  protected String sqlMigrationDialect() {
    return "mysql";
  }
}
