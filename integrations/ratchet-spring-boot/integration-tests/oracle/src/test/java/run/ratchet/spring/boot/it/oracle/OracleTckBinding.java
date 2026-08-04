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
package run.ratchet.spring.boot.it.oracle;

import org.junit.jupiter.api.extension.ExtensionContext;
import run.ratchet.spring.boot.it.sharedtck.fixture.tck.SqlStoreTckBinding;

/** Oracle-specific container and property binding for the shared Spring TCK. */
public class OracleTckBinding extends SqlStoreTckBinding {

  private final OracleContainerExtension containerExtension = new OracleContainerExtension();

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
      "spring.datasource.driver-class-name=oracle.jdbc.OracleDriver",
      "spring.jpa.hibernate.ddl-auto=none",
      "spring.jpa.properties.hibernate.jdbc.time_zone=UTC",
      "spring.jpa.properties.hibernate.type.preferred_instant_jdbc_type=TIMESTAMP",
      "spring.jpa.open-in-view=false",
      "spring.jpa.show-sql=false",
      "ratchet.class-policy.allowed-packages=" + allowedPackagesProperty(),
      "ratchet.lifecycle.drain-timeout=PT30S",
      "logging.level.org.hibernate=WARN"
    };
  }

  @Override
  public String applicationPackage() {
    return "run.ratchet.spring.boot.it.oracle";
  }

  @Override
  protected String sqlMigrationDialect() {
    return "oracle";
  }
}
