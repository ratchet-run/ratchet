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
package run.ratchet.spring.boot.it.sqlserver;

import java.util.Locale;
import org.junit.jupiter.api.extension.ExtensionContext;
import run.ratchet.spring.boot.it.sharedtck.fixture.tck.SqlStoreTckBinding;

/** SQL Server-specific container and property binding for the shared Spring TCK. */
public class SqlserverTckBinding extends SqlStoreTckBinding {

  private final SqlserverContainerExtension containerExtension = new SqlserverContainerExtension();

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
      "spring.datasource.driver-class-name=com.microsoft.sqlserver.jdbc.SQLServerDriver",
      "spring.jpa.hibernate.ddl-auto=none",
      "spring.jpa.properties.hibernate.jdbc.time_zone=UTC",
      "spring.jpa.properties.hibernate.type.preferred_instant_jdbc_type=TIMESTAMP",
      "spring.jpa.properties.hibernate.type.preferred_uuid_jdbc_type=BINARY",
      "spring.jpa.open-in-view=false",
      "spring.jpa.show-sql=false",
      "ratchet.class-policy.allowed-packages=" + allowedPackagesProperty(),
      "ratchet.lifecycle.drain-timeout=PT30S",
      "logging.level.org.hibernate=WARN"
    };
  }

  @Override
  public String applicationPackage() {
    return "run.ratchet.spring.boot.it.sqlserver";
  }

  @Override
  protected String sqlMigrationDialect() {
    return "sqlserver";
  }

  @Override
  public int storeClearAttempts() {
    return 5;
  }

  @Override
  public boolean isRetryableStoreClearFailure(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      String message = current.getMessage();
      if (message != null && message.toLowerCase(Locale.ROOT).contains("deadlock")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  @Override
  public long storeClearRetryBackoffMillis() {
    return 200L;
  }
}
