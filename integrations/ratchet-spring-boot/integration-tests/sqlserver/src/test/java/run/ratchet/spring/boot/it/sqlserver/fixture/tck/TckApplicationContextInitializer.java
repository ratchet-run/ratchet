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
package run.ratchet.spring.boot.it.sqlserver.fixture.tck;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import run.ratchet.spring.boot.it.sqlserver.SqlserverContainerExtension;

/** Supplies the shared SQL Server container before Spring creates the application context. */
public final class TckApplicationContextInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    TestPropertyValues.of(
            "spring.datasource.url=" + SqlserverContainerExtension.jdbcUrl(),
            "spring.datasource.username=" + SqlserverContainerExtension.username(),
            "spring.datasource.password=" + SqlserverContainerExtension.password(),
            "spring.datasource.driver-class-name=com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "spring.jpa.hibernate.ddl-auto=none",
            "spring.jpa.properties.hibernate.jdbc.time_zone=UTC",
            "spring.jpa.properties.hibernate.type.preferred_instant_jdbc_type=TIMESTAMP",
            "spring.jpa.properties.hibernate.type.preferred_uuid_jdbc_type=BINARY",
            "spring.jpa.open-in-view=false",
            "spring.jpa.show-sql=false",
            "ratchet.class-policy.allowed-packages="
                + TckConfiguration.APPLICATION_PACKAGE
                + ","
                + TckConfiguration.TCK_PACKAGE,
            "ratchet.lifecycle.drain-timeout=PT30S",
            "logging.level.org.hibernate=WARN")
        .applyTo(applicationContext.getEnvironment());
  }
}
