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
package run.ratchet.spring.boot.it.mysql.fixture.tck;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import run.ratchet.spring.boot.it.mysql.MysqlContainerExtension;

/** Supplies the shared MySQL container before Spring creates the application context. */
public final class TckApplicationContextInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    TestPropertyValues.of(
            "spring.datasource.url=" + MysqlContainerExtension.jdbcUrl(),
            "spring.datasource.username=" + MysqlContainerExtension.username(),
            "spring.datasource.password=" + MysqlContainerExtension.password(),
            "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
            "spring.datasource.hikari.transaction-isolation=TRANSACTION_READ_COMMITTED",
            "spring.jpa.hibernate.ddl-auto=none",
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
