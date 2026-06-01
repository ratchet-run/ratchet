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
package run.ratchet.testsuite.util;

import org.jboss.shrinkwrap.api.spec.WebArchive;
import run.ratchet.testsuite.infra.JdbcDatabaseConfig;

/**
 * Strategy for configuring a datasource within a test deployment archive.
 *
 * <p>Each application server has a different mechanism for defining datasources within a deployment
 * (e.g., WildFly uses {@code -ds.xml}, GlassFish uses {@code glassfish-resources.xml}).
 */
public interface DataSourceStrategy {

  /**
   * Configures the given archive with a datasource pointing to the test database.
   *
   * @param archive the web archive to configure
   * @param config the database connection details
   */
  void configureArchive(WebArchive archive, JdbcDatabaseConfig config);

  /**
   * Returns the JTA datasource name that should be written into {@code persistence.xml}.
   *
   * @return the server-specific JTA datasource name
   */
  String jtaDataSourceName();
}
