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
 * WildFly-specific datasource configuration strategy.
 *
 * <p>The datasource and JDBC driver are configured in standalone.xml via system property
 * expressions. The JDBC driver is installed as a WildFly module by the Maven build. This class is a
 * no-op because the datasource is managed at the server level, not within the deployment.
 */
public class WildflyDataSourceStrategy implements DataSourceStrategy {

  @Override
  public void configureArchive(WebArchive archive, JdbcDatabaseConfig config) {
    // No-op: datasource is configured in standalone.xml with system property expressions.
    // The JDBC driver module and datasource are installed by the Maven build
    // (see wildfly-managed profile in pom.xml).
  }

  @Override
  public String jtaDataSourceName() {
    return "java:jboss/datasources/RatchetDS";
  }
}
