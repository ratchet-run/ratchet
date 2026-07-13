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

import org.jboss.shrinkwrap.api.spec.EnterpriseArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import run.ratchet.testsuite.infra.JdbcDatabaseConfig;

/**
 * Payara-specific datasource configuration strategy.
 *
 * <p>Payara supports application-scoped JDBC resources via {@code WEB-INF/glassfish-resources.xml},
 * so the datasource can be created with the Testcontainers JDBC URL captured during deployment
 * assembly.
 */
public class PayaraDataSourceStrategy implements DataSourceStrategy {

  private static final String DOCTYPE =
      """
      <!DOCTYPE resources PUBLIC "-//Payara.fish//DTD Payara Server 4 Resource Definitions//EN" \
      "http://docs.payara.fish/schemas/payara-resources_1_8.dtd">\
      """;

  @Override
  public void configureArchive(WebArchive archive, JdbcDatabaseConfig config) {
    GlassFishResources.addResources(archive, config, DOCTYPE);
  }

  @Override
  public void configureEnterpriseArchive(EnterpriseArchive ear, JdbcDatabaseConfig config) {
    GlassFishResources.addEarResources(ear, config, DOCTYPE);
  }

  @Override
  public String jtaDataSourceName() {
    return GlassFishResources.JTA_DATASOURCE;
  }
}
