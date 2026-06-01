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
 * Eclipse GlassFish 8 datasource configuration strategy.
 *
 * <p>Identical structure to {@link PayaraDataSourceStrategy} but uses the upstream GlassFish
 * resources DTD (Payara forks it under {@code docs.payara.fish}). GlassFish 8 rejects deployments
 * whose {@code glassfish-resources.xml} declares the Payara DTD, so we cannot reuse the Payara
 * strategy verbatim.
 */
public class GlassFishDataSourceStrategy implements DataSourceStrategy {

  private static final String DOCTYPE =
      """
      <!DOCTYPE resources PUBLIC \
      "-//GlassFish.org//DTD GlassFish Application Server 3.1 Resource Definitions//EN" \
      "http://glassfish.org/dtds/glassfish-resources_1_5.dtd">\
      """;

  @Override
  public void configureArchive(WebArchive archive, JdbcDatabaseConfig config) {
    GlassFishResources.addResources(archive, config, DOCTYPE);
  }

  @Override
  public String jtaDataSourceName() {
    return GlassFishResources.JTA_DATASOURCE;
  }
}
