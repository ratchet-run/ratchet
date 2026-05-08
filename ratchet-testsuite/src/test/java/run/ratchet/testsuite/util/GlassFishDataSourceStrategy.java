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
