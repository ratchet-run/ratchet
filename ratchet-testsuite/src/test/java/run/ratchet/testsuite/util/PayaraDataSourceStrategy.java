package run.ratchet.testsuite.util;

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
  public String jtaDataSourceName() {
    return GlassFishResources.JTA_DATASOURCE;
  }
}
