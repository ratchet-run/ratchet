package run.ratchet.testsuite.util;

import run.ratchet.testsuite.infra.JdbcDatabaseConfig;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;

/**
 * Payara-specific datasource configuration strategy.
 *
 * <p>Payara supports application-scoped JDBC resources via {@code WEB-INF/glassfish-resources.xml},
 * so the datasource can be created with the Testcontainers JDBC URL captured during deployment
 * assembly.
 */
public class PayaraDataSourceStrategy implements DataSourceStrategy {

  private static final String JTA_DATASOURCE = "java:app/jdbc/RatchetDS";

  @Override
  public void configureArchive(WebArchive archive, JdbcDatabaseConfig config) {
    archive.addAsWebInfResource(
        new StringAsset(glassfishResourcesXml(config)), "glassfish-resources.xml");
    archive.addAsLibraries(
        Maven.configureResolver()
            .loadPomFromFile("pom.xml", config.dbType())
            .resolve(driverCoordinates(config.dbType()))
            .withTransitivity()
            .asFile());
  }

  @Override
  public String jtaDataSourceName() {
    return JTA_DATASOURCE;
  }

  private static String glassfishResourcesXml(JdbcDatabaseConfig config) {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<!DOCTYPE resources PUBLIC \"-//Payara.fish//DTD Payara Server 4 Resource Definitions//EN\" "
        + "\"http://docs.payara.fish/schemas/payara-resources_1_8.dtd\">\n"
        + "<resources>\n"
        + "  <jdbc-connection-pool name=\"RatchetPool\"\n"
        + "                        res-type=\"javax.sql.DataSource\"\n"
        + "                        datasource-classname=\""
        + dataSourceClassName(config.dbType())
        + "\"\n"
        + "                        transaction-isolation-level=\"read-committed\"\n"
        + "                        is-isolation-level-guaranteed=\"true\">\n"
        + "    <property name=\"URL\" value=\""
        + xml(config.url())
        + "\"/>\n"
        + "    <property name=\"User\" value=\""
        + xml(config.username())
        + "\"/>\n"
        + "    <property name=\"Password\" value=\""
        + xml(config.password())
        + "\"/>\n"
        + mysqlProperties(config.dbType())
        + "  </jdbc-connection-pool>\n"
        + "  <jdbc-resource enabled=\"true\"\n"
        + "                 jndi-name=\""
        + JTA_DATASOURCE
        + "\"\n"
        + "                 object-type=\"user\"\n"
        + "                 pool-name=\"RatchetPool\"/>\n"
        + "</resources>\n";
  }

  private static String mysqlProperties(String dbType) {
    if (!"mysql".equals(dbType)) {
      return "";
    }
    return "    <property name=\"sslMode\" value=\"DISABLED\"/>\n"
        + "    <property name=\"allowPublicKeyRetrieval\" value=\"true\"/>\n";
  }

  private static String dataSourceClassName(String dbType) {
    return switch (dbType) {
      case "mysql" -> "com.mysql.cj.jdbc.MysqlDataSource";
      case "postgresql" -> "org.postgresql.ds.PGSimpleDataSource";
      default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
    };
  }

  private static String driverCoordinates(String dbType) {
    return switch (dbType) {
      case "mysql" -> "com.mysql:mysql-connector-j";
      case "postgresql" -> "org.postgresql:postgresql";
      default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
    };
  }

  private static String xml(String value) {
    return value
        .replace("&", "&amp;")
        .replace("\"", "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;");
  }
}
