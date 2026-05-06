package run.ratchet.testsuite.util;

import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
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
    // language=XML
    String template =
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE resources PUBLIC \
        "-//GlassFish.org//DTD GlassFish Application Server 3.1 Resource Definitions//EN" \
        "http://glassfish.org/dtds/glassfish-resources_1_5.dtd">
        <resources>
          <jdbc-connection-pool name="RatchetPool"
                                res-type="javax.sql.DataSource"
                                datasource-classname="%s"
                                transaction-isolation-level="read-committed"
                                is-isolation-level-guaranteed="true">
            <property name="URL" value="%s"/>
            <property name="User" value="%s"/>
            <property name="Password" value="%s"/>
        %s\
          </jdbc-connection-pool>
          <jdbc-resource enabled="true"
                         jndi-name="%s"
                         object-type="user"
                         pool-name="RatchetPool"/>
        </resources>
        """;
    return template.formatted(
        dataSourceClassName(config.dbType()),
        xml(config.url()),
        xml(config.username()),
        xml(config.password()),
        mysqlProperties(config.dbType()),
        JTA_DATASOURCE);
  }

  private static String mysqlProperties(String dbType) {
    if (!"mysql".equals(dbType)) {
      return "";
    }
    // language=XML
    return """
            <property name="sslMode" value="DISABLED"/>
            <property name="allowPublicKeyRetrieval" value="true"/>
        """;
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
