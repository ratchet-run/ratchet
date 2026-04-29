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
    // language=XML
    String template =
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE resources PUBLIC "-//Payara.fish//DTD Payara Server 4 Resource Definitions//EN" \
        "http://docs.payara.fish/schemas/payara-resources_1_8.dtd">
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
