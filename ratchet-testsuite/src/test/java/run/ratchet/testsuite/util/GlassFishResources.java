package run.ratchet.testsuite.util;

import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import run.ratchet.testsuite.infra.JdbcDatabaseConfig;

final class GlassFishResources {

  static final String JTA_DATASOURCE = "java:app/jdbc/RatchetDS";

  private GlassFishResources() {}

  static void addResources(WebArchive archive, JdbcDatabaseConfig config, String doctype) {
    archive.addAsWebInfResource(
        new StringAsset(resourcesXml(config, doctype)), "glassfish-resources.xml");
    archive.addAsLibraries(
        Maven.configureResolver()
            .loadPomFromFile("pom.xml", config.dbType())
            .resolve(driverCoordinates(config.dbType()))
            .withTransitivity()
            .asFile());
  }

  private static String resourcesXml(JdbcDatabaseConfig config, String doctype) {
    // language=XML
    String template =
        """
        <?xml version="1.0" encoding="UTF-8"?>
        %s
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
        doctype,
        DataSourceResources.dataSourceClassName(config.dbType()),
        DataSourceResources.xml(config.url()),
        DataSourceResources.xml(config.username()),
        DataSourceResources.xml(config.password()),
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

  private static String driverCoordinates(String dbType) {
    return DataSourceResources.driverCoordinates(dbType);
  }
}
