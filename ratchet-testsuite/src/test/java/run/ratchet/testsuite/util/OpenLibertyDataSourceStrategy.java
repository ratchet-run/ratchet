package run.ratchet.testsuite.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import run.ratchet.testsuite.infra.JdbcDatabaseConfig;

/**
 * Open Liberty datasource configuration strategy.
 *
 * <p>Liberty datasources are server-level resources, so this writes a test-only config drop-in and
 * JDBC driver library under the managed server's {@code target} directory after Testcontainers has
 * produced the real JDBC URL.
 */
public class OpenLibertyDataSourceStrategy implements DataSourceStrategy {

  private static final String JTA_DATASOURCE = "jdbc/RatchetDS";
  private static final String SERVER_CONFIG_DIR_PROPERTY = "openliberty.server.config.dir";

  @Override
  public void configureArchive(WebArchive archive, JdbcDatabaseConfig config) {
    Path serverConfigDir = serverConfigDir();
    Path jdbcDir = serverConfigDir.resolve("jdbc").resolve(config.dbType());
    Path configDropinsDir = serverConfigDir.resolve("configDropins").resolve("defaults");

    try {
      Files.createDirectories(jdbcDir);
      Files.createDirectories(configDropinsDir);
      deleteExistingDriverFiles(jdbcDir);
      copyJdbcDriver(config, jdbcDir);
      Files.writeString(configDropinsDir.resolve("ratchet-datasource.xml"), dataSourceXml(config));
    } catch (IOException e) {
      throw new IllegalStateException("Unable to configure Open Liberty datasource", e);
    }
  }

  @Override
  public String jtaDataSourceName() {
    return JTA_DATASOURCE;
  }

  private static Path serverConfigDir() {
    String configured = System.getProperty(SERVER_CONFIG_DIR_PROPERTY);
    if (configured == null || configured.isBlank()) {
      throw new IllegalStateException(
          SERVER_CONFIG_DIR_PROPERTY + " system property must be set for Open Liberty tests");
    }
    return Path.of(configured);
  }

  private static void deleteExistingDriverFiles(Path jdbcDir) throws IOException {
    try (var files = Files.newDirectoryStream(jdbcDir)) {
      for (Path file : files) {
        if (Files.isRegularFile(file)) {
          Files.delete(file);
        }
      }
    }
  }

  private static void copyJdbcDriver(JdbcDatabaseConfig config, Path jdbcDir) throws IOException {
    for (var file :
        Maven.configureResolver()
            .loadPomFromFile("pom.xml", config.dbType())
            .resolve(driverCoordinates(config.dbType()))
            .withTransitivity()
            .asFile()) {
      Files.copy(
          file.toPath(), jdbcDir.resolve(file.getName()), StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static String dataSourceXml(JdbcDatabaseConfig config) {
    // language=XML
    String template =
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <server>
          <library id="RatchetJdbcDriverLib">
            <fileset dir="${server.config.dir}/jdbc/%s" includes="*.jar"/>
          </library>
          <jdbcDriver id="RatchetJdbcDriver"
                      libraryRef="RatchetJdbcDriverLib"
                      javax.sql.DataSource="%s"/>
          <dataSource id="RatchetDS"
                      jndiName="%s"
                      jdbcDriverRef="RatchetJdbcDriver"
                      transactional="true">
            <properties URL="%s" user="%s" password="%s"/>
          </dataSource>
        </server>
        """;
    return template.formatted(
        xml(config.dbType()),
        dataSourceClassName(config.dbType()),
        JTA_DATASOURCE,
        xml(config.url()),
        xml(config.username()),
        xml(config.password()));
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
