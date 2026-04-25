package run.ratchet.testsuite.util;

import run.ratchet.testsuite.infra.JdbcDatabaseConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;

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
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<server>\n"
        + "  <library id=\"RatchetJdbcDriverLib\">\n"
        + "    <fileset dir=\"${server.config.dir}/jdbc/"
        + xml(config.dbType())
        + "\" includes=\"*.jar\"/>\n"
        + "  </library>\n"
        + "  <jdbcDriver id=\"RatchetJdbcDriver\"\n"
        + "              libraryRef=\"RatchetJdbcDriverLib\"\n"
        + "              javax.sql.DataSource=\""
        + dataSourceClassName(config.dbType())
        + "\"/>\n"
        + "  <dataSource id=\"RatchetDS\"\n"
        + "              jndiName=\""
        + JTA_DATASOURCE
        + "\"\n"
        + "              jdbcDriverRef=\"RatchetJdbcDriver\"\n"
        + "              transactional=\"true\">\n"
        + "    <properties URL=\""
        + xml(config.url())
        + "\" user=\""
        + xml(config.username())
        + "\" password=\""
        + xml(config.password())
        + "\"/>\n"
        + "  </dataSource>\n"
        + "</server>\n";
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
