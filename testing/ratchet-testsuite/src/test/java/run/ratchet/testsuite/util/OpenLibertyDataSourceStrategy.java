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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
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
      createDirectories(jdbcDir);
      createDirectories(configDropinsDir);
      deleteExistingDriverFiles(jdbcDir);
      copyJdbcDriver(config, jdbcDir);
      writeDataSourceXml(configDropinsDir.resolve("ratchet-datasource.xml"), config);
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
    Path path;
    try {
      path = Path.of(configured).toAbsolutePath().normalize();
    } catch (InvalidPathException e) {
      throw new IllegalStateException(
          SERVER_CONFIG_DIR_PROPERTY + " system property is not a valid path: " + configured, e);
    }
    if (!Files.isDirectory(path)) {
      throw new IllegalStateException(
          SERVER_CONFIG_DIR_PROPERTY + " must point to an existing directory: " + path);
    }
    if (!Files.isWritable(path)) {
      throw new IllegalStateException(
          SERVER_CONFIG_DIR_PROPERTY + " must point to a writable directory: " + path);
    }
    return path;
  }

  private static void createDirectories(Path dir) throws IOException {
    try {
      Files.createDirectories(dir);
    } catch (IOException e) {
      throw new IOException("Unable to create Open Liberty datasource directory: " + dir, e);
    }
  }

  private static void deleteExistingDriverFiles(Path jdbcDir) throws IOException {
    DirectoryStream<Path> files;
    try {
      files = Files.newDirectoryStream(jdbcDir);
    } catch (IOException e) {
      throw new IOException("Unable to list Open Liberty JDBC driver directory: " + jdbcDir, e);
    }
    try (files) {
      for (Path file : files) {
        if (Files.isRegularFile(file)) {
          deleteDriverFile(file);
        }
      }
    }
  }

  private static void deleteDriverFile(Path file) throws IOException {
    try {
      Files.delete(file);
    } catch (IOException e) {
      throw new IOException("Unable to delete stale Open Liberty JDBC driver: " + file, e);
    }
  }

  private static void copyJdbcDriver(JdbcDatabaseConfig config, Path jdbcDir) throws IOException {
    for (var file :
        Maven.configureResolver()
            .loadPomFromFile("pom.xml", config.dbType())
            .resolve(driverCoordinates(config.dbType()))
            .withTransitivity()
            .asFile()) {
      Path target = jdbcDir.resolve(file.getName());
      try {
        Files.copy(file.toPath(), target, StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException e) {
        throw new IOException("Unable to copy Open Liberty JDBC driver to: " + target, e);
      }
    }
  }

  private static void writeDataSourceXml(Path target, JdbcDatabaseConfig config)
      throws IOException {
    try {
      Files.writeString(target, dataSourceXml(config));
    } catch (IOException e) {
      throw new IOException("Unable to write Open Liberty datasource config: " + target, e);
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
        DataSourceResources.xml(config.dbType()),
        DataSourceResources.dataSourceClassName(config.dbType()),
        JTA_DATASOURCE,
        DataSourceResources.xml(config.url()),
        DataSourceResources.xml(config.username()),
        DataSourceResources.xml(config.password()));
  }

  private static String driverCoordinates(String dbType) {
    return DataSourceResources.driverCoordinates(dbType);
  }
}
