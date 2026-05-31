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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import run.ratchet.testsuite.infra.JdbcDatabaseConfig;

class OpenLibertyDataSourceStrategyTest {

  private static final String SERVER_CONFIG_DIR_PROPERTY = "openliberty.server.config.dir";

  @TempDir Path tempDir;

  @Test
  void configureArchiveRejectsMissingServerConfigDirProperty() {
    String original = System.getProperty(SERVER_CONFIG_DIR_PROPERTY);
    System.clearProperty(SERVER_CONFIG_DIR_PROPERTY);
    try {
      IllegalStateException failure =
          assertThrows(
              IllegalStateException.class,
              () -> new OpenLibertyDataSourceStrategy().configureArchive(null, config()));

      assertTrue(failure.getMessage().contains(SERVER_CONFIG_DIR_PROPERTY));
      assertTrue(failure.getMessage().contains("must be set"));
    } finally {
      restoreProperty(original);
    }
  }

  @Test
  void configureArchiveRejectsServerConfigDirThatIsNotDirectory() throws Exception {
    Path file = tempDir.resolve("server.xml");
    java.nio.file.Files.writeString(file, "<server/>");

    String original = System.getProperty(SERVER_CONFIG_DIR_PROPERTY);
    System.setProperty(SERVER_CONFIG_DIR_PROPERTY, file.toString());
    try {
      IllegalStateException failure =
          assertThrows(
              IllegalStateException.class,
              () -> new OpenLibertyDataSourceStrategy().configureArchive(null, config()));

      assertTrue(failure.getMessage().contains(SERVER_CONFIG_DIR_PROPERTY));
      assertTrue(failure.getMessage().contains("existing directory"));
      assertTrue(failure.getMessage().contains(file.toAbsolutePath().normalize().toString()));
    } finally {
      restoreProperty(original);
    }
  }

  private static JdbcDatabaseConfig config() {
    return new JdbcDatabaseConfig(
        "jdbc:mysql://localhost:3306/ratchet",
        "user",
        "password",
        "com.mysql.cj.jdbc.Driver",
        "mysql");
  }

  private static void restoreProperty(String value) {
    if (value == null) {
      System.clearProperty(SERVER_CONFIG_DIR_PROPERTY);
    } else {
      System.setProperty(SERVER_CONFIG_DIR_PROPERTY, value);
    }
  }
}
