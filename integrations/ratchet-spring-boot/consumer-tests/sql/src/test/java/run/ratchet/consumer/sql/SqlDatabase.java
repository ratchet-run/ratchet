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
package run.ratchet.consumer.sql;

import com.github.dockerjava.api.model.Driver;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Mount;
import com.github.dockerjava.api.model.MountType;
import com.github.dockerjava.api.model.VolumeOptions;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/** A real SQL database matching the store selected by {@code -Dstore}. */
public final class SqlDatabase implements AutoCloseable {
  private static final String TEST_DATA_DIRECTORY = "RATCHET_TEST_DATA_DIRECTORY";
  private static final String ORACLE_DATA_DIRECTORY = "/opt/oracle/oradata/FREE";
  private static final String TEST_TMPFS = "RATCHET_TEST_TMPFS";
  private static final Map<String, String> TMPFS_DRIVER_OPTIONS =
      Map.of("type", "tmpfs", "device", "tmpfs", "o", "size=16g");

  private final String store;
  private final JdbcDatabaseContainer<?> container;
  private final Path ownedDataDirectory;

  private SqlDatabase(String store, JdbcDatabaseContainer<?> container, Path ownedDataDirectory) {
    this.store = store;
    this.container = container;
    this.ownedDataDirectory = ownedDataDirectory;
  }

  public static SqlDatabase start() {
    return start(System.getProperty("store", "postgresql"));
  }

  /** Starts a separate container for tests that need a fresh catalog. */
  public static SqlDatabase startIsolated() {
    return start();
  }

  public static SqlDatabase start(String store) {
    Path ownedDataDirectory = createOwnedDataDirectory(store);
    JdbcDatabaseContainer<?> container = create(store, ownedDataDirectory);
    SqlDatabase database = new SqlDatabase(store, container, ownedDataDirectory);
    try {
      container.start();
      return database;
    } catch (RuntimeException failure) {
      try {
        database.close();
      } catch (RuntimeException cleanupFailure) {
        failure.addSuppressed(cleanupFailure);
      }
      throw failure;
    }
  }

  public String store() {
    return store;
  }

  public Map<String, Object> properties() {
    return Map.of(
        "spring.datasource.url", container.getJdbcUrl(),
        "spring.datasource.username", container.getUsername(),
        "spring.datasource.password", container.getPassword(),
        "spring.profiles.active", store);
  }

  /** Returns the JDBC value used with {@link #jobIdPredicate()} for this store. */
  public Object queryId(UUID jobId) {
    return "postgresql".equals(store) ? jobId : jobId.toString();
  }

  /** SQL expression that compares a scheduler binary UUID column to {@link #queryId(UUID)}. */
  public String jobIdPredicate() {
    return switch (store) {
      case "postgresql" -> "job_id = ?";
      case "mysql" -> "BIN_TO_UUID(job_id) = ?";
      case "oracle" -> "RAWTOHEX(job_id) = REPLACE(UPPER(?), '-', '')";
      case "sqlserver" -> "CONVERT(varchar(32), job_id, 2) = REPLACE(UPPER(?), '-', '')";
      default -> throw new IllegalStateException("Unsupported SQL consumer store: " + store);
    };
  }

  @Override
  public void close() {
    try {
      container.stop();
    } finally {
      deleteOwnedDataDirectory(ownedDataDirectory);
    }
  }

  private static Path createOwnedDataDirectory(String store) {
    String parent = System.getenv(TEST_DATA_DIRECTORY);
    if (!"oracle".equals(store)) return null;
    if (parent == null || parent.isBlank()) {
      if ("true".equalsIgnoreCase(System.getenv(TEST_TMPFS))) {
        throw new IllegalStateException(
            "Oracle seeded data requires RATCHET_TEST_DATA_DIRECTORY when RATCHET_TEST_TMPFS is"
                + " enabled");
      }
      return null;
    }
    try {
      Path directory =
          Files.createTempDirectory(Path.of(parent).toAbsolutePath(), "ratchet-oracle-");
      Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwxrwxrwx"));
      return directory;
    } catch (IOException failure) {
      throw new IllegalStateException(
          "Cannot create owned Oracle data directory under " + parent, failure);
    }
  }

  private static void deleteOwnedDataDirectory(Path directory) {
    if (directory == null || !Files.exists(directory)) return;
    try {
      Files.walkFileTree(
          directory,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                throws IOException {
              Files.delete(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException failure)
                throws IOException {
              if (failure != null) throw failure;
              Files.delete(path);
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException failure) {
      throw new IllegalStateException(
          "Cannot remove owned Oracle test data directory " + directory, failure);
    }
  }

  @SuppressWarnings("resource")
  private static JdbcDatabaseContainer<?> create(String store, Path ownedDataDirectory) {
    return switch (store) {
      case "postgresql" ->
          withTestTmpfsData(
              new PostgreSQLContainer("postgres:16")
                  .withDatabaseName("ratchet_test")
                  .withUsername("ratchet")
                  .withPassword("ratchet"),
              "/var/lib/postgresql/data");
      case "mysql" ->
          withTestTmpfsData(
              new MySQLContainer("mysql:8.0")
                  .withDatabaseName("ratchet_test")
                  .withUsername("ratchet")
                  .withPassword("ratchet"),
              "/var/lib/mysql");
      case "oracle" ->
          withOracleData(
              new OracleDataContainer(ownedDataDirectory)
                  .withDatabaseName("ratchet_test")
                  .withUsername("ratchet")
                  .withPassword("ratchet")
                  .withSharedMemorySize(2L * 1024 * 1024 * 1024)
                  .withStartupTimeout(Duration.ofMinutes(5)),
              ownedDataDirectory);
      case "sqlserver" -> {
        String image =
            System.getenv()
                .getOrDefault("RATCHET_MSSQL_IMAGE", "mcr.microsoft.com/mssql/server:2022-latest");
        yield withTestTmpfsData(
            new MSSQLServerContainer(
                    DockerImageName.parse(image)
                        .asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server"))
                .acceptLicense()
                .withPassword("Ratchet!Str0ngPwd")
                .withUrlParam("trustServerCertificate", "true")
                .withStartupTimeout(Duration.ofMinutes(5)),
            "/var/opt/mssql");
      }
      default ->
          throw new IllegalArgumentException(
              "Unsupported SQL consumer store '"
                  + store
                  + "'; expected postgresql, mysql, oracle, or sqlserver");
    };
  }

  /** Uses an owned host directory only when explicitly requested; copy-up preserves image seeds. */
  private static OracleContainer withOracleData(OracleContainer container, Path directory) {
    if (directory == null) return container;
    return withDataVolume(
        container,
        ORACLE_DATA_DIRECTORY,
        Map.of("type", "none", "o", "bind", "device", directory.toString()));
  }

  private static final class OracleDataContainer extends OracleContainer {
    private final Path ownedDirectory;

    private OracleDataContainer(Path ownedDirectory) {
      super("gvenzl/oracle-free:slim-faststart");
      this.ownedDirectory = ownedDirectory;
    }

    @Override
    public void stop() {
      try {
        if (ownedDirectory != null && isRunning()) {
          var result = execInContainer("chmod", "-R", "a+rwX", ORACLE_DATA_DIRECTORY);
          if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                "Cannot prepare owned Oracle data for deletion: " + result.getStderr());
          }
        }
      } catch (IOException failure) {
        throw new IllegalStateException("Cannot prepare owned Oracle data for deletion", failure);
      } catch (InterruptedException failure) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Interrupted preparing owned Oracle data for deletion", failure);
      } finally {
        super.stop();
      }
    }
  }

  /**
   * Moves database writes off Docker's overlay filesystem when {@value #TEST_TMPFS} is explicitly
   * enabled. These databases initialize fresh data after startup. Oracle's image-seeded data
   * requires the persistent host-directory option instead.
   */
  private static <T extends JdbcDatabaseContainer<?>> T withTestTmpfsData(
      T container, String dataDirectory) {
    if (!"true".equalsIgnoreCase(System.getenv(TEST_TMPFS))) return container;
    return withDataVolume(container, dataDirectory, TMPFS_DRIVER_OPTIONS);
  }

  private static <T extends JdbcDatabaseContainer<?>> T withDataVolume(
      T container, String dataDirectory, Map<String, String> driverOptions) {
    container.withCreateContainerCmdModifier(
        command -> {
          HostConfig hostConfig = command.getHostConfig();
          if (hostConfig == null) {
            hostConfig = HostConfig.newHostConfig();
            command.withHostConfig(hostConfig);
          }
          List<Mount> mounts = new ArrayList<>();
          if (hostConfig.getMounts() != null) mounts.addAll(hostConfig.getMounts());
          mounts.add(
              new Mount()
                  .withType(MountType.VOLUME)
                  .withTarget(dataDirectory)
                  .withVolumeOptions(
                      new VolumeOptions()
                          .withNoCopy(false)
                          .withDriverConfig(
                              new Driver().withName("local").withOptions(driverOptions))));
          hostConfig.withMounts(mounts);
        });
    return container;
  }
}
