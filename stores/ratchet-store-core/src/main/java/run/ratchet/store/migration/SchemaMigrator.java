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
package run.ratchet.store.migration;

import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * Optional classpath SQL migrator for Ratchet-managed schemas.
 *
 * <p>Ratchet intentionally does not depend on Flyway or Liquibase. Applications that want a small
 * built-in bootstrap step can call this utility from a {@code SchedulerLifecycleHook#beforeStart}
 * implementation, before the poller and recurring scheduler are initialized.
 */
public final class SchemaMigrator {

  private static final String SINGLE_STATEMENT_DIRECTIVE = "-- ratchet:single-statement";

  public static final String DEFAULT_MIGRATION_PREFIX = "ddl/migrations";

  private static final Pattern SCRIPT_NAME = Pattern.compile("V(\\d+)__(.+)\\.sql");

  private final DataSource dataSource;
  private final SchemaMigrationDialect dialect;
  private final String classpathPrefix;
  private final ClassLoader classLoader;

  public SchemaMigrator(DataSource dataSource, SchemaMigrationDialect dialect) {
    this(dataSource, dialect, DEFAULT_MIGRATION_PREFIX);
  }

  public SchemaMigrator(
      DataSource dataSource, SchemaMigrationDialect dialect, String classpathPrefix) {
    this(dataSource, dialect, classpathPrefix, defaultClassLoader());
  }

  SchemaMigrator(
      DataSource dataSource,
      SchemaMigrationDialect dialect,
      String classpathPrefix,
      ClassLoader classLoader) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    this.dialect = Objects.requireNonNull(dialect, "dialect");
    this.classpathPrefix = normalizePrefix(classpathPrefix);
    this.classLoader = Objects.requireNonNull(classLoader, "classLoader");
  }

  private static ClassLoader defaultClassLoader() {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    return contextClassLoader != null ? contextClassLoader : SchemaMigrator.class.getClassLoader();
  }

  private static String normalizePrefix(String prefix) {
    String normalized =
        prefix == null || prefix.isBlank() ? DEFAULT_MIGRATION_PREFIX : prefix.trim();
    while (normalized.startsWith("/")) {
      normalized = normalized.substring(1);
    }
    while (normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    return normalized;
  }

  private static String sha256(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new SchemaMigrationException("SHA-256 digest is not available", e);
    }
  }

  private static List<String> splitStatements(String sql) {
    String stripped = sql.stripLeading();
    if (stripped.startsWith(SINGLE_STATEMENT_DIRECTIVE)) {
      int directiveEnd = stripped.indexOf('\n');
      if (directiveEnd < 0) {
        throw new SchemaMigrationException(
            "Single-statement migration directive must be followed by SQL");
      }
      String statement = stripped.substring(directiveEnd + 1).strip();
      if (statement.isEmpty()) {
        throw new SchemaMigrationException(
            "Single-statement migration directive must be followed by SQL");
      }
      return List.of(statement);
    }

    List<String> statements = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean singleQuoted = false;
    boolean doubleQuoted = false;
    boolean lineComment = false;
    boolean blockComment = false;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';

      if (lineComment) {
        current.append(c);
        if (c == '\n') {
          lineComment = false;
        }
        continue;
      }
      if (blockComment) {
        current.append(c);
        if (c == '*' && next == '/') {
          current.append(next);
          i++;
          blockComment = false;
        }
        continue;
      }
      if (singleQuoted) {
        current.append(c);
        if (c == '\'' && next == '\'') {
          current.append(next);
          i++;
        } else if (c == '\'') {
          singleQuoted = false;
        }
        continue;
      }
      if (doubleQuoted) {
        current.append(c);
        if (c == '"' && next == '"') {
          current.append(next);
          i++;
        } else if (c == '"') {
          doubleQuoted = false;
        }
        continue;
      }

      if (c == '-' && next == '-') {
        current.append(c).append(next);
        i++;
        lineComment = true;
      } else if (c == '/' && next == '*') {
        current.append(c).append(next);
        i++;
        blockComment = true;
      } else if (c == '\'') {
        current.append(c);
        singleQuoted = true;
      } else if (c == '"') {
        current.append(c);
        doubleQuoted = true;
      } else if (c == ';') {
        addStatement(statements, current);
      } else {
        current.append(c);
      }
    }

    addStatement(statements, current);
    return statements;
  }

  private static void addStatement(List<String> statements, StringBuilder current) {
    String statement = current.toString().trim();
    if (!statement.isEmpty()) {
      statements.add(statement);
    }
    current.setLength(0);
  }

  /**
   * Discovers, validates, and applies pending migration scripts in ascending version order.
   *
   * @return result containing the applied and skipped scripts
   * @throws IOException if classpath resources cannot be read
   * @throws SQLException if a database operation fails
   */
  public MigrationResult migrate() throws IOException, SQLException {
    List<MigrationScript> scripts = discoverMigrations();
    List<MigrationScript> applied = new ArrayList<>();
    List<MigrationScript> skipped = new ArrayList<>();

    try (Connection connection = dataSource.getConnection()) {
      // Most dialects hold a session-level advisory lock on the migration connection itself; a
      // dialect whose DDL auto-commits asks for a dedicated lock connection instead so the lock
      // survives the per-script commits.
      Connection lockConnection =
          dialect.usesDedicatedLockConnection() ? openLockConnection() : connection;
      try {
        dialect.acquireLock(lockConnection);
        Throwable migrationFailure = null;
        try {
          ensureSchemaVersionTable(connection);
          for (MigrationScript script : scripts) {
            String existingChecksum = existingChecksum(connection, script.version());
            if (existingChecksum != null) {
              if (!existingChecksum.equals(script.checksum())) {
                throw new SchemaMigrationException(
                    "Checksum mismatch for Ratchet schema migration "
                        + script.version()
                        + " ("
                        + script.resourceName()
                        + ")");
              }
              skipped.add(script);
              continue;
            }
            applyMigration(connection, script);
            applied.add(script);
          }
        } catch (SQLException | RuntimeException | Error e) {
          migrationFailure = e;
          throw e;
        } finally {
          releaseLock(lockConnection, migrationFailure);
        }
      } finally {
        if (lockConnection != connection) {
          lockConnection.close();
        }
      }
    }

    return new MigrationResult(applied, skipped);
  }

  /**
   * Returns migration scripts under the configured classpath prefix, sorted by numeric version.
   *
   * @throws IOException if classpath resources cannot be read
   */
  public List<MigrationScript> discoverMigrations() throws IOException {
    Map<String, MigrationScript> scripts = new HashMap<>();
    Set<String> seenResources = new HashSet<>();
    Enumeration<URL> roots = classLoader.getResources(classpathPrefix);
    while (roots.hasMoreElements()) {
      URL root = roots.nextElement();
      for (String resourceName : resourceNames(root)) {
        if (!seenResources.add(resourceName)) {
          continue;
        }
        MigrationScript script = readScript(resourceName);
        MigrationScript previous = scripts.putIfAbsent(script.version(), script);
        if (previous != null) {
          throw new SchemaMigrationException(
              "Duplicate Ratchet schema migration version "
                  + script.version()
                  + ": "
                  + previous.resourceName()
                  + " and "
                  + script.resourceName());
        }
      }
    }

    return scripts.values().stream()
        .sorted(Comparator.comparingInt(MigrationScript::numericVersion))
        .toList();
  }

  private Connection openLockConnection() throws SQLException {
    try {
      return dataSource.getConnection();
    } catch (SQLException e) {
      throw new SchemaMigrationException(
          "This dialect holds its migration lock on a dedicated JDBC connection, but the DataSource"
              + " could not supply a second one. Configure a connection pool maximum of at least 2"
              + " (one connection runs the migration, the other holds the lock).",
          e);
    }
  }

  private void releaseLock(Connection connection, Throwable primaryFailure) throws SQLException {
    try {
      dialect.releaseLock(connection);
    } catch (SQLException e) {
      if (primaryFailure != null) {
        primaryFailure.addSuppressed(e);
        return;
      }
      throw e;
    }
  }

  private void ensureSchemaVersionTable(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(dialect.createVersionTableSql());
    }
  }

  private String existingChecksum(Connection connection, String version) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "SELECT checksum FROM ratchet_schema_version WHERE version = ?")) {
      statement.setString(1, version);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return null;
        }
        String checksum = resultSet.getString(1);
        if (checksum == null || checksum.isBlank()) {
          throw new SchemaMigrationException(
              "Ratchet schema migration " + version + " is already recorded without a checksum");
        }
        return checksum;
      }
    }
  }

  private void applyMigration(Connection connection, MigrationScript script) throws SQLException {
    boolean originalAutoCommit = connection.getAutoCommit();
    // The bundled migrator owns this connection while applying one script. Container-managed
    // callers should provide an unmanaged migration connection.
    connection.setAutoCommit(false);
    try {
      for (String sql : splitStatements(script.sql())) {
        try (Statement statement = connection.createStatement()) {
          statement.execute(sql);
        }
      }
      // Upsert so the bundled migrator authoritatively owns the schema-version metadata. If a
      // script self-recorded an older description/checksum, a managed run rewrites both values to
      // match the bundled script.
      try (PreparedStatement statement = connection.prepareStatement(dialect.recordVersionSql())) {
        statement.setString(1, script.version());
        statement.setString(2, script.description());
        statement.setString(3, script.checksum());
        statement.executeUpdate();
      }
      connection.commit();
    } catch (SQLException | RuntimeException e) {
      try {
        connection.rollback();
      } catch (SQLException rollbackException) {
        e.addSuppressed(rollbackException);
      }
      throw e;
    } finally {
      connection.setAutoCommit(originalAutoCommit);
    }
  }

  private List<String> resourceNames(URL root) throws IOException {
    try {
      return switch (root.getProtocol()) {
        case "file" -> fileResourceNames(root);
        case "jar" -> jarResourceNames(root);
        default -> List.of();
      };
    } catch (URISyntaxException e) {
      throw new IOException("Could not read Ratchet migration resources under " + root, e);
    }
  }

  private List<String> fileResourceNames(URL root) throws IOException, URISyntaxException {
    Path directory = Path.of(root.toURI());
    if (!Files.isDirectory(directory)) {
      return List.of();
    }
    try (var files = Files.list(directory)) {
      return files
          .filter(Files::isRegularFile)
          .map(path -> classpathPrefix + "/" + path.getFileName())
          .filter(this::isMigrationResource)
          .toList();
    }
  }

  @SuppressWarnings(
      "resource") // JarFile is a JVM-cached handle; closing it breaks concurrent callers (see
  // comment)
  private List<String> jarResourceNames(URL root) throws IOException {
    // Do NOT close the JarFile returned by JarURLConnection — it is a process-wide cached handle
    // (see JarURLConnection.getJarFile()), and closing it sabotages concurrent callers reading the
    // same JAR (e.g., two parallel SchemaMigrator instances during clustered startup).
    JarURLConnection connection = (JarURLConnection) root.openConnection();
    JarFile jarFile = connection.getJarFile();
    List<String> names = new ArrayList<>();
    Enumeration<JarEntry> entries = jarFile.entries();
    while (entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      String name = entry.getName();
      if (entry.isDirectory()
          || !name.startsWith(classpathPrefix + "/")
          || name.substring(classpathPrefix.length() + 1).contains("/")
          || !isMigrationResource(name)) {
        continue;
      }
      names.add(name);
    }
    return names;
  }

  private boolean isMigrationResource(String resourceName) {
    String fileName = resourceName.substring(resourceName.lastIndexOf('/') + 1);
    return SCRIPT_NAME.matcher(fileName).matches();
  }

  private MigrationScript readScript(String resourceName) throws IOException {
    String fileName = resourceName.substring(resourceName.lastIndexOf('/') + 1);
    Matcher matcher = SCRIPT_NAME.matcher(fileName);
    if (!matcher.matches()) {
      throw new SchemaMigrationException("Invalid Ratchet schema migration name: " + resourceName);
    }

    try (InputStream inputStream = classLoader.getResourceAsStream(resourceName)) {
      if (inputStream == null) {
        throw new IOException("Could not open Ratchet schema migration " + resourceName);
      }
      String sql = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
      return new MigrationScript(
          matcher.group(1), matcher.group(2).replace('_', ' '), resourceName, sha256(sql), sql);
    }
  }

  public record MigrationResult(List<MigrationScript> applied, List<MigrationScript> skipped) {

    public MigrationResult {
      applied = List.copyOf(applied);
      skipped = List.copyOf(skipped);
    }

    public int appliedCount() {
      return applied.size();
    }

    public int skippedCount() {
      return skipped.size();
    }
  }

  public record MigrationScript(
      String version, String description, String resourceName, String checksum, String sql) {

    private int numericVersion() {
      return Integer.parseInt(version);
    }
  }
}
