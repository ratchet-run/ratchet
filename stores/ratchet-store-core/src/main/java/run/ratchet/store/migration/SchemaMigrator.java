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
import java.sql.DatabaseMetaData;
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
import java.util.Locale;
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

  public static final String DEFAULT_MIGRATION_PREFIX = "ddl/migrations";

  private static final Pattern SCRIPT_NAME = Pattern.compile("V(\\d+)__(.+)\\.sql");
  private static final String LOCK_NAME = "ratchet_schema_migration";
  private static final long POSTGRESQL_LOCK_KEY = 0x52617463686574L;
  // Oracle has no grant-free session-level advisory lock and its DDL auto-commits, so the lock is
  // held as an EXCLUSIVE table lock on a dedicated connection (see acquireLock/migrate). WAIT N is
  // in seconds; a second migrator that cannot acquire the lock within this window fails loudly.
  private static final int ORACLE_LOCK_WAIT_SECONDS = 120;
  private static final int ORA_NAME_ALREADY_USED = 955;
  private static final int ORA_RESOURCE_BUSY = 54;

  private final DataSource dataSource;
  private final Dialect dialect;
  private final String classpathPrefix;
  private final ClassLoader classLoader;

  public SchemaMigrator(DataSource dataSource, String dialect) {
    this(dataSource, dialect, DEFAULT_MIGRATION_PREFIX);
  }

  public SchemaMigrator(DataSource dataSource, String dialect, String classpathPrefix) {
    this(
        dataSource,
        dialect,
        classpathPrefix,
        Thread.currentThread().getContextClassLoader() != null
            ? Thread.currentThread().getContextClassLoader()
            : SchemaMigrator.class.getClassLoader());
  }

  SchemaMigrator(
      DataSource dataSource, String dialect, String classpathPrefix, ClassLoader classLoader) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    this.dialect = Dialect.from(dialect);
    this.classpathPrefix = normalizePrefix(classpathPrefix);
    this.classLoader = Objects.requireNonNull(classLoader, "classLoader");
  }

  /**
   * Resolves the migration dialect string ({@code "mysql"}, {@code "postgresql"}, or {@code
   * "oracle"}) from a JDBC connection's product name.
   *
   * <p>Whitelist only — auto-detection is intentionally narrow because look-alike products such as
   * CockroachDB report a PostgreSQL wire protocol but lack {@code pg_advisory_lock}, and MariaDB
   * variants beyond the explicit allow-list have not been verified. Operators running on an
   * unsupported product must set {@code RATCHET_SCHEMA_MIGRATION_DIALECT} explicitly.
   *
   * @throws SchemaInitializationException if the product is not on the whitelist
   */
  public static String dialectFromMetadata(Connection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String product = metaData.getDatabaseProductName();
    if (product == null) {
      throw new SchemaInitializationException(
          "JDBC driver did not report a database product name; set the migration dialect"
              + " explicitly via ratchet.schema.migration-dialect"
              + " (RATCHET_SCHEMA_MIGRATION_DIALECT)");
    }
    String normalized = product.trim().toLowerCase(Locale.ROOT);
    if (normalized.equals("mysql") || normalized.equals("mariadb")) {
      return "mysql";
    }
    if (normalized.equals("postgresql")) {
      return "postgresql";
    }
    if (normalized.equals("oracle")) {
      return "oracle";
    }
    throw new SchemaInitializationException(
        "Unsupported database product '"
            + product
            + "' for Ratchet schema auto-migration. Supported: MySQL, MariaDB, PostgreSQL, Oracle."
            + " Override via ratchet.schema.migration-dialect"
            + " (RATCHET_SCHEMA_MIGRATION_DIALECT) if your driver reports a non-standard name,"
            + " or apply the bundled DDL externally and leave ratchet.schema.auto-migrate=false.");
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
      // Most dialects hold a session-level advisory lock on the migration connection itself.
      // Oracle cannot: its DDL auto-commits (which would drop a transactional lock) and DBMS_LOCK
      // needs an EXECUTE grant managed users often lack, so it locks on a second connection.
      Connection lockConnection =
          dialect.usesDedicatedLockConnection() ? openLockConnection() : connection;
      try {
        acquireLock(lockConnection);
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
          "Oracle schema migration needs a second JDBC connection to hold its advisory lock, but"
              + " the DataSource could not supply one. Configure a connection pool maximum of at"
              + " least 2 (one connection runs the migration, the other holds the lock).",
          e);
    }
  }

  private void acquireLock(Connection connection) throws SQLException {
    if (dialect == Dialect.ORACLE) {
      acquireOracleLock(connection);
      return;
    }
    try (Statement statement = connection.createStatement()) {
      switch (dialect) {
        case MYSQL -> {
          try (ResultSet resultSet =
              statement.executeQuery("SELECT GET_LOCK('" + LOCK_NAME + "', 30)")) {
            if (!resultSet.next() || resultSet.getInt(1) != 1) {
              throw new SchemaMigrationException("Timed out acquiring MySQL schema migration lock");
            }
          }
        }
        case POSTGRESQL ->
            statement.execute("SELECT pg_advisory_lock(" + POSTGRESQL_LOCK_KEY + ")");
        default -> {
          // Oracle is handled above; no other dialect reaches here.
        }
      }
    }
  }

  /**
   * Acquires the Oracle migration lock on a dedicated connection.
   *
   * <p>Oracle DDL auto-commits, which would release any transactional lock held on the migration
   * connection itself, and {@code DBMS_LOCK} requires an EXECUTE grant that managed users often
   * lack. Instead a tiny dedicated {@code ratchet_schema_lock} table is locked {@code IN EXCLUSIVE
   * MODE} on a second connection that never runs DDL, so the lock survives the migration's
   * per-script commits. The lock table is separate from {@code ratchet_schema_version} on purpose:
   * an EXCLUSIVE lock on the ledger would block the migration connection's own ledger writes.
   */
  private void acquireOracleLock(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE IF NOT EXISTS ratchet_schema_lock (lock_name VARCHAR2(128) NOT NULL,"
              + " CONSTRAINT pk_ratchet_schema_lock PRIMARY KEY (lock_name))");
    } catch (SQLException e) {
      // ORA-00955: a concurrent migrator created the lock table first. Any other error is fatal.
      if (e.getErrorCode() != ORA_NAME_ALREADY_USED) {
        throw e;
      }
    }
    connection.setAutoCommit(false);
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "LOCK TABLE ratchet_schema_lock IN EXCLUSIVE MODE WAIT " + ORACLE_LOCK_WAIT_SECONDS);
    } catch (SQLException e) {
      if (e.getErrorCode() == ORA_RESOURCE_BUSY) {
        throw new SchemaMigrationException(
            "Timed out after "
                + ORACLE_LOCK_WAIT_SECONDS
                + "s acquiring the Oracle schema migration lock; another migrator held it longer"
                + " than the wait window.",
            e);
      }
      throw e;
    }
  }

  private void releaseLock(Connection connection) throws SQLException {
    if (dialect == Dialect.ORACLE) {
      // Commit releases the EXCLUSIVE table lock; the dedicated lock connection is closed by the
      // caller immediately afterward.
      connection.commit();
      return;
    }
    try (Statement statement = connection.createStatement()) {
      switch (dialect) {
        case MYSQL -> {
          try (ResultSet resultSet =
              statement.executeQuery("SELECT RELEASE_LOCK('" + LOCK_NAME + "')")) {
            if (!resultSet.next() || resultSet.getInt(1) != 1) {
              throw new SQLException("Failed to release MySQL schema migration lock");
            }
          }
        }
        case POSTGRESQL ->
            statement.execute("SELECT pg_advisory_unlock(" + POSTGRESQL_LOCK_KEY + ")");
        default -> {
          // Oracle is handled above; no other dialect reaches here.
        }
      }
    }
  }

  private void releaseLock(Connection connection, Throwable primaryFailure) throws SQLException {
    try {
      releaseLock(connection);
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

  private enum Dialect {
    MYSQL,
    POSTGRESQL,
    ORACLE;

    private static Dialect from(String value) {
      if (value == null || value.isBlank()) {
        throw new IllegalArgumentException("Ratchet schema migration dialect is required");
      }
      return switch (value.trim().toLowerCase(Locale.ROOT)) {
        case "mysql" -> MYSQL;
        case "postgres", "postgresql", "pg" -> POSTGRESQL;
        case "oracle" -> ORACLE;
        default ->
            throw new IllegalArgumentException("Unsupported Ratchet schema dialect: " + value);
      };
    }

    private boolean usesDedicatedLockConnection() {
      return this == ORACLE;
    }

    private String createVersionTableSql() {
      return switch (this) {
        case MYSQL ->
            """
            CREATE TABLE IF NOT EXISTS ratchet_schema_version
            (
                version     VARCHAR(20)  NOT NULL,
                applied_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                description VARCHAR(200) NOT NULL,
                checksum    VARCHAR(64)  NULL,
                PRIMARY KEY (version)
            ) ENGINE = InnoDB
              DEFAULT CHARSET = utf8mb4
              COLLATE = utf8mb4_unicode_ci\
            """;
        case POSTGRESQL ->
            """
            CREATE TABLE IF NOT EXISTS ratchet_schema_version
            (
                version VARCHAR(20) NOT NULL,
                applied_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                description VARCHAR(200) NOT NULL,
                checksum VARCHAR(64),
                CONSTRAINT pk_ratchet_schema_version PRIMARY KEY (version)
            )\
            """;
        case ORACLE ->
            """
            CREATE TABLE IF NOT EXISTS ratchet_schema_version
            (
                version     VARCHAR2(20)  NOT NULL,
                applied_at  TIMESTAMP(6)  DEFAULT SYSTIMESTAMP NOT NULL,
                description VARCHAR2(200) NOT NULL,
                checksum    VARCHAR2(64),
                CONSTRAINT pk_ratchet_schema_version PRIMARY KEY (version)
            )\
            """;
      };
    }

    private String recordVersionSql() {
      return switch (this) {
        case MYSQL ->
            "INSERT INTO ratchet_schema_version (version, description, checksum) VALUES (?, ?, ?)"
                + " ON DUPLICATE KEY UPDATE description = VALUES(description),"
                + " checksum = VALUES(checksum)";
        case POSTGRESQL ->
            "INSERT INTO ratchet_schema_version (version, description, checksum) VALUES (?, ?, ?)"
                + " ON CONFLICT (version) DO UPDATE SET description = EXCLUDED.description,"
                + " checksum = EXCLUDED.checksum";
        case ORACLE ->
            "MERGE INTO ratchet_schema_version t"
                + " USING (SELECT ? AS version, ? AS description, ? AS checksum FROM dual) s"
                + " ON (t.version = s.version)"
                + " WHEN MATCHED THEN UPDATE SET t.description = s.description,"
                + " t.checksum = s.checksum"
                + " WHEN NOT MATCHED THEN INSERT (version, description, checksum)"
                + " VALUES (s.version, s.description, s.checksum)";
      };
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
