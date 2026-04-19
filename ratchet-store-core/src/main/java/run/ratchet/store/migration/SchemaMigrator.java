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
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
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
      acquireLock(connection);
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
      } finally {
        releaseLock(connection);
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
    Enumeration<URL> roots = classLoader.getResources(classpathPrefix);
    while (roots.hasMoreElements()) {
      URL root = roots.nextElement();
      for (String resourceName : resourceNames(root)) {
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

  private void acquireLock(Connection connection) throws SQLException {
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
      }
    }
  }

  private void releaseLock(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      switch (dialect) {
        case MYSQL -> {
          try (ResultSet ignored =
              statement.executeQuery("SELECT RELEASE_LOCK('" + LOCK_NAME + "')")) {
            // Result value is informational; closing the connection would release the lock anyway.
          }
        }
        case POSTGRESQL ->
            statement.execute("SELECT pg_advisory_unlock(" + POSTGRESQL_LOCK_KEY + ")");
      }
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
    connection.setAutoCommit(false);
    try {
      for (String sql : splitStatements(script.sql())) {
        try (Statement statement = connection.createStatement()) {
          statement.execute(sql);
        }
      }
      try (PreparedStatement statement =
          connection.prepareStatement(
              "INSERT INTO ratchet_schema_version (version, description, checksum) VALUES (?, ?, ?)")) {
        statement.setString(1, script.version());
        statement.setString(2, script.description());
        statement.setString(3, script.checksum());
        statement.executeUpdate();
      }
      connection.commit();
    } catch (SQLException | RuntimeException e) {
      connection.rollback();
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

  private List<String> jarResourceNames(URL root) throws IOException {
    JarURLConnection connection = (JarURLConnection) root.openConnection();
    try (JarFile jarFile = connection.getJarFile()) {
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

  private enum Dialect {
    MYSQL,
    POSTGRESQL;

    private static Dialect from(String value) {
      if (value == null || value.isBlank()) {
        throw new IllegalArgumentException("Ratchet schema migration dialect is required");
      }
      return switch (value.trim().toLowerCase(Locale.ROOT)) {
        case "mysql" -> MYSQL;
        case "postgres", "postgresql", "pg" -> POSTGRESQL;
        default ->
            throw new IllegalArgumentException("Unsupported Ratchet schema dialect: " + value);
      };
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
