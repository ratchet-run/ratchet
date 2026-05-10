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
  private static final String BASELINE_PROBE_TABLE = "scheduler_job_queue";

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
   * Resolves the migration dialect string ({@code "mysql"} or {@code "postgresql"}) from a JDBC
   * connection's product name.
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
    throw new SchemaInitializationException(
        "Unsupported database product '"
            + product
            + "' for Ratchet schema auto-migration. Supported: MySQL, MariaDB, PostgreSQL."
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
      acquireLock(connection);
      try {
        ensureSchemaVersionTable(connection);
        verifyBaselineCompatible(connection);
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
          try (ResultSet resultSet =
              statement.executeQuery("SELECT RELEASE_LOCK('" + LOCK_NAME + "')")) {
            if (!resultSet.next() || resultSet.getInt(1) != 1) {
              throw new SQLException("Failed to release MySQL schema migration lock");
            }
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

  /**
   * Refuses to baseline a pre-existing Ratchet schema implicitly. A populated set of {@code
   * scheduler_*} tables coupled with an empty {@code ratchet_schema_version} table would silently
   * skip every migration on first run, leaving column-level upgrades (V005 hot/cold split, V006
   * trace_context, V007/V008 query indexes) unapplied.
   */
  private void verifyBaselineCompatible(Connection connection) throws SQLException {
    if (!isVersionTableEmpty(connection)) {
      return;
    }
    if (!coreTableExists(connection)) {
      return;
    }
    throw new SchemaInitializationException(
        "Detected existing Ratchet tables ("
            + BASELINE_PROBE_TABLE
            + " is present) but ratchet_schema_version is empty."
            + " Auto-migration would skip the upgrade history and leave the schema stale."
            + " To enable auto-migration on this database, seed ratchet_schema_version manually"
            + " with the highest applied version (one row per V### already applied; see"
            + " docs/deployment/database-setup.md), or drop and recreate the schema. To keep"
            + " managing the schema externally, set ratchet.schema.auto-migrate=false.");
  }

  private boolean isVersionTableEmpty(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT 1 FROM ratchet_schema_version")) {
      return !rs.next();
    }
  }

  private boolean coreTableExists(Connection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    String[] candidates =
        new String[] {
          BASELINE_PROBE_TABLE,
          BASELINE_PROBE_TABLE.toUpperCase(Locale.ROOT),
          BASELINE_PROBE_TABLE.toLowerCase(Locale.ROOT)
        };
    for (String candidate : candidates) {
      try (ResultSet rs = metaData.getTables(null, null, candidate, new String[] {"TABLE"})) {
        if (rs.next()) {
          return true;
        }
      }
    }
    return false;
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
