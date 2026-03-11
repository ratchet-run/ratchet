package run.ratchet.testsuite.infra;

/**
 * Configuration record holding database connection details for test containers.
 *
 * @param url the JDBC URL
 * @param username the database username
 * @param password the database password
 * @param driverClass the JDBC driver class name
 * @param dbType the database type ("mysql" or "postgresql")
 */
public record DatabaseConfig(
    String url, String username, String password, String driverClass, String dbType) {}
