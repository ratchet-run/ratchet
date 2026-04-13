package run.ratchet.testsuite.infra;

/** Configuration record holding database connection details for test containers. */
public record JdbcDatabaseConfig(
    String url, String username, String password, String driverClass, String dbType) {}
