package run.ratchet.testsuite.infra;

/**
 * Configuration record holding MongoDB connection details for test containers.
 *
 * @param connectionString the MongoDB connection string (e.g., {@code mongodb://localhost:27017})
 * @param databaseName the database name to use for tests
 */
public record MongoStoreConfig(String connectionString, String databaseName) {}
