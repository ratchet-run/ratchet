package run.ratchet.testsuite.app;

/**
 * Strategy for cleaning up scheduler data between integration tests.
 *
 * <p>Each store backend provides an implementation that clears all scheduler data using
 * backend-specific APIs (SQL TRUNCATE for JPA stores, deleteMany for MongoDB, etc.). The active
 * implementation is determined by which class is packaged in the test WAR — controlled by {@code
 * RatchetArchiveBuilder.addStoreInfrastructure()}.
 */
public interface TestCleanupStrategy {

  /** Removes all scheduler data to ensure test isolation. */
  void truncateAll();
}
