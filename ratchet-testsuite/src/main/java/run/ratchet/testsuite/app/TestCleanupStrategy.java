package run.ratchet.testsuite.app;

/** Wipes scheduler data between tests. */
public interface TestCleanupStrategy {

  /** Removes all scheduler data to ensure test isolation. */
  void truncateAll();
}
