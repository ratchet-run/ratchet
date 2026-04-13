package run.ratchet.testsuite.app;

import java.time.Instant;

/** Backend-specific test data mutations not in store SPI. */
public interface TestDataManipulator {

  void setJobUpdatedAt(long jobId, Instant updatedAt);
}
