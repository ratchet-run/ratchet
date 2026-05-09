package run.ratchet.testsuite.app;

import java.time.Instant;
import java.util.UUID;

/** Backend-specific test data mutations not in store SPI. */
public interface TestDataManipulator {

  void setJobUpdatedAt(UUID jobId, Instant updatedAt);

  void setArchivedAt(UUID archiveId, Instant archivedAt);
}
