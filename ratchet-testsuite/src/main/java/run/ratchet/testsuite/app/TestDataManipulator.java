package run.ratchet.testsuite.app;

import java.time.Instant;

/**
 * Strategy for manipulating test data using backend-specific APIs.
 *
 * <p>A small number of integration tests need to modify data in ways not exposed through the store
 * SPI (e.g., backdating {@code updated_at} for retention/purge tests). This interface abstracts
 * those operations so tests remain store-agnostic.
 *
 * <p>The active implementation is determined by which class is packaged in the test WAR.
 */
public interface TestDataManipulator {

  /**
   * Sets the {@code updated_at} timestamp of a job to the given value.
   *
   * @param jobId the job ID to update
   * @param updatedAt the timestamp to set
   */
  void setJobUpdatedAt(long jobId, Instant updatedAt);
}
