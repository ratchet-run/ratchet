package run.ratchet.testsuite.util;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import run.ratchet.api.JobHandle;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import java.time.Duration;

/**
 * Awaitility-based async assertions for job lifecycle verification in integration tests.
 *
 * <p>These methods poll the store until the expected state is reached or timeout occurs. Default
 * timeout is 30 seconds with 500ms polling interval.
 */
public final class JobAssertions {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration POLL_INTERVAL = Duration.ofMillis(500);

  private JobAssertions() {}

  /** Asserts that a job reaches SUCCEEDED status within the default timeout. */
  public static void assertJobCompleted(JobCrudStore store, JobHandle handle) {
    assertJobCompleted(store, handle, DEFAULT_TIMEOUT);
  }

  /** Asserts that a job reaches SUCCEEDED status within the specified timeout. */
  public static void assertJobCompleted(JobCrudStore store, JobHandle handle, Duration timeout) {
    await()
        .atMost(timeout)
        .pollInterval(POLL_INTERVAL)
        .untilAsserted(
            () -> {
              JobStatus status = store.getJobStatus(handle.id());
              if (status != JobStatus.SUCCEEDED) {
                throw new AssertionError(
                    "Expected job " + handle.id() + " to be SUCCEEDED but was " + status);
              }
            });
  }

  /** Asserts that a job reaches FAILED status within the default timeout. */
  public static void assertJobFailed(JobCrudStore store, JobHandle handle) {
    assertJobFailed(store, handle, DEFAULT_TIMEOUT);
  }

  /** Asserts that a job reaches FAILED status within the specified timeout. */
  public static void assertJobFailed(JobCrudStore store, JobHandle handle, Duration timeout) {
    await()
        .atMost(timeout)
        .pollInterval(POLL_INTERVAL)
        .untilAsserted(
            () -> {
              JobStatus status = store.getJobStatus(handle.id());
              if (status != JobStatus.FAILED) {
                throw new AssertionError(
                    "Expected job " + handle.id() + " to be FAILED but was " + status);
              }
            });
  }

  /** Asserts that a job reaches CANCELED status within the default timeout. */
  public static void assertJobCanceled(JobCrudStore store, JobHandle handle) {
    await()
        .atMost(DEFAULT_TIMEOUT)
        .pollInterval(POLL_INTERVAL)
        .untilAsserted(
            () -> {
              JobStatus status = store.getJobStatus(handle.id());
              if (status != JobStatus.CANCELED) {
                throw new AssertionError(
                    "Expected job " + handle.id() + " to be CANCELED but was " + status);
              }
            });
  }

  /**
   * Asserts that a batch parent job reaches a terminal status (SUCCEEDED or FAILED) within the
   * specified timeout. Does NOT assert success — use {@link #assertBatchSucceeded} if success is
   * required.
   */
  public static void assertBatchTerminated(
      JobCrudStore store, JobHandle batchHandle, Duration timeout) {
    await()
        .atMost(timeout)
        .pollInterval(POLL_INTERVAL)
        .untilAsserted(
            () -> {
              JobStatus status = store.getJobStatus(batchHandle.id());
              if (status != JobStatus.SUCCEEDED && status != JobStatus.FAILED) {
                throw new AssertionError(
                    "Expected batch " + batchHandle.id() + " to be completed but was " + status);
              }
            });
  }

  /**
   * Asserts that all steps in a chain completed successfully. Walks the dependsOn chain from the
   * submitted handle (first step) by following dependants until the chain of the expected length is
   * verified.
   */
  public static void assertChainCompleted(
      JobCrudStore store, JobHandle firstHandle, int chainLength, Duration timeout) {
    // Wait for the first step to complete
    assertJobCompleted(store, firstHandle, timeout);

    // Walk dependants from the first step
    long currentId = firstHandle.id();
    int stepsVerified = 1;

    while (stepsVerified < chainLength) {
      final long parentId = currentId;
      final int step = stepsVerified;

      // Wait for a dependant to appear and complete
      await()
          .atMost(timeout)
          .pollInterval(POLL_INTERVAL)
          .untilAsserted(
              () -> {
                var dependants = store.findDependants(parentId);
                if (dependants.isEmpty()) {
                  throw new AssertionError(
                      "Expected dependant for step " + step + " (job " + parentId + ") not found");
                }
                JobStatus status = dependants.get(0).getStatus();
                if (status != JobStatus.SUCCEEDED) {
                  throw new AssertionError(
                      "Chain step " + (step + 1) + " expected SUCCEEDED but was " + status);
                }
              });

      var dependants = store.findDependants(currentId);
      assertNotNull(dependants);
      assertEquals(1, dependants.size(), "Expected exactly one dependant at step " + stepsVerified);
      currentId = dependants.get(0).getId();
      stepsVerified++;
    }
  }

  /**
   * Asserts that a batch parent job reaches SUCCEEDED status within the specified timeout, meaning
   * all child items completed successfully.
   */
  public static void assertBatchSucceeded(
      JobCrudStore store, JobHandle batchHandle, Duration timeout) {
    assertJobCompleted(store, batchHandle, timeout);
  }

  /** Asserts that a job reaches a specific status within the default timeout. */
  public static void assertJobStatus(JobCrudStore store, JobHandle handle, JobStatus expected) {
    await()
        .atMost(DEFAULT_TIMEOUT)
        .pollInterval(POLL_INTERVAL)
        .untilAsserted(
            () -> {
              JobStatus status = store.getJobStatus(handle.id());
              if (status != expected) {
                throw new AssertionError(
                    "Expected job " + handle.id() + " to be " + expected + " but was " + status);
              }
            });
  }
}
