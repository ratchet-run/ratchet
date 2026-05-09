package run.ratchet.testsuite.util;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
import java.util.UUID;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobCrudStore;

/** Awaitility-based assertions that poll the store until a job reaches the expected status. */
public final class JobAssertions {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration POLL_INTERVAL = Duration.ofMillis(500);

  private JobAssertions() {}

  public static void assertJobCompleted(JobCrudStore store, JobHandle handle) {
    assertJobCompleted(store, handle, DEFAULT_TIMEOUT);
  }

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

  public static void assertJobFailed(JobCrudStore store, JobHandle handle) {
    assertJobFailed(store, handle, DEFAULT_TIMEOUT);
  }

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

  public static void assertChainCompleted(
      JobCrudStore store, JobHandle firstHandle, int chainLength, Duration timeout) {
    // Wait for the first step to complete
    assertJobCompleted(store, firstHandle, timeout);

    // Walk dependants from the first step
    UUID currentId = firstHandle.id();
    int stepsVerified = 1;

    while (stepsVerified < chainLength) {
      final UUID parentId = currentId;
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
   * Waits until the batch root job reaches {@link JobStatus#SUCCEEDED}.
   *
   * <p>This assertion intentionally checks only the batch handle passed by the caller. It does not
   * traverse or verify child jobs; callers that need child-level guarantees should assert those
   * jobs explicitly.
   */
  public static void assertBatchSucceeded(
      JobCrudStore store, JobHandle batchHandle, Duration timeout) {
    assertJobCompleted(store, batchHandle, timeout);
  }

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
