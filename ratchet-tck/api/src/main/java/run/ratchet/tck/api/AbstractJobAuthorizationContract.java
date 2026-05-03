package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.exception.JobAuthorizationException;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Base contract for {@link run.ratchet.spi.JobAuthorizationPolicy} integration.
 *
 * <p>Verifies that the runtime correctly invokes the authorization policy at job creation, and that
 * the default (permit-all) policy is backward-compatible.
 *
 * <p>Subclasses provide a {@link RatchetTckRuntime} for the default scheduler and, optionally, a
 * scheduler pre-wired with a deny-all policy (via {@link #schedulerWithDenyAllPolicy()}). Deny-all
 * tests are skipped when the optional scheduler is absent, which allows container-managed runtimes
 * that cannot swap SPIs in a test context to skip those assertions.
 */
public abstract class AbstractJobAuthorizationContract {

  /** The runtime under test (default scheduler, permit-all policy). */
  protected abstract RatchetTckRuntime runtime();

  /**
   * Returns a scheduler configured with a deny-all {@link
   * run.ratchet.spi.JobAuthorizationPolicy}, or empty if the implementation cannot wire a
   * custom policy in the test context. Subclasses backed by in-process runtimes SHOULD provide
   * this.
   *
   * <p>The returned scheduler is NOT backed by {@link RatchetTckRuntime} — it is used only to
   * verify that authorization exceptions propagate from {@code submit()}.
   */
  protected Optional<JobSchedulerService> schedulerWithDenyAllPolicy() {
    return Optional.empty();
  }

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void defaultPolicy_permitsJobCreation() {
    JobHandle handle = runtime().scheduler().enqueueNow(() -> TckJobs.noop());
    assertNotNull(handle.id(), "Job submitted with permit-all policy must return a non-null ID");
  }

  @Test
  void defaultPolicy_permitsJobCreationViaBuilder() {
    JobHandle handle = runtime().scheduler().enqueue(() -> TckJobs.noop()).submit();
    assertNotNull(
        handle.id(), "Job submitted via builder with permit-all policy must return a non-null ID");
  }

  @Test
  protected void denyAllPolicy_throwsJobAuthorizationExceptionOnSubmit() {
    Optional<JobSchedulerService> denyScheduler = schedulerWithDenyAllPolicy();
    assumeTrue(denyScheduler.isPresent(), "Deny-all policy scheduler not provided by this runtime");

    JobAuthorizationException ex =
        assertThrows(
            JobAuthorizationException.class,
            () -> denyScheduler.get().enqueueNow(() -> TckJobs.noop()),
            "submit() with a deny-all policy must throw JobAuthorizationException");

    assertEquals(
        "create",
        ex.getOperation(),
        "JobAuthorizationException must report operation='create' for job submission");
    assertNotNull(
        ex.getJobId(), "JobAuthorizationException must carry the job ID of the denied job");
  }

  @Test
  protected void denyAllPolicy_nullPrincipalIsPassedThrough() {
    Optional<JobSchedulerService> denyScheduler = schedulerWithDenyAllPolicy();
    assumeTrue(denyScheduler.isPresent(), "Deny-all policy scheduler not provided by this runtime");

    // The policy MUST receive whatever principal is present (null for system-initiated jobs)
    // without coercion. The exception is still thrown; this test just verifies no NPE occurs.
    assertThrows(
        JobAuthorizationException.class,
        () -> denyScheduler.get().enqueueNow(() -> TckJobs.noop()),
        "Deny-all policy must throw even when callerPrincipal is null");
  }
}
