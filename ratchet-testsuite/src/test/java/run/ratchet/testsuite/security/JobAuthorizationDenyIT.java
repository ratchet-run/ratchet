package run.ratchet.testsuite.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.testsuite.app.DenyCreateJobAuthorizationPolicy;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;

/**
 * Verifies that a denying {@link run.ratchet.spi.JobAuthorizationPolicy} propagates {@link
 * JobAuthorizationException} to callers and prevents job persistence.
 *
 * <p>Uses {@link DenyCreateJobAuthorizationPolicy} ({@code @Alternative @Priority(1)}) to reject
 * every creation attempt, confirming that the exception bubbles through {@code enqueueNow()} and
 * that no job is persisted.
 */
class JobAuthorizationDenyIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(DenyCreateJobAuthorizationPolicy.class, SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void enqueueNow_throwsJobAuthorizationException_whenPolicyDeniesCreate() {
    assertThrows(
        JobAuthorizationException.class,
        () -> jobService.enqueueNow(SimpleJob::execute),
        "enqueueNow must propagate JobAuthorizationException when the policy denies creation");
  }

  @Test
  void deniedException_carriesOperationAndJobId() {
    JobAuthorizationException ex =
        assertThrows(
            JobAuthorizationException.class, () -> jobService.enqueueNow(SimpleJob::execute));

    assertEquals(
        "create",
        ex.getOperation(),
        "JobAuthorizationException must report operation='create' for enqueueNow");
    assertNotNull(ex.getJobId(), "JobAuthorizationException must carry the proposed job ID");
  }

  @Test
  void enqueueViaBuilder_throwsJobAuthorizationException() {
    assertThrows(
        JobAuthorizationException.class,
        () -> jobService.enqueue(SimpleJob::execute).submit(),
        "submit() via builder must propagate JobAuthorizationException when policy denies");
  }

  @Test
  void noJobsExecute_whenCreationIsDenied() {
    SimpleJob.resetCount();

    assertThrows(JobAuthorizationException.class, () -> jobService.enqueueNow(SimpleJob::execute));

    // No task body should ever run since the job was never persisted
    assertEquals(
        0,
        SimpleJob.getInvocationCount(),
        "Job task body must not execute when creation is denied by policy");
  }
}
