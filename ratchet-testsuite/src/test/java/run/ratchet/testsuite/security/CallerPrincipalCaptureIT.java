package run.ratchet.testsuite.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.StubCallerPrincipalProvider;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Verifies that the framework stamps the captured caller principal onto every persisted {@link
 * JobEntity} at creation.
 *
 * <p>This deployment does NOT configure a real security realm — a realm-configured Arquillian test
 * is deferred follow-up work. Instead, a {@link StubCallerPrincipalProvider}
 * {@code @Alternative @Priority(1)} replaces the default {@link CallerPrincipalProvider} in the CDI
 * graph and returns a fixed principal, proving the framework invokes the provider during job
 * creation and persists the returned value.
 */
class CallerPrincipalCaptureIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(StubCallerPrincipalProvider.class, SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void enqueueNow_stampsCallerPrincipalFromProvider() {
    JobHandle handle = jobService.enqueueNow(SimpleJob::execute);

    assertNotNull(handle);
    // Wait for the job to complete so the persisted row is definitely flushed; the caller
    // principal is stamped at creation and must not be overwritten by execution.
    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    JobEntity reloaded =
        jobCrudStore
            .findById(handle.id())
            .orElseThrow(() -> new AssertionError("Job not found after completion"));

    assertEquals(
        StubCallerPrincipalProvider.STUB_PRINCIPAL,
        reloaded.getCallerPrincipal(),
        "Framework MUST persist the principal returned by CallerPrincipalProvider at job creation");
  }
}
