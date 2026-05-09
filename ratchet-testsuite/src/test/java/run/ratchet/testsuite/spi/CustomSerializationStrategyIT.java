package run.ratchet.testsuite.spi;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.CountingPayloadSerializer;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Validates that a custom {@link PayloadSerializer} {@code @Alternative} overrides the default and
 * is actually invoked by the framework during persistence — not just resolvable as a bean.
 *
 * <p>The /dg review flagged the original test as "architectural theater" because it injected the
 * SPI and called it directly, proving only that the bean was producible. This rewrite schedules a
 * real job, waits for it to reach a terminal state, and asserts that the framework drove traffic
 * through the counting SPI during payload/result persistence.
 */
class CustomSerializationStrategyIT extends BaseRatchetIT {

  @Inject private PayloadSerializer payloadSerializer;

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(CountingPayloadSerializer.class, SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCounts() {
    CountingPayloadSerializer.resetCounts();
    SimpleJob.resetCount();
  }

  @Test
  void customPayloadSerializer_isAlternativeAndBound() {
    // The @Alternative @Priority(1) CountingPayloadSerializer MUST replace the default
    // JsonbPayloadSerializer in the CDI resolution graph for this deployment.
    assertInstanceOf(CountingPayloadSerializer.class, payloadSerializer);
  }

  @Test
  void customPayloadSerializer_isExercisedByFramework() {
    JobHandle handle = jobService.enqueueNow(SimpleJob::execute);

    assertNotNull(handle);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () ->
                CountingPayloadSerializer.getSerializeCount() > 0
                    && CountingPayloadSerializer.getDeserializeCount() > 0);

    assertTrue(
        CountingPayloadSerializer.getSerializeCount() > 0,
        "Framework MUST invoke PayloadSerializer.serialize during job persistence. "
            + "serialize="
            + CountingPayloadSerializer.getSerializeCount()
            + ", deserialize="
            + CountingPayloadSerializer.getDeserializeCount());
    assertTrue(
        CountingPayloadSerializer.getDeserializeCount() > 0,
        "Framework MUST invoke PayloadSerializer.deserialize during job execution. "
            + "serialize="
            + CountingPayloadSerializer.getSerializeCount()
            + ", deserialize="
            + CountingPayloadSerializer.getDeserializeCount());
    assertTrue(SimpleJob.getInvocationCount() > 0);
  }
}
