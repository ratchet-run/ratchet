package run.ratchet.testsuite.chain;

import static org.junit.jupiter.api.Assertions.assertEquals;

import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.ChainStepTracker;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Validates linear chain execution: A → B → C in order. */
class LinearChainIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(ChainStepTracker.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    ChainStepTracker.reset();
  }

  @Test
  void linearChain_shouldExecuteStepsInOrder() {
    JobHandle handle =
        jobService
            .enqueue(ChainStepTracker::stepA)
            .then(ChainStepTracker::stepB)
            .then(ChainStepTracker::stepC)
            .submit();

    JobAssertions.assertChainCompleted(jobCrudStore, handle, 3, Duration.ofSeconds(30));

    List<String> order = ChainStepTracker.executionOrder();
    assertEquals(List.of("A", "B", "C"), order, "Chain steps should execute in order");
  }
}
