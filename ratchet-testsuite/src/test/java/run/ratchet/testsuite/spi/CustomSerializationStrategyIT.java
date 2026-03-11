package run.ratchet.testsuite.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import run.ratchet.spi.SerializationStrategy;
import run.ratchet.testsuite.app.CountingSerializationStrategy;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Validates that a custom {@link SerializationStrategy} alternative overrides the default. */
class CustomSerializationStrategyIT extends BaseRatchetIT {

  @Inject private SerializationStrategy strategy;

  @Inject private TestJobService jobService;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(CountingSerializationStrategy.class, TestJobService.class)
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
        .build();
  }

  @BeforeEach
  void resetCounts() {
    CountingSerializationStrategy.resetCounts();
  }

  @Test
  void customSerializationStrategy_shouldBeUsedForPayloads() {
    // Verify CDI selected the custom alternative
    assertInstanceOf(CountingSerializationStrategy.class, strategy);

    // Round-trip a payload through the custom strategy
    String payload = "test-payload-data";
    byte[] serialized = strategy.serialize(payload);
    String deserialized = strategy.deserialize(serialized, String.class);

    assertEquals(payload, deserialized);
    assertEquals(1, CountingSerializationStrategy.getSerializeCount());
    assertEquals(1, CountingSerializationStrategy.getDeserializeCount());
  }
}
