package run.ratchet.testsuite.tck;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import jakarta.inject.Inject;
import java.io.File;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.micrometer.MicrometerMetricsCollector;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;
import run.ratchet.tck.jakarta.AbstractTxEnqueueContract;
import run.ratchet.tck.util.ConcurrentTestRunner;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Verifies that {@link MicrometerMetricsCollector} is selected over {@link
 * run.ratchet.ri.cdi.NoOpMetricsCollector} when {@code ratchet-micrometer} is on the deployment
 * classpath.
 *
 * <p>{@code MicrometerMetricsCollector} is annotated {@code @Alternative @Priority(1000)}, which
 * globally enables it per CDI 2.0+ spec without requiring a {@code beans.xml} entry. This test
 * confirms that the CDI container honours the priority and selects the Micrometer implementation.
 * {@code ratchet-micrometer} also ships {@code MicrometerMeterRegistryProducer}, which provides the
 * {@code SimpleMeterRegistry} needed to satisfy {@code MicrometerMetricsCollector}'s constructor
 * injection — no additional test producer is required.
 */
@ExtendWith(ArquillianExtension.class)
class RiMicrometerAlternativeIT {

  @Inject private MetricsCollector metricsCollector;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    // Resolve ratchet-micrometer and its transitive deps (micrometer-core) separately so they
    // can be layered onto a standard ratchet deployment. Other ITs must not include micrometer
    // so they exercise the NoOp default path.
    File[] micrometerJars =
        Maven.resolver()
            .loadPomFromFile("pom.xml")
            .resolve("run.ratchet:ratchet-micrometer")
            .withTransitivity()
            .asFile();

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addPackage(RatchetTckRuntime.class.getPackage())
        .addPackage(AbstractTxEnqueueContract.class.getPackage())
        .addPackage(ConcurrentTestRunner.class.getPackage())
        .addClasses(RiRatchetTckRuntime.class, ListenerProbe.class, TckJobs.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build()
        .addAsLibraries(micrometerJars);
  }

  @Test
  void micrometerAlternativeSelectedOverNoOp() {
    assertInstanceOf(
        MicrometerMetricsCollector.class,
        metricsCollector,
        "When ratchet-micrometer is on the deployment classpath, the @Alternative @Priority(1000) "
            + "MicrometerMetricsCollector must be selected over the default NoOpMetricsCollector");
  }
}
