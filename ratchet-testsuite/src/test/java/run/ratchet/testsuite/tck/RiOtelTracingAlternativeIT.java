package run.ratchet.testsuite.tck;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import run.ratchet.otel.OtelTracingCollector;
import run.ratchet.spi.TracingCollector;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.io.File;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Verifies that {@link OtelTracingCollector} is selected over {@link
 * run.ratchet.ri.cdi.NoOpTracingCollector} when {@code ratchet-otel} is on the deployment
 * classpath.
 *
 * <p>{@code OtelTracingCollector} is annotated {@code @Alternative @Priority(1000)}, which
 * globally enables it per CDI 2.0+ spec without requiring a {@code beans.xml} entry. This test
 * confirms the CDI container honours the priority and selects the OTel implementation.
 *
 * <p>When no {@code OpenTelemetry} CDI bean is available (as in this test container),
 * {@code OtelTracingCollector} falls back to {@link
 * io.opentelemetry.api.GlobalOpenTelemetry#get()}, which returns a no-op instance. The test only
 * verifies selection — not span emission.
 */
@ExtendWith(ArquillianExtension.class)
class RiOtelTracingAlternativeIT {

  @Inject private TracingCollector tracingCollector;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    File[] otelJars =
        Maven.resolver()
            .loadPomFromFile("pom.xml")
            .resolve("run.ratchet:ratchet-otel")
            .withTransitivity()
            .asFile();

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addStoreInfrastructure()
        .addBeansXml()
        .build()
        .addAsLibraries(otelJars);
  }

  @Test
  void otelAlternativeSelectedOverNoOp() {
    assertInstanceOf(
        OtelTracingCollector.class,
        tracingCollector,
        "When ratchet-otel is on the deployment classpath, the @Alternative @Priority(1000) "
            + "OtelTracingCollector must be selected over the default NoOpTracingCollector");
  }
}
