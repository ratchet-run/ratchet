/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.testsuite.ear;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.Testable;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.EnterpriseArchive;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.DocumentStorePerformanceTestHelper;
import run.ratchet.testsuite.app.DocumentStoreTestCleanupStrategy;
import run.ratchet.testsuite.app.DocumentStoreTestDataManipulator;
import run.ratchet.testsuite.app.EjbModuleCallerPrincipalProvider;
import run.ratchet.testsuite.app.EjbModuleMarkerBean;
import run.ratchet.testsuite.app.JpaPerformanceTestHelper;
import run.ratchet.testsuite.app.JpaTestCleanupStrategy;
import run.ratchet.testsuite.app.JpaTestDataManipulator;
import run.ratchet.testsuite.app.PerformanceTestHelper;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.SqlDialectTestSupportProvider;
import run.ratchet.testsuite.app.TestCleanupStrategy;
import run.ratchet.testsuite.app.TestDataManipulator;
import run.ratchet.testsuite.app.TestEntityManagerProvider;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TestMongoProducer;
import run.ratchet.testsuite.app.TestRatchetOptionsProducer;
import run.ratchet.testsuite.app.TestRuntimeConfig;
import run.ratchet.testsuite.infra.JdbcContainerExtension;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.DataSourceStrategy;
import run.ratchet.testsuite.util.DataSourceStrategyFactory;
import run.ratchet.testsuite.util.GlassFishDataSourceStrategy;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.PayaraDataSourceStrategy;
import run.ratchet.testsuite.util.PollerControl;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import run.ratchet.testsuite.util.TestClassPolicy;

/**
 * Documents that a {@code CallerPrincipalProvider} {@code @Alternative} override packaged in a
 * separate EJB-jar subdeployment — mirroring the {@code nets-ejb} module — rather than alongside
 * Ratchet in {@code EAR/lib}, is honored only on WildFly-family servers and Open Liberty, and NOT
 * on Payara or GlassFish.
 *
 * <p>The EAR deploys and the job reaches {@code COMPLETED} identically on all five managed servers;
 * only visibility of the subdeployment {@code @Alternative} differs:
 *
 * <ul>
 *   <li>Honored ({@code JobEntity.getCallerPrincipal()} equals {@link
 *       EjbModuleCallerPrincipalProvider#STUB_PRINCIPAL}): {@code wildfly-managed}, {@code
 *       wildfly-ee11-managed}, {@code openliberty-managed}.
 *   <li>Not honored ({@code JobEntity.getCallerPrincipal()} is {@code null} — the default
 *       provider's result in this unauthenticated test context): {@code payara-managed}, {@code
 *       glassfish-managed}.
 * </ul>
 *
 * <p>This is spec-compliant, not a Ratchet defect: a subdeployment {@code @Alternative} is not
 * guaranteed visible to Ratchet's EAR/lib injection points, and Payara/GlassFish's stricter Weld
 * resolution does not expose it there. Portable code should either package {@code
 * CallerPrincipalProvider} overrides in {@code EAR/lib} alongside Ratchet — see {@link
 * run.ratchet.testsuite.security.EarCallerPrincipalCaptureIT}, green on all servers — or use {@code
 * RatchetOptions.withCallerPrincipalResolver} / the {@code CallerPrincipalResolver} seam, which
 * bypasses CDI {@code @Alternative} resolution entirely.
 */
class EarEjbSubmoduleOverrideIT extends BaseRatchetIT {

  /**
   * Server profiles on which a subdeployment {@code @Alternative} override is empirically honored.
   * Measured against a live 5-server run (mysql); see class Javadoc.
   */
  private static final Set<String> SUBMODULE_ALTERNATIVE_HONORING_PROFILES =
      Set.of("wildfly-managed", "wildfly-ee11-managed", "openliberty-managed");

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static EnterpriseArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");
    DataSourceStrategy strategy = DataSourceStrategyFactory.create();
    boolean sql = !"mongodb".equals(dbType);
    boolean earLevelDataSource =
        sql
            && (strategy instanceof PayaraDataSourceStrategy
                || strategy instanceof GlassFishDataSourceStrategy);

    Map<String, String> executorOverrides =
        profile.startsWith("wildfly")
            ? Map.of(
                "ratchet.worker.job-executor-jndi",
                "java:jboss/ee/concurrency/executor/default",
                "ratchet.worker.scheduled-executor-jndi",
                "java:jboss/ee/concurrency/scheduler/default",
                "ratchet.coordinator.thread-factory-jndi",
                "java:jboss/ee/concurrency/factory/default")
            : Map.of();

    // EAR/lib support jar carries only the config producers and runtime props. The
    // CallerPrincipalProvider override deliberately does NOT live here — it is isolated in the
    // ejb-jar subdeployment below.
    JavaArchive supportJar =
        ShrinkWrap.create(JavaArchive.class, "ejb-submodule-support.jar")
            .addClasses(
                SimpleJob.class,
                TestRatchetOptionsProducer.class,
                TestRuntimeConfig.class,
                TestClassPolicy.class)
            // Ratchet's EAR-level startup observers run outside any component namespace. On
            // WildFly, java:comp/* therefore does not resolve, so the global
            // java:jboss/ee/concurrency/* bindings are required. Other Jakarta EE 10+ containers
            // (Payara, GlassFish, and Open Liberty) bind the portable
            // java:comp/DefaultManagedExecutorService and
            // java:comp/DefaultManagedScheduledExecutorService names even for EAR-level observers,
            // so an empty override map lets DefaultExecutorProvider use the RI defaults there.
            .addAsResource(
                new StringAsset(
                    RatchetArchiveBuilder.testRuntimeConfigContent(dbType, executorOverrides)),
                "ratchet-testsuite.properties")
            .addAsManifestResource(new StringAsset(allModeBeansXml()), "beans.xml");

    JavaArchive persistenceUnitJar = null;
    if (sql) {
      supportJar.addClass(TestEntityManagerProvider.class);
      persistenceUnitJar =
          ShrinkWrap.create(JavaArchive.class, "ejb-submodule-pu.jar")
              .addAsManifestResource(
                  new StringAsset(
                      RatchetArchiveBuilder.persistenceXmlContent(
                          dbType, strategy.jtaDataSourceName())),
                  "persistence.xml");
    } else {
      supportJar.addClass(TestMongoProducer.class);
    }

    // The override lives ONLY in this EJB-jar subdeployment, never in EAR/lib. A trivial
    // @Singleton bean makes the jar a genuine ejb-jar, mirroring the nets-ejb module.
    JavaArchive overrideEjbJar =
        ShrinkWrap.create(JavaArchive.class, "override-ejb.jar")
            .addClasses(EjbModuleCallerPrincipalProvider.class, EjbModuleMarkerBean.class)
            .addAsManifestResource(new StringAsset(allModeBeansXml()), "beans.xml");

    WebArchive war =
        ShrinkWrap.create(WebArchive.class, "ejb-submodule-test.war")
            .addClasses(
                EarEjbSubmoduleOverrideIT.class,
                TestJobService.class,
                BaseRatchetIT.class,
                JobAssertions.class,
                PollerControl.class,
                TestCleanupStrategy.class,
                TestDataManipulator.class,
                PerformanceTestHelper.class)
            .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");

    if (sql) {
      war.addClasses(
          JpaTestCleanupStrategy.class,
          JpaTestDataManipulator.class,
          JpaPerformanceTestHelper.class,
          SqlDialectTestSupportProvider.class);
      RatchetArchiveBuilder warBuilder =
          RatchetArchiveBuilder.forArchive(war).addSqlDialectTestSupport();
      if (!earLevelDataSource) {
        warBuilder.addDataSource(strategy);
      }
    } else {
      war.addClasses(
          DocumentStoreTestCleanupStrategy.class,
          DocumentStoreTestDataManipulator.class,
          DocumentStorePerformanceTestHelper.class);
    }

    EnterpriseArchive ear =
        ShrinkWrap.create(EnterpriseArchive.class, "ejb-submodule-override.ear")
            .addAsLibraries(RatchetArchiveBuilder.ratchetDependencyFiles(profile, dbType))
            .addAsLibraries(RatchetArchiveBuilder.awaitilityFiles())
            .addAsLibraries(supportJar)
            .addAsModule(overrideEjbJar)
            .addAsModule(Testable.archiveToTest(war))
            .addAsManifestResource(new StringAsset(applicationXmlContent()), "application.xml");

    if (persistenceUnitJar != null) {
      ear.addAsLibraries(persistenceUnitJar);
    }

    if (earLevelDataSource) {
      strategy.configureEnterpriseArchive(ear, JdbcContainerExtension.getConfig());
    }

    return ear;
  }

  @BeforeEach
  void resetSimpleJobCount() {
    SimpleJob.resetCount();
  }

  private static boolean honorsSubmoduleAlternative() {
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");
    return SUBMODULE_ALTERNATIVE_HONORING_PROFILES.contains(profile);
  }

  @Test
  void scheduledJob_honorsEjbSubmoduleAlternativeOnlySupportedServers() {
    boolean honored = honorsSubmoduleAlternative();

    JobHandle handle = jobService.schedule(Duration.ofSeconds(2), SimpleJob::execute).submit();

    assertNotNull(handle);
    JobEntity beforeExecution =
        jobCrudStore
            .findById(handle.id())
            .orElseThrow(() -> new AssertionError("Job not found after submit"));

    if (honored) {
      assertEquals(
          EjbModuleCallerPrincipalProvider.STUB_PRINCIPAL,
          beforeExecution.getCallerPrincipal(),
          "Framework MUST persist the principal returned by the EJB-jar subdeployment "
              + "alternative on servers that honor it");
    } else {
      assertNull(
          beforeExecution.getCallerPrincipal(),
          "Server does not honor a subdeployment @Alternative; the default provider must yield "
              + "no principal in this unauthenticated test context");
    }

    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    JobEntity afterExecution =
        jobCrudStore
            .findById(handle.id())
            .orElseThrow(() -> new AssertionError("Job not found after completion"));

    assertEquals(
        beforeExecution.getCallerPrincipal(),
        afterExecution.getCallerPrincipal(),
        "Execution must not overwrite the caller principal stamped at job creation");
  }

  private static String allModeBeansXml() {
    return """
        <beans xmlns="https://jakarta.ee/xml/ns/jakartaee"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xsi:schemaLocation="https://jakarta.ee/xml/ns/jakartaee
                   https://jakarta.ee/xml/ns/jakartaee/beans_4_0.xsd"
               version="4.0"
               bean-discovery-mode="all">
        </beans>
        """;
  }

  private static String applicationXmlContent() {
    return """
        <?xml version="1.0" encoding="UTF-8"?>
        <application xmlns="https://jakarta.ee/xml/ns/jakartaee"
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xsi:schemaLocation="https://jakarta.ee/xml/ns/jakartaee
                         https://jakarta.ee/xml/ns/jakartaee/application_10.xsd"
                     version="10">
          <module>
            <web>
              <web-uri>ejb-submodule-test.war</web-uri>
              <context-root>/ejb-submodule-test</context-root>
            </web>
          </module>
          <module>
            <ejb>override-ejb.jar</ejb>
          </module>
        </application>
        """;
  }
}
