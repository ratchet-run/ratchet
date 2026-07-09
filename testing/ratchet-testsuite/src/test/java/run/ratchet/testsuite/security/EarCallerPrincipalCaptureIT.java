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
package run.ratchet.testsuite.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.Testable;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.EnterpriseArchive;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.EarAlternativeMarkerBean;
import run.ratchet.testsuite.app.EarStubCallerPrincipalProvider;
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
import run.ratchet.testsuite.app.TestRatchetOptionsProducer;
import run.ratchet.testsuite.app.TestRuntimeConfig;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.DataSourceStrategy;
import run.ratchet.testsuite.util.DataSourceStrategyFactory;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.PollerControl;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import run.ratchet.testsuite.util.TestClassPolicy;

/**
 * Verifies caller-principal capture when Ratchet is deployed from EAR/lib and the application
 * overrides {@code CallerPrincipalProvider} from an EJB-jar subdeployment.
 */
class EarCallerPrincipalCaptureIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static EnterpriseArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");
    DataSourceStrategy strategy = DataSourceStrategyFactory.create();

    JavaArchive providerJar =
        ShrinkWrap.create(JavaArchive.class, "caller-principal-provider.jar")
            .addClasses(
                EarStubCallerPrincipalProvider.class,
                EarAlternativeMarkerBean.class,
                // Job target classes must be loadable from the EAR-level module classloader;
                // this mirrors the downstream EAR where job targets live in the EJB module.
                SimpleJob.class,
                TestRatchetOptionsProducer.class,
                TestRuntimeConfig.class,
                TestClassPolicy.class,
                TestEntityManagerProvider.class)
            // java:comp defaults are component-scoped; Ratchet's EAR-level startup observers run
            // with no component namespace, so this EAR uses WildFly's global concurrency bindings.
            .addAsResource(
                new StringAsset(
                    RatchetArchiveBuilder.testRuntimeConfigContent(
                        dbType,
                        Map.of(
                            "ratchet.worker.job-executor-jndi",
                            "java:jboss/ee/concurrency/executor/default",
                            "ratchet.worker.scheduled-executor-jndi",
                            "java:jboss/ee/concurrency/scheduler/default",
                            "ratchet.coordinator.thread-factory-jndi",
                            "java:jboss/ee/concurrency/factory/default"))),
                "ratchet-testsuite.properties")
            .addAsManifestResource(
                new StringAsset(
                    RatchetArchiveBuilder.persistenceXmlContent(
                        dbType, strategy.jtaDataSourceName())),
                "persistence.xml")
            .addAsManifestResource(new StringAsset(allModeBeansXml()), "beans.xml");

    WebArchive war =
        ShrinkWrap.create(WebArchive.class, "caller-principal-test.war")
            .addClasses(
                EarCallerPrincipalCaptureIT.class,
                TestJobService.class,
                BaseRatchetIT.class,
                JobAssertions.class,
                PollerControl.class,
                TestCleanupStrategy.class,
                TestDataManipulator.class,
                PerformanceTestHelper.class,
                JpaTestCleanupStrategy.class,
                JpaTestDataManipulator.class,
                JpaPerformanceTestHelper.class,
                SqlDialectTestSupportProvider.class)
            .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");

    RatchetArchiveBuilder.forArchive(war).addSqlDialectTestSupport().addDataSource(strategy);

    return ShrinkWrap.create(EnterpriseArchive.class, "caller-principal-capture.ear")
        .addAsLibraries(RatchetArchiveBuilder.ratchetDependencyFiles(profile, dbType))
        .addAsLibraries(RatchetArchiveBuilder.awaitilityFiles())
        .addAsModule(providerJar)
        .addAsModule(Testable.archiveToTest(war))
        .addAsManifestResource(
            new StringAsset(
                """
                <jboss-deployment-structure>
                  <ear-subdeployments-isolated>false</ear-subdeployments-isolated>
                </jboss-deployment-structure>
                """),
            "jboss-deployment-structure.xml");
  }

  @Test
  void scheduledJob_stampsCallerPrincipalFromEjbJarAlternativeBeforeExecution() {
    JobHandle handle = jobService.schedule(Duration.ofSeconds(2), SimpleJob::execute).submit();

    assertNotNull(handle);
    JobEntity beforeExecution =
        jobCrudStore
            .findById(handle.id())
            .orElseThrow(() -> new AssertionError("Job not found after submit"));

    assertEquals(
        EarStubCallerPrincipalProvider.STUB_PRINCIPAL,
        beforeExecution.getCallerPrincipal(),
        "Framework MUST persist the principal returned by the EJB-jar alternative");

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
}
