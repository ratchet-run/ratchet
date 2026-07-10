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
import run.ratchet.testsuite.app.DocumentStorePerformanceTestHelper;
import run.ratchet.testsuite.app.DocumentStoreTestCleanupStrategy;
import run.ratchet.testsuite.app.DocumentStoreTestDataManipulator;
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
 * Verifies caller-principal capture when Ratchet is deployed from EAR/lib and the application
 * overrides {@code CallerPrincipalProvider} with an {@code @Alternative} packaged in EAR/lib
 * alongside Ratchet.
 */
class EarCallerPrincipalCaptureIT extends BaseRatchetIT {

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

    JavaArchive supportJar =
        ShrinkWrap.create(JavaArchive.class, "caller-principal-support.jar")
            .addClasses(
                EarStubCallerPrincipalProvider.class,
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
          ShrinkWrap.create(JavaArchive.class, "caller-principal-pu.jar")
              .addAsManifestResource(
                  new StringAsset(
                      RatchetArchiveBuilder.persistenceXmlContent(
                          dbType, strategy.jtaDataSourceName())),
                  "persistence.xml");
    } else {
      supportJar.addClass(TestMongoProducer.class);
    }

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
        ShrinkWrap.create(EnterpriseArchive.class, "caller-principal-capture.ear")
            .addAsLibraries(RatchetArchiveBuilder.ratchetDependencyFiles(profile, dbType))
            .addAsLibraries(RatchetArchiveBuilder.awaitilityFiles())
            .addAsLibraries(supportJar)
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

  @Test
  void scheduledJob_stampsCallerPrincipalFromEarLibAlternativeBeforeExecution() {
    JobHandle handle = jobService.schedule(Duration.ofSeconds(2), SimpleJob::execute).submit();

    assertNotNull(handle);
    JobEntity beforeExecution =
        jobCrudStore
            .findById(handle.id())
            .orElseThrow(() -> new AssertionError("Job not found after submit"));

    assertEquals(
        EarStubCallerPrincipalProvider.STUB_PRINCIPAL,
        beforeExecution.getCallerPrincipal(),
        "Framework MUST persist the principal returned by the EAR/lib alternative");

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
              <web-uri>caller-principal-test.war</web-uri>
              <context-root>/caller-principal-test</context-root>
            </web>
          </module>
        </application>
        """;
  }
}
