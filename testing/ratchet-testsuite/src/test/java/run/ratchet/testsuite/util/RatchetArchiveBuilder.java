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
package run.ratchet.testsuite.util;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import run.ratchet.store.converter.InstantAttributeConverter;
import run.ratchet.tck.store.SqlDialectTestSupport;
import run.ratchet.testsuite.app.DocumentStorePerformanceTestHelper;
import run.ratchet.testsuite.app.DocumentStoreTestCleanupStrategy;
import run.ratchet.testsuite.app.DocumentStoreTestDataManipulator;
import run.ratchet.testsuite.app.JpaPerformanceTestHelper;
import run.ratchet.testsuite.app.JpaTestCleanupStrategy;
import run.ratchet.testsuite.app.JpaTestDataManipulator;
import run.ratchet.testsuite.app.PerformanceTestHelper;
import run.ratchet.testsuite.app.SqlDialectTestSupportProvider;
import run.ratchet.testsuite.app.TestCleanupStrategy;
import run.ratchet.testsuite.app.TestDataManipulator;
import run.ratchet.testsuite.app.TestEntityManagerProvider;
import run.ratchet.testsuite.app.TestMongoProducer;
import run.ratchet.testsuite.app.TestRatchetOptionsProducer;
import run.ratchet.testsuite.app.TestRuntimeConfig;
import run.ratchet.testsuite.infra.JdbcContainerExtension;
import run.ratchet.testsuite.infra.JdbcDatabaseConfig;

/**
 * Fluent ShrinkWrap builder for Ratchet integration test deployments.
 *
 * <p>Uses Maven resolver to import compile and runtime dependencies from the testsuite POM. The
 * active Maven profile determines which store module and drivers are included.
 */
public class RatchetArchiveBuilder {

  private final WebArchive archive;

  private RatchetArchiveBuilder() {
    archive = ShrinkWrap.create(WebArchive.class, "ratchet-test.war");
  }

  public static RatchetArchiveBuilder create() {
    return new RatchetArchiveBuilder();
  }

  public RatchetArchiveBuilder addRatchetDependencies(String... profiles) {
    archive.addAsLibraries(
        Maven.configureResolver()
            .loadPomFromFile("pom.xml", profiles)
            .importCompileAndRuntimeDependencies()
            .resolve()
            .withTransitivity()
            .asFile());
    return this;
  }

  public RatchetArchiveBuilder addClasses(Class<?>... classes) {
    archive.addClasses(classes);
    return this;
  }

  public RatchetArchiveBuilder addPackage(Package pkg) {
    archive.addPackage(pkg);
    return this;
  }

  public RatchetArchiveBuilder addBeansXml() {
    archive.addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
    return this;
  }

  public RatchetArchiveBuilder addPersistenceXml(String dbType, String jtaDataSourceName) {
    if (!dbType.equals("mysql")
        && !dbType.equals("postgresql")
        && !dbType.equals("oracle")
        && !dbType.equals("sqlserver")) {
      throw new IllegalArgumentException("Unsupported db type: " + dbType);
    }
    // MySQL and SQL Server store UUIDs in BINARY(16) and Oracle in RAW(16). EclipseLink's default
    // would send the 36-char hyphenated string and overflow/mistype the column, so EclipseLink
    // deployments route every UUID through a converter via orm-mysql.xml / orm-oracle.xml /
    // orm-sqlserver.xml. The WildFly cells run Hibernate (6.6 on wildfly-managed, 7 on
    // wildfly-ee11-managed); Hibernate maps UUID natively and Hibernate 7 rejects an
    // AttributeConverter on an @Id outright, so those cells take no UUID mapping file — on MySQL
    // the
    // native byte order is already canonical, on SQL Server the preferred_uuid_jdbc_type property
    // below forces it canonical, and on Oracle the native UUID maps to RAW(16). PostgreSQL stores
    // UUID natively and must NOT include the converter — it would re-encode native uuid as bytea.
    String launch = System.getProperty("arquillian.launch", "");
    boolean hibernate = launch.startsWith("wildfly");
    boolean hibernate7 = "wildfly-ee11-managed".equals(launch);
    String mappingFile = "";
    if (dbType.equals("mysql") && !hibernate7) {
      mappingFile = "<mapping-file>META-INF/orm-mysql.xml</mapping-file>";
    } else if (dbType.equals("oracle") && !hibernate7) {
      mappingFile = "<mapping-file>META-INF/orm-oracle.xml</mapping-file>";
    } else if (dbType.equals("sqlserver") && !hibernate) {
      mappingFile = "<mapping-file>META-INF/orm-sqlserver.xml</mapping-file>";
    }
    // Per-dialect Hibernate tuning (no-op under EclipseLink). Oracle: map entity Instant to plain
    // TIMESTAMP rather than Hibernate's default TIMESTAMP WITH TIME ZONE (ORA-18716 on the plain
    // TIMESTAMP columns), and on Hibernate 7 — where orm-oracle.xml is omitted — auto-quote the
    // LEVEL reserved word so scheduler_job_log agrees with the shipped DDL. SQL Server: force the
    // BINARY JDBC type for UUID so Hibernate writes canonical big-endian bytes into BINARY(16)
    // instead of the uniqueidentifier mixed-endian layout, matching UuidByteArrayConverter.toBytes.
    String dialectProps;
    if (dbType.equals("oracle")) {
      dialectProps =
          """
                  <property name="hibernate.type.preferred_instant_jdbc_type" value="TIMESTAMP"/>
                  <property name="hibernate.jdbc.time_zone" value="UTC"/>
                  <property name="hibernate.auto_quote_keyword" value="true"/>
            """;
    } else if (dbType.equals("sqlserver")) {
      dialectProps =
          """
                  <property name="hibernate.type.preferred_uuid_jdbc_type" value="BINARY"/>
            """;
    } else {
      dialectProps = "";
    }
    // No <provider> or hibernate.dialect pin — WildFly auto-discovers via ServiceLoader and the
    // JPA provider auto-detects the dialect from the JDBC URL exposed by RatchetDS. Remaining
    // property keys are opt-in Hibernate tuning and no-op under any other JPA provider.
    // language=XML
    String persistenceXml =
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <persistence xmlns="https://jakarta.ee/xml/ns/persistence"
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xsi:schemaLocation="https://jakarta.ee/xml/ns/persistence
                         https://jakarta.ee/xml/ns/persistence/persistence_3_0.xsd"
                     version="3.0">
          <persistence-unit name="ratchet-test" transaction-type="JTA">
            <jta-data-source>%s</jta-data-source>
            %s
            <class>run.ratchet.store.entity.JobEntity</class>
            <class>run.ratchet.store.entity.JobExecutionEntity</class>
            <class>run.ratchet.store.entity.ResourceLimitEntity</class>
            <class>run.ratchet.store.entity.BatchMetricsEntity</class>
            <class>run.ratchet.store.entity.WorkflowConditionEntity</class>
            <class>run.ratchet.store.entity.ArchivedJobEntity</class>
            <class>run.ratchet.store.entity.NodeEntity</class>
            <class>run.ratchet.store.entity.DlqAlertEntity</class>
            <class>run.ratchet.store.entity.JobLogEntity</class>
            <class>run.ratchet.store.entity.ResourcePermitEntity</class>
            <class>run.ratchet.store.entity.BatchEntity</class>
            <class>run.ratchet.store.converter.InstantAttributeConverter</class>
            <class>run.ratchet.store.converter.JobPayloadConverter</class>
            <class>run.ratchet.store.converter.JsonListConverter</class>
            <class>run.ratchet.store.converter.JsonMapConverter</class>
            <class>run.ratchet.store.converter.JsonObjectMapConverter</class>
            <exclude-unlisted-classes>true</exclude-unlisted-classes>
            <properties>
              <property name="hibernate.hbm2ddl.auto" value="none"/>
              <property name="hibernate.show_sql" value="true"/>
              <property name="hibernate.connection.isolation" value="2"/>
        %s    </properties>
          </persistence-unit>
        </persistence>
        """
            .formatted(jtaDataSourceName, mappingFile, dialectProps);

    archive.addAsResource(new StringAsset(persistenceXml), "META-INF/persistence.xml");
    return this;
  }

  public RatchetArchiveBuilder addDataSource(DataSourceStrategy strategy) {
    JdbcDatabaseConfig config = JdbcContainerExtension.getConfig();
    strategy.configureArchive(archive, config);
    return this;
  }

  public RatchetArchiveBuilder addResource(String resource, String target) {
    archive.addAsResource(new StringAsset(resource), target);
    return this;
  }

  public RatchetArchiveBuilder addStoreInfrastructure() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");

    // Common classes always included.
    // Production DefaultExecutorProvider (JNDI inject + lookup fallback) is the only executor
    // provider in test deployments — no test override needed.
    archive.addClasses(
        BaseRatchetIT.class,
        JobAssertions.class,
        PollerControl.class,
        TestClassPolicy.class,
        TestCleanupStrategy.class,
        TestDataManipulator.class,
        PerformanceTestHelper.class,
        TestRatchetOptionsProducer.class,
        TestRuntimeConfig.class);

    // Store-specific classes
    switch (dbType) {
      case "mysql", "postgresql", "oracle", "sqlserver" -> {
        DataSourceStrategy strategy = DataSourceStrategyFactory.create();
        archive.addClasses(
            JpaTestCleanupStrategy.class,
            JpaTestDataManipulator.class,
            JpaPerformanceTestHelper.class,
            SqlDialectTestSupportProvider.class,
            TestEntityManagerProvider.class,
            InstantAttributeConverter.class);
        addSqlDialectTestSupport();
        addPersistenceXml(dbType, strategy.jtaDataSourceName());
        addDataSource(strategy);
      }
      case "mongodb" -> {
        archive.addClasses(
            DocumentStoreTestCleanupStrategy.class,
            DocumentStoreTestDataManipulator.class,
            DocumentStorePerformanceTestHelper.class,
            TestMongoProducer.class);
      }
      default -> throw new IllegalArgumentException("Unsupported db type: " + dbType);
    }

    addTestRuntimeConfig(dbType);
    addAwaitility();
    return this;
  }

  public WebArchive build() {
    return archive;
  }

  /**
   * Bundles the active store's {@link SqlDialectTestSupport} into the WAR. Exactly one store is on
   * the test classpath (profile-gated test-jar dependency), so {@link ServiceLoader} resolves a
   * single implementation here in the build JVM. ShrinkWrap adds named classes only, so the
   * interface, the implementation (plus any nested types it declares), and a regenerated service
   * registration are all added explicitly for the in-container lookup that {@code
   * SqlDialectTestSupportProvider} performs at runtime.
   */
  private void addSqlDialectTestSupport() {
    // SqlDialectTestSupportProvider already resolves (and caches) the single implementation via the
    // same ServiceLoader lookup in this build JVM, so reuse it rather than scanning a second time.
    Class<?> implClass = SqlDialectTestSupportProvider.get().getClass();
    archive.addClass(SqlDialectTestSupport.class);
    archive.addClass(implClass);
    archive.addClasses(implClass.getDeclaredClasses());
    archive.addAsResource(
        new StringAsset(implClass.getName()),
        "META-INF/services/" + SqlDialectTestSupport.class.getName());
  }

  private void addAwaitility() {
    // Awaitility is needed in-container by JobAssertions but is test-scoped
    // (not pulled in by importCompileAndRuntimeDependencies)
    archive.addAsLibraries(
        Maven.configureResolver()
            .loadPomFromFile("pom.xml")
            .resolve("org.awaitility:awaitility")
            .withTransitivity()
            .asFile());
  }

  private void addTestRuntimeConfig(String dbType) {
    Map<String, String> properties = new TreeMap<>();
    put(properties, "ratchet.test.db.type", dbType);
    putIfPresent(properties, "ratchet.test.db.url");
    putIfPresent(properties, "ratchet.test.db.username");
    putIfPresent(properties, "ratchet.test.db.password");
    putIfPresent(properties, "ratchet.test.db.driver");
    putIfPresent(properties, "ratchet.test.db.driver.name");
    putIfPresent(properties, "ratchet.test.mongo.uri");
    putIfPresent(properties, "ratchet.test.mongo.database");

    put(properties, "ratchet.poller.deep-idle-threshold-ms", "5000");
    put(properties, "ratchet.poller.deep-idle-delay-ms", "2000");
    put(properties, "ratchet.poller.max-delay-ms", "2000");
    // Arquillian redeploys test archives rapidly; a long heartbeat cadence keeps the initial
    // liveness row without leaving periodic managed-executor work racing application undeploy.
    put(properties, "ratchet.node.dynamic-heartbeat-enabled", "false");
    put(properties, "ratchet.node.heartbeat-interval-seconds", "300");

    StringBuilder content = new StringBuilder();
    properties.forEach(
        (key, value) ->
            content
                .append(escapeProperty(key))
                .append('=')
                .append(escapeProperty(value))
                .append('\n'));
    archive.addAsResource(new StringAsset(content.toString()), "ratchet-testsuite.properties");
  }

  private static void putIfPresent(Map<String, String> properties, String key) {
    String value = System.getProperty(key);
    if (value != null && !value.isBlank()) {
      put(properties, key, value);
    }
  }

  private static void put(Map<String, String> properties, String key, String value) {
    properties.put(key, value);
  }

  private static String escapeProperty(String value) {
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
  }
}
