package run.ratchet.testsuite.util;

import java.util.Map;
import java.util.TreeMap;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import run.ratchet.store.converter.InstantAttributeConverter;
import run.ratchet.testsuite.app.DocumentStorePerformanceTestHelper;
import run.ratchet.testsuite.app.DocumentStoreTestCleanupStrategy;
import run.ratchet.testsuite.app.DocumentStoreTestDataManipulator;
import run.ratchet.testsuite.app.JpaPerformanceTestHelper;
import run.ratchet.testsuite.app.JpaTestCleanupStrategy;
import run.ratchet.testsuite.app.JpaTestDataManipulator;
import run.ratchet.testsuite.app.PerformanceTestHelper;
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
    if (!dbType.equals("mysql") && !dbType.equals("postgresql")) {
      throw new IllegalArgumentException("Unsupported db type: " + dbType);
    }
    // MySQL needs the orm-mysql.xml override so EclipseLink (and any other non-Hibernate JPA
    // provider) routes UUID columns through UuidByteArrayConverter; Hibernate already produces
    // standard-byte-order BINARY(16), so the override is idempotent there. PostgreSQL stores UUID
    // natively and must NOT include the converter — it would re-encode native uuid as bytea.
    String mappingFile =
        dbType.equals("mysql") ? "<mapping-file>META-INF/orm-mysql.xml</mapping-file>" : "";
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
            </properties>
          </persistence-unit>
        </persistence>
        """
            .formatted(jtaDataSourceName, mappingFile);

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
        TestClassPolicy.class,
        TestCleanupStrategy.class,
        TestDataManipulator.class,
        PerformanceTestHelper.class,
        TestRatchetOptionsProducer.class,
        TestRuntimeConfig.class);

    // Store-specific classes
    switch (dbType) {
      case "mysql", "postgresql" -> {
        DataSourceStrategy strategy = DataSourceStrategyFactory.create();
        archive.addClasses(
            JpaTestCleanupStrategy.class,
            JpaTestDataManipulator.class,
            JpaPerformanceTestHelper.class,
            TestEntityManagerProvider.class,
            InstantAttributeConverter.class);
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
