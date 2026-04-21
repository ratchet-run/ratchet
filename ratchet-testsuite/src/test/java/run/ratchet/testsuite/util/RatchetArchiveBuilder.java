package run.ratchet.testsuite.util;

import run.ratchet.testsuite.app.DocumentStorePerformanceTestHelper;
import run.ratchet.testsuite.app.DocumentStoreTestCleanupStrategy;
import run.ratchet.testsuite.app.DocumentStoreTestDataManipulator;
import run.ratchet.testsuite.app.JpaPerformanceTestHelper;
import run.ratchet.testsuite.app.JpaTestCleanupStrategy;
import run.ratchet.testsuite.app.JpaTestDataManipulator;
import run.ratchet.testsuite.app.PerformanceTestHelper;
import run.ratchet.testsuite.app.TestCleanupStrategy;
import run.ratchet.testsuite.app.TestDataManipulator;
import run.ratchet.testsuite.app.TestMongoProducer;
import run.ratchet.testsuite.app.TestRatchetOptionsProducer;
import run.ratchet.testsuite.infra.JdbcContainerExtension;
import run.ratchet.testsuite.infra.JdbcDatabaseConfig;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;

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

  public RatchetArchiveBuilder addPersistenceXml(String dbType) {
    if (!dbType.equals("mysql") && !dbType.equals("postgresql")) {
      throw new IllegalArgumentException("Unsupported db type: " + dbType);
    }
    // No <provider> or hibernate.dialect pin — WildFly auto-discovers via ServiceLoader and the
    // JPA provider auto-detects the dialect from the JDBC URL exposed by RatchetDS. Remaining
    // property keys are opt-in Hibernate tuning and no-op under any other JPA provider.
    String persistenceXml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<persistence xmlns=\"https://jakarta.ee/xml/ns/persistence\"\n"
            + "             xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "             xsi:schemaLocation=\"https://jakarta.ee/xml/ns/persistence\n"
            + "                 https://jakarta.ee/xml/ns/persistence/persistence_3_0.xsd\"\n"
            + "             version=\"3.0\">\n"
            + "  <persistence-unit name=\"ratchet-test\" transaction-type=\"JTA\">\n"
            + "    <jta-data-source>java:jboss/datasources/RatchetDS</jta-data-source>\n"
            + "    <class>run.ratchet.store.entity.JobEntity</class>\n"
            + "    <class>run.ratchet.store.entity.JobExecutionEntity</class>\n"
            + "    <class>run.ratchet.store.entity.ResourceLimitEntity</class>\n"
            + "    <class>run.ratchet.store.entity.BatchMetricsEntity</class>\n"
            + "    <class>run.ratchet.store.entity.WorkflowConditionEntity</class>\n"
            + "    <class>run.ratchet.store.entity.ArchivedJobEntity</class>\n"
            + "    <class>run.ratchet.store.entity.NodeEntity</class>\n"
            + "    <class>run.ratchet.store.entity.DlqAlertEntity</class>\n"
            + "    <class>run.ratchet.store.entity.JobLogEntity</class>\n"
            + "    <class>run.ratchet.store.entity.ResourcePermitEntity</class>\n"
            + "    <class>run.ratchet.store.entity.BatchEntity</class>\n"
            + "    <class>run.ratchet.store.entity.LockEntity</class>\n"
            + "    <exclude-unlisted-classes>true</exclude-unlisted-classes>\n"
            + "    <properties>\n"
            + "      <property name=\"hibernate.hbm2ddl.auto\" value=\"none\"/>\n"
            + "      <property name=\"hibernate.show_sql\" value=\"true\"/>\n"
            + "      <property name=\"hibernate.connection.isolation\" value=\"2\"/>\n"
            + "    </properties>\n"
            + "  </persistence-unit>\n"
            + "</persistence>\n";

    archive.addAsResource(new StringAsset(persistenceXml), "META-INF/persistence.xml");
    return this;
  }

  public RatchetArchiveBuilder addDataSource() {
    JdbcDatabaseConfig config = JdbcContainerExtension.getConfig();
    DataSourceStrategy strategy = DataSourceStrategyFactory.create();
    strategy.configureArchive(archive, config);
    return this;
  }

  public RatchetArchiveBuilder addResource(String resource, String target) {
    archive.addAsResource(new StringAsset(resource), target);
    return this;
  }

  public RatchetArchiveBuilder addStoreInfrastructure() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");

    // Common classes always included
    archive.addClasses(
        BaseRatchetIT.class,
        JobAssertions.class,
        TestClassPolicy.class,
        TestCleanupStrategy.class,
        TestDataManipulator.class,
        PerformanceTestHelper.class,
        TestRatchetOptionsProducer.class);

    // Store-specific classes
    switch (dbType) {
      case "mysql", "postgresql" -> {
        archive.addClasses(
            JpaTestCleanupStrategy.class,
            JpaTestDataManipulator.class,
            JpaPerformanceTestHelper.class);
        addPersistenceXml(dbType);
        addDataSource();
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

    addAwaitility();
    return this;
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

  public WebArchive build() {
    return archive;
  }
}
