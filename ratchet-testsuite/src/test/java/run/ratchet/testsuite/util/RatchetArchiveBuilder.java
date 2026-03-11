package run.ratchet.testsuite.util;

import run.ratchet.testsuite.infra.DatabaseConfig;
import run.ratchet.testsuite.infra.DatabaseContainerExtension;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;

/**
 * Fluent ShrinkWrap builder for Ratchet integration test deployments.
 *
 * <p>Uses Maven resolver to import compile and runtime dependencies from the testsuite POM,
 * following the Krazo WebArchiveBuilder pattern. The active Maven profile determines which store
 * module and JDBC driver are included.
 */
public class RatchetArchiveBuilder {

  private final WebArchive archive;

  private RatchetArchiveBuilder() {
    archive = ShrinkWrap.create(WebArchive.class, "ratchet-test.war");
  }

  /** Creates a new builder with an empty WAR archive. */
  public static RatchetArchiveBuilder create() {
    return new RatchetArchiveBuilder();
  }

  /**
   * Imports all compile and runtime dependencies from the testsuite POM, using the specified Maven
   * profiles for profile-specific store modules and JDBC drivers.
   *
   * @param profiles the Maven profiles to activate (e.g., "wildfly-managed", "mysql")
   * @return this builder
   */
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

  /**
   * Adds classes to the archive.
   *
   * @param classes the classes to add
   * @return this builder
   */
  public RatchetArchiveBuilder addClasses(Class<?>... classes) {
    archive.addClasses(classes);
    return this;
  }

  /**
   * Adds a package and all its classes to the archive.
   *
   * @param pkg the package to add
   * @return this builder
   */
  public RatchetArchiveBuilder addPackage(Package pkg) {
    archive.addPackage(pkg);
    return this;
  }

  /**
   * Adds a CDI beans.xml descriptor enabling bean discovery.
   *
   * @return this builder
   */
  public RatchetArchiveBuilder addBeansXml() {
    archive.addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
    return this;
  }

  /**
   * Adds a persistence.xml for the given database type.
   *
   * <p>Selects the appropriate dialect and references the {@code RatchetDS} datasource JNDI name.
   *
   * @param dbType "mysql" or "postgresql"
   * @return this builder
   */
  public RatchetArchiveBuilder addPersistenceXml(String dbType) {
    String dialect =
        switch (dbType) {
          case "mysql" -> "org.hibernate.dialect.MySQLDialect";
          case "postgresql" -> "org.hibernate.dialect.PostgreSQLDialect";
          default -> throw new IllegalArgumentException("Unsupported db type: " + dbType);
        };

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
            + "      <property name=\"hibernate.dialect\" value=\""
            + dialect
            + "\"/>\n"
            + "      <property name=\"hibernate.hbm2ddl.auto\" value=\"none\"/>\n"
            + "      <property name=\"hibernate.show_sql\" value=\"true\"/>\n"
            + "    </properties>\n"
            + "  </persistence-unit>\n"
            + "</persistence>\n";

    archive.addAsResource(new StringAsset(persistenceXml), "META-INF/persistence.xml");
    return this;
  }

  /**
   * Configures a datasource for the active application server using the test database container.
   *
   * @return this builder
   */
  public RatchetArchiveBuilder addDataSource() {
    DatabaseConfig config = DatabaseContainerExtension.getConfig();
    DataSourceStrategy strategy = DataSourceStrategyFactory.create();
    strategy.configureArchive(archive, config);
    return this;
  }

  /**
   * Adds a custom resource to the archive.
   *
   * @param resource the resource content
   * @param target the target path within the archive
   * @return this builder
   */
  public RatchetArchiveBuilder addResource(String resource, String target) {
    archive.addAsResource(new StringAsset(resource), target);
    return this;
  }

  /**
   * Adds test infrastructure classes needed inside the container. Includes the base test class and
   * assertion utilities from the {@code util} package. The {@code infra} package (Testcontainers
   * extensions) is deliberately excluded — those classes only run on the client side via JUnit 5
   * automatic extension detection.
   *
   * @return this builder
   */
  public RatchetArchiveBuilder addTestInfrastructure() {
    archive.addClasses(BaseRatchetIT.class, JobAssertions.class, TestClassPolicy.class);
    // Awaitility is needed in-container by JobAssertions but is test-scoped
    // (not pulled in by importCompileAndRuntimeDependencies)
    archive.addAsLibraries(
        Maven.configureResolver()
            .loadPomFromFile("pom.xml")
            .resolve("org.awaitility:awaitility")
            .withTransitivity()
            .asFile());
    return this;
  }

  /**
   * Returns the built WebArchive.
   *
   * @return the configured WebArchive
   */
  public WebArchive build() {
    return archive;
  }
}
