package run.ratchet.testsuite.security;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.ri.security.JobSecurityValidator;
import run.ratchet.ri.security.PackagePrefixClassPolicy;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import run.ratchet.testsuite.util.TestClassPolicy;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Set;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;

/**
 * Validates that ClassPolicy CDI wiring works correctly: the default policy is produced by
 * RatchetProducer, injected into JobSecurityValidator, and correctly gates job execution.
 */
class ClassPolicyIT extends BaseRatchetIT {

  @Inject private ClassPolicy classPolicy;

  @Inject private JobSecurityValidator securityValidator;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void classPolicy_shouldBeInjectable() {
    assertNotNull(classPolicy, "ClassPolicy should be injectable as a CDI bean");
    assertTrue(
        classPolicy instanceof TestClassPolicy,
        "Injected ClassPolicy should be TestClassPolicy (test @Alternative)");
  }

  @Test
  void testClassPolicy_shouldAllowRatchetClasses() {
    assertTrue(classPolicy.isAllowed(SimpleJob.class.getName()));
    assertFalse(classPolicy.isAllowed("java.lang.Runtime"));
    assertFalse(classPolicy.isAllowed("com.example.SomeClass"));
  }

  @Test
  void emptyClassPolicy_shouldBlockAllClasses() {
    ClassPolicy empty = new PackagePrefixClassPolicy();
    assertFalse(empty.isAllowed("java.lang.Runtime"));
    assertFalse(empty.isAllowed("com.example.SomeClass"));
    assertFalse(empty.isAllowed(SimpleJob.class.getName()));
  }

  @Test
  void securityValidator_shouldBeInjectable() {
    assertNotNull(securityValidator, "JobSecurityValidator should be injectable as a CDI bean");
  }

  @Test
  void securityValidator_shouldAcceptAllowedClass() {
    JobPayload payload =
        new JobPayload(SimpleJob.class.getName(), "execute", "()V", true, List.of());

    assertDoesNotThrow(
        () -> securityValidator.validate(payload),
        "Security validator should accept classes from allowed packages");
  }

  @Test
  void securityValidator_shouldRejectJdkClasses() {
    JobPayload payload =
        new JobPayload("java.lang.Runtime", "getRuntime", "()Ljava/lang/Runtime;", true, List.of());

    assertThrows(
        SecurityException.class,
        () -> securityValidator.validate(payload),
        "Security validator should block JDK classes");
  }

  @Test
  void customClassPolicy_shouldAllowConfiguredPackages() {
    ClassPolicy custom = new PackagePrefixClassPolicy(Set.of("run.ratchet.testsuite."));

    assertTrue(custom.isAllowed(SimpleJob.class.getName()));
    assertFalse(custom.isAllowed("java.lang.Runtime"));
  }

  @Test
  void securityValidator_withCustomPolicy_shouldAcceptAllowedClass() {
    ClassPolicy custom = new PackagePrefixClassPolicy(Set.of("run.ratchet.testsuite."));
    JobSecurityValidator customValidator = new JobSecurityValidator(custom);

    JobPayload payload =
        new JobPayload(SimpleJob.class.getName(), "execute", "()V", true, List.of());

    assertDoesNotThrow(
        () -> customValidator.validate(payload),
        "Security validator should accept classes from allowed packages");
  }
}
