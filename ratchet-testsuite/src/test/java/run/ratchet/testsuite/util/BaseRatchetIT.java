package run.ratchet.testsuite.util;

import run.ratchet.testsuite.app.TestCleanupStrategy;
import jakarta.inject.Inject;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Unified abstract base class for all Ratchet integration tests.
 *
 * <p>Provides Arquillian lifecycle management and store-agnostic data cleanup via {@link
 * TestCleanupStrategy}. The active cleanup strategy is determined by which implementation is
 * packaged in the test WAR — controlled by the Maven profile and {@code
 * RatchetArchiveBuilder.addStoreInfrastructure()}.
 *
 * <p>Database container management is handled by store-specific JUnit 5 extensions registered via
 * {@code META-INF/services/org.junit.jupiter.api.extension.Extension} on the client side.
 */
@ExtendWith(ArquillianExtension.class)
public abstract class BaseRatchetIT {

  @Inject private TestCleanupStrategy cleanupStrategy;

  /** Clears all scheduler data before each test to ensure test isolation. */
  @BeforeEach
  protected void truncateAll() throws Exception {
    cleanupStrategy.truncateAll();
  }
}
