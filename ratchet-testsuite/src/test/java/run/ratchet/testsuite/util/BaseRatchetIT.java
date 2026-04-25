package run.ratchet.testsuite.util;

import run.ratchet.testsuite.app.TestCleanupStrategy;
import jakarta.inject.Inject;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for all Ratchet integration tests. Injects TestCleanupStrategy and truncates tables
 * before each test.
 */
@ExtendWith(ArquillianExtension.class)
public abstract class BaseRatchetIT {

  @Inject private TestCleanupStrategy cleanupStrategy;

  @BeforeEach
  protected void truncateAll() throws Exception {
    cleanupStrategy.truncateAll();
  }

  @AfterEach
  protected void cleanupAfterEach() throws Exception {
    cleanupStrategy.truncateAll();
  }
}
