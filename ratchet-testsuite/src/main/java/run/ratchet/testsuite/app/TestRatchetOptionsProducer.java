package run.ratchet.testsuite.app;

import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/**
 * Produces {@link RatchetOptions} for integration tests by reading environment variables and
 * MicroProfile Config. Every test deployment must include this bean (or an equivalent override) so
 * {@code RatchetLifecycle} can start.
 */
@ApplicationScoped
public class TestRatchetOptionsProducer {

  @Produces
  @ApplicationScoped
  public RatchetOptions ratchetOptions() {
    return RatchetOptionsFactory.fromEnvironment(new TestRuntimeConfig());
  }
}
