package run.ratchet.testsuite.concurrency;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import jakarta.interceptor.Interceptor;
import java.util.Optional;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.RatchetOptionsFactory;
import run.ratchet.spi.RatchetConfigSource;
import run.ratchet.testsuite.app.TestRuntimeConfig;

/**
 * Overrides the default {@code TestRatchetOptionsProducer} for the virtual-thread deployment only.
 * Turns on the virtual-thread backpressure model and points the job executor at the
 * application-declared {@link VirtualThreadTestExecutor} ({@code @ManagedExecutorDefinition(virtual
 * = true)}). All other settings (DB type, poller tunings) still flow from {@link TestRuntimeConfig}.
 *
 * <p>Selected via {@code @Alternative @Priority(APPLICATION + 100)}, so it wins over the
 * non-alternative default producer wherever this class is bundled — and nowhere else.
 */
@ApplicationScoped
@Alternative
@Priority(Interceptor.Priority.APPLICATION + 100)
public class VirtualThreadOptionsProducer {

  // The test owns its executor as a WAR application class (see VirtualThreadTestExecutor) so it
  // binds on every EE 11 container, not just those that scan library jars for resource definitions.
  static final String VIRTUAL_EXECUTOR = "java:app/concurrent/RatchetTestVirtualExecutor";

  @Produces
  @ApplicationScoped
  public RatchetOptions ratchetOptions() {
    return RatchetOptionsFactory.fromEnvironment(
        new VirtualThreadOverrides(), new TestRuntimeConfig());
  }

  /**
   * Highest-precedence config source returning only the threading keys. It adds the virtual pool
   * (backed by {@link VirtualThreadTestExecutor}) and makes it the default, so plain jobs route
   * there. The platform pool stays at the container default executor, which lets the routing IT
   * send a {@code .platform()} job to one pool and a {@code .virtual()} job to the other. The job
   * executor is the only thing redirected; the scheduled executor stays at the container default,
   * because Ratchet uses it during CDI startup (node heartbeat) before an app-defined executor is
   * bound on some containers (GlassFish). The virtual executor resolves lazily after deployment.
   */
  private static final class VirtualThreadOverrides implements RatchetConfigSource {
    @Override
    public Optional<String> get(String propertyName, String environmentVariable) {
      return switch (propertyName) {
        case "ratchet.worker.default-threading-mode" -> Optional.of("virtual");
        case "ratchet.worker.virtual-executor-jndi" -> Optional.of(VIRTUAL_EXECUTOR);
        default -> Optional.empty();
      };
    }
  }
}
