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
 * = true)}). It also caps the per-execution-type virtual-thread limit so GlassFish 8 + JDK 21
 * cells never pin more virtual carriers than the CI runner has (see {@link
 * VirtualThreadOverrides}). All other settings (DB type, poller tunings) still flow from {@link
 * TestRuntimeConfig}.
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

  // Per-execution-type in-flight cap (see VirtualThreadOverrides.get() for why). The manyJobs
  // concurrency assertion needs at least 2 in-flight jobs to observe overlap, so this is the
  // floor, not an arbitrary throttle.
  private static final String VIRTUAL_THREAD_LIMIT = "2";

  @Produces
  @ApplicationScoped
  public RatchetOptions ratchetOptions() {
    return RatchetOptionsFactory.fromEnvironment(
        new VirtualThreadOverrides(), new TestRuntimeConfig());
  }

  /**
   * Highest-precedence config source returning the threading keys and the per-execution-type
   * virtual-thread limit override. It adds the virtual pool (backed by {@link
   * VirtualThreadTestExecutor}) and makes it the default, so plain jobs route there. The platform
   * pool stays at the container default executor, which lets the routing IT send a {@code
   * .platform()} job to one pool and a {@code .virtual()} job to the other. The job executor is
   * the only thing redirected; the scheduled executor stays at the container default, because
   * Ratchet uses it during CDI startup (node heartbeat) before an app-defined executor is bound on
   * some containers (GlassFish). The virtual executor resolves lazily after deployment.
   */
  private static final class VirtualThreadOverrides implements RatchetConfigSource {
    @Override
    public Optional<String> get(String propertyName, String environmentVariable) {
      // On GlassFish 8 + JDK 21, virtual job tasks committing through EclipseLink pin their
      // carriers inside synchronized sections while contending for the container ConnectionPool
      // lock. JEP 444 pinned parks get no scheduler compensation, so in-flight virtual jobs must
      // stay below the carrier count or no carrier is left for whichever thread holds the pool
      // lock (the local repro hangs or passes purely as a function of carrier count) — observed
      // as a total wedge of the first virtual-executor deployment on the 4-core CI runners. Cap
      // every execution type (RatchetConfigKeys.virtualThreadLimit:
      // single, recurring, batch-child, batch-parent, chain-step, workflow-branch, workflow-join)
      // well below the carrier count.
      if (propertyName.startsWith("ratchet.virtual-thread-limit.")) {
        return Optional.of(VIRTUAL_THREAD_LIMIT);
      }
      return switch (propertyName) {
        case "ratchet.worker.default-threading-mode" -> Optional.of("virtual");
        case "ratchet.worker.virtual-executor-jndi" -> Optional.of(VIRTUAL_EXECUTOR);
        default -> Optional.empty();
      };
    }
  }
}
