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
package run.ratchet.coordinator.infinispan;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NoOpMetricsCollector;
import run.ratchet.spi.NodeIdentityProvider;

/**
 * Boots a real CDI container to prove the coordinator deploys with no unsatisfied dependency.
 *
 * <p>The tuning config record has no producing bean, so a direct {@code @Inject
 * InfinispanCoordinatorConfig} is an unsatisfied dependency that fails deployment validation out of
 * the box. Resolving it through an {@link jakarta.enterprise.inject.Instance} with a {@code
 * defaults()} fallback makes the coordinator deployable with zero application wiring. The container
 * here provides only the SPI collaborators an application would already supply; the config is left
 * deliberately unproduced.
 */
class InfinispanCoordinatorCdiDeploymentTest {

  @Test
  void coordinatorDeploysWithoutAProducerForItsConfigRecord() {
    SeContainerInitializer initializer =
        SeContainerInitializer.newInstance()
            .disableDiscovery()
            .addBeanClasses(InfinispanClusterCoordinator.class, SpiCollaborators.class);

    try (SeContainer container = assertDoesNotThrow(initializer::initialize)) {
      assertTrue(
          container.select(InfinispanClusterCoordinator.class).isResolvable(),
          "coordinator must resolve as an enabled bean with every injection point satisfied");
    }
  }

  @ApplicationScoped
  static class SpiCollaborators {

    @Produces
    NodeIdentityProvider nodeIdentityProvider() {
      return () -> "test-node";
    }

    @Produces
    @ApplicationScoped
    MetricsCollector metricsCollector() {
      return new NoOpMetricsCollector();
    }
  }
}
