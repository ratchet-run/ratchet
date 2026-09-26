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
package run.ratchet.quarkus.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.cdi.RatchetLifecycle;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.store.spi.JobCrudStore;

@QuarkusTest
@QuarkusTestResource(RatchetDatabaseTestResource.class)
@TestProfile(RatchetShutdownOrderingTest.ShutdownProfile.class)
class RatchetShutdownOrderingTest {
  @Inject Event<ShutdownEvent> shutdown;
  @Inject ShutdownState probe;
  @Inject RatchetLifecycle lifecycle;
  @Inject Instance<SchedulerLifecycleHook> hooks;

  @Test
  void stopsSchedulerOnShutdownEventWhilePersistenceIsAvailable() {
    assertTrue(
        hooks.stream().anyMatch(h -> h instanceof ShutdownProbe),
        hooks.stream().map(h -> h.getClass().getName()).toList().toString());
    assertEquals(1, probe.starts(), "Probe must participate in engine startup");
    assertEquals(0, probe.stops());
    shutdown.fire(new ShutdownEvent());
    assertEquals(1, probe.stops(), "Quarkus shutdown must stop Ratchet before CDI destruction");
    assertTrue(probe.persistenceAvailable(), "Shutdown hook must still be able to use the store");
    lifecycle.onShutdown();
    assertEquals(1, probe.stops(), "Later CDI destruction must not repeat shutdown hooks");
  }

  public static class ShutdownProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("ratchet.test.shutdown-probe", "true");
    }
  }

  @IfBuildProperty(name = "ratchet.test.shutdown-probe", stringValue = "true")
  @ApplicationScoped
  public static class ShutdownProbe implements SchedulerLifecycleHook {
    @Inject JobCrudStore jobs;
    @Inject ShutdownState state;

    @Override
    public void beforeStart() {
      state.started();
    }

    @Override
    public void afterStop() {
      jobs.countPendingJobs();
      state.stoppedWithPersistence();
    }
  }

  // Lifecycle.destroyHooks destroys the hook itself; retain observations outside that context.
  @IfBuildProperty(name = "ratchet.test.shutdown-probe", stringValue = "true")
  @ApplicationScoped
  public static class ShutdownState {
    private int starts;
    private int stops;

    public void started() {
      starts++;
    }

    public void stoppedWithPersistence() {
      stops++;
    }

    public int starts() {
      return starts;
    }

    public int stops() {
      return stops;
    }

    public boolean persistenceAvailable() {
      return stops > 0;
    }
  }
}
