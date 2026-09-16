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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobStatus;
import run.ratchet.api.Recurring;
import run.ratchet.store.spi.JobCrudStore;

/** Tests inherited annotation discovery and execution through a real CDI proxy and database. */
@QuarkusTest
@QuarkusTestResource(RatchetDatabaseTestResource.class)
@TestProfile(InheritedRecurringMethodTest.InheritedProfile.class)
class InheritedRecurringMethodTest {
  @Inject InvocationState state;
  @Inject JobCrudStore jobs;

  @Test
  void inheritedNoArgumentMethodIsDiscoveredAndExecuted() {
    assertSuccessfulExecution(state::noArgumentJobId);
  }

  @Test
  void inheritedContextMethodReceivesTheActiveExecutionContext() {
    assertSuccessfulExecution(state::contextJobId);
    assertEquals(state.contextJobId(), state.currentContextJobId());
  }

  private void assertSuccessfulExecution(java.util.function.Supplier<UUID> id) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              UUID jobId = id.get();
              assertNotNull(jobId, "Inherited recurring method must actually execute");
              assertEquals(JobStatus.SUCCEEDED, jobs.findById(jobId).orElseThrow().getStatus());
            });
  }

  public static class InheritedProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("ratchet.test.inherited-recurring", "true");
    }
  }

  public abstract static class BaseRecurringJobs {
    @Inject InvocationState state;

    @Recurring(id = "it-inherited-no-argument", cron = "0/1 * * * * ?")
    public void inheritedNoArgument() {
      state.recordNoArgument(JobContext.current().jobId());
    }

    @Recurring(id = "it-inherited-context", cron = "0/1 * * * * ?")
    public void inheritedWithContext(JobContext context) {
      state.recordContext(context.jobId(), JobContext.current().jobId());
    }
  }

  @IfBuildProperty(name = "ratchet.test.inherited-recurring", stringValue = "true")
  @ApplicationScoped
  public static class InheritedJobs extends BaseRecurringJobs {}

  @IfBuildProperty(name = "ratchet.test.inherited-recurring", stringValue = "true")
  @ApplicationScoped
  public static class InvocationState {
    private final AtomicReference<UUID> noArgument = new AtomicReference<>();
    private final AtomicReference<ContextIds> context = new AtomicReference<>();

    public void recordNoArgument(UUID id) {
      noArgument.compareAndSet(null, id);
    }

    public void recordContext(UUID parameterId, UUID currentId) {
      context.compareAndSet(null, new ContextIds(parameterId, currentId));
    }

    public UUID noArgumentJobId() {
      return noArgument.get();
    }

    public UUID contextJobId() {
      return context.get() == null ? null : context.get().parameterId();
    }

    public UUID currentContextJobId() {
      return context.get() == null ? null : context.get().currentId();
    }

    private record ContextIds(UUID parameterId, UUID currentId) {}
  }
}
