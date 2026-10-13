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
package run.ratchet.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.Recurring;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.spi.JobStore;

class MicronautRecurringDiscoveryTest {
  static final AtomicInteger created = new AtomicInteger();

  public interface Contract {
    @Recurring(cron = "0 * * * * ?")
    void run();
  }

  public static class Task implements Contract {
    public Task() {
      created.incrementAndGet();
    }

    public void run() {}
  }

  public static class DirectTask {
    public DirectTask() {
      created.incrementAndGet();
    }

    @Recurring(cron = "0 * * * * ?")
    public void run() {}
  }

  public interface DefaultContract {
    @Recurring(cron = "0 * * * * ?")
    default void run() {}
  }

  public static class DefaultTask implements DefaultContract {}

  public static class OverriddenTask extends DirectTask {
    @Override
    public void run() {}
  }

  @Factory
  @Requires(property = "test.recurring", value = "true")
  static class Tasks {
    @Singleton
    @RatchetTransactionBoundary
    Task task() {
      return new Task();
    }

    @Prototype
    DirectTask directTask() {
      return new DirectTask();
    }

    @Singleton
    DefaultTask defaultTask() {
      return new DefaultTask();
    }
  }

  @Test
  void closestOverrideAndDefaultInterfaceMethodsAreRetainedOnce() throws Exception {
    assertEquals(
        Set.of(OverriddenTask.class.getMethod("run")),
        MicronautRecurringDiscovery.recurringMethods(OverriddenTask.class).keySet());
    assertEquals(
        Set.of(DefaultContract.class.getMethod("run")),
        MicronautRecurringDiscovery.recurringMethods(DefaultTask.class).keySet());
  }

  @Test
  void discoveryAndRegistrationDoNotInstantiateLazyOrPrototypeJobs() throws Exception {
    created.set(0);
    try (var context =
        TestContexts.core(Map.of("ratchet.enabled", false, "test.recurring", true))) {
      context.start();
      var submissions = mock(InvocationSubmissionService.class);
      var builder = mock(RecurringJobBuilder.class, RETURNS_SELF);
      when(submissions.scheduleRecurringInvocation(anyString(), any(), any())).thenReturn(builder);
      when(builder.submit()).thenReturn((JobHandle) UUID::randomUUID);
      var invoker = new RecurringMethodInvoker(new MicronautBeanResolver(context), name -> true);
      var discovery =
          new MicronautRecurringDiscovery(
              context,
              submissions,
              mock(JobStore.class),
              mock(RecurringAnnotationMaintenanceService.class),
              invoker,
              mock(StartupCoordinator.class),
              new RecurringRegistrationState(),
              RatchetOptions.defaults(),
              Clock.systemUTC());
      assertEquals(Set.of(Task.class, DirectTask.class, DefaultTask.class), discovery.discover());
      discovery.register();
      verify(builder, times(3)).submit();
      assertEquals(0, created.get());
      invoker.invoke(Task.class.getName(), "run", false);
      invoker.invoke(DirectTask.class.getName(), "run", false);
      invoker.invoke(DirectTask.class.getName(), "run", false);
      // Factory AOP creates both a target and a forwarding proxy via the public constructor.
      // The two prototype invocations each construct one additional target.
      assertEquals(4, created.get());
    }
  }
}
