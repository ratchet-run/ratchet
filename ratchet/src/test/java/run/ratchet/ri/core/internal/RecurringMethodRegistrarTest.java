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
package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.time.Clock;
import java.time.ZoneId;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.Recurring;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.runtime.RecurringMethodDiscovery;

class RecurringMethodRegistrarTest {

  @Test
  void register_usesOnlyClassesReturnedByPortableDiscovery() {
    JobSchedulerService schedulerService = mock(JobSchedulerService.class);
    RecurringJobBuilder builder = recurringJobBuilder();
    when(schedulerService.scheduleRecurring(any(String.class), any(ZoneId.class), any()))
        .thenReturn(builder);
    RecurringMethodInvoker methodInvoker = mock(RecurringMethodInvoker.class);
    RecurringMethodDiscovery discovery = () -> Set.of(IncludedBean.class);
    RecurringMethodRegistrar registrar = newRegistrar(schedulerService, discovery, methodInvoker);

    registrar.register();

    verify(schedulerService).scheduleRecurring(any(String.class), any(ZoneId.class), any());
    verify(builder).withBusinessKey("included-job");
    verify(builder, never()).withBusinessKey("omitted-job");
    verify(methodInvoker).validateBeanResolvable(IncludedBean.class);
    verify(methodInvoker, never()).validateBeanResolvable(OmittedBean.class);
  }

  @Test
  void register_skipsMethodsRejectedByContainerInvocabilityPolicy() {
    AtomicReference<Class<?>> checkedBeanClass = new AtomicReference<>();
    AtomicReference<Method> checkedMethod = new AtomicReference<>();
    RecurringMethodDiscovery discovery =
        new RecurringMethodDiscovery() {
          @Override
          public Set<Class<?>> recurringBeanClasses() {
            return Set.of(RejectedBean.class);
          }

          @Override
          public boolean isMethodInvocable(Class<?> beanClass, Method method) {
            checkedBeanClass.set(beanClass);
            checkedMethod.set(method);
            return false;
          }
        };
    JobSchedulerService schedulerService = mock(JobSchedulerService.class);
    RecurringMethodInvoker methodInvoker = mock(RecurringMethodInvoker.class);
    RecurringMethodRegistrar registrar = newRegistrar(schedulerService, discovery, methodInvoker);

    registrar.register();

    assertSame(RejectedBean.class, checkedBeanClass.get());
    assertEquals("run", checkedMethod.get().getName());
    verifyNoInteractions(schedulerService);
    verify(methodInvoker).validateBeanResolvable(RejectedBean.class);
  }

  @Test
  void register_checksContainerInvocabilityOnlyAfterEnabledAndResolvableValidation() {
    RecurringMethodDiscovery discovery =
        new RecurringMethodDiscovery() {
          @Override
          public Set<Class<?>> recurringBeanClasses() {
            return Set.of(DisabledBean.class, IncludedBean.class);
          }

          @Override
          public boolean isMethodInvocable(Class<?> beanClass, Method method) {
            return true;
          }
        };
    JobSchedulerService schedulerService = mock(JobSchedulerService.class);
    RecurringJobBuilder builder = recurringJobBuilder();
    when(schedulerService.scheduleRecurring(any(String.class), any(ZoneId.class), any()))
        .thenReturn(builder);
    RecurringMethodInvoker methodInvoker = mock(RecurringMethodInvoker.class);
    RecurringMethodRegistrar registrar = newRegistrar(schedulerService, discovery, methodInvoker);

    registrar.register();

    verify(methodInvoker).validateBeanResolvable(IncludedBean.class);
    verify(methodInvoker, never()).validateBeanResolvable(DisabledBean.class);
    verify(schedulerService, times(1))
        .scheduleRecurring(any(String.class), any(ZoneId.class), any());
  }

  private static RecurringMethodRegistrar newRegistrar(
      JobSchedulerService schedulerService,
      RecurringMethodDiscovery discovery,
      RecurringMethodInvoker methodInvoker) {
    return new RecurringMethodRegistrar(
        schedulerService,
        mock(RecurringAnnotationMaintenanceService.class),
        discovery,
        methodInvoker,
        null,
        new RecurringRegistrationState(),
        RatchetOptions.defaults(),
        null,
        null,
        Clock.systemUTC());
  }

  private static RecurringJobBuilder recurringJobBuilder() {
    RecurringJobBuilder builder = mock(RecurringJobBuilder.class);
    when(builder.withOptions(any())).thenReturn(builder);
    when(builder.withBusinessKey(any())).thenReturn(builder);
    when(builder.withTags(any())).thenReturn(builder);
    when(builder.submit()).thenReturn((JobHandle) () -> UUID.randomUUID());
    return builder;
  }

  static class IncludedBean {
    @Recurring(id = "included-job", cron = "0 0/5 * * * ?")
    public void run() {}
  }

  static class OmittedBean {
    @Recurring(id = "omitted-job", cron = "0 0/5 * * * ?")
    public void run() {}
  }

  static class RejectedBean {
    @Recurring(id = "rejected-job", cron = "0 0/5 * * * ?")
    public void run() {}
  }

  static class DisabledBean {
    @Recurring(id = "disabled-job", cron = "0 0/5 * * * ?", enabled = false)
    public void run() {}
  }
}
