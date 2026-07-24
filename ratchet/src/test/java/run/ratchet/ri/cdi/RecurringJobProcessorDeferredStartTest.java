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
package run.ratchet.ri.cdi;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static run.ratchet.ri.cdi.RecurringJobProcessorLeaderGateTest.beanFor;
import static run.ratchet.ri.cdi.RecurringJobProcessorLeaderGateTest.mockRecurringJobBuilder;

import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import java.time.ZoneId;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.Recurring;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.store.spi.JobBatchStatusStore;

/**
 * Verifies the onStartup()/onRuntimeStart() split added to defer registration on build-time-CDI
 * runtimes (e.g. Quarkus) until {@link RatchetRuntimeStart} fires. This is the highest-risk code in
 * the ratchet-quarkus branch: a regression here would silently stop @Recurring registration on
 * every runtime, or double-register it, with no automated signal before this test existed.
 */
class RecurringJobProcessorDeferredStartTest {

  @AfterEach
  void clearDeferFlag() {
    System.clearProperty(RatchetRuntimeStart.DEFER_PROPERTY);
  }

  @Test
  void onStartup_whenAutoStartDeferred_doesNotRegisterJobs() {
    System.setProperty(RatchetRuntimeStart.DEFER_PROPERTY, "true");
    var schedulerService = mock(JobSchedulerService.class);
    var processor = newProcessor(schedulerService);

    processor.onStartup(new Object());

    verifyNoInteractions(schedulerService);
  }

  @Test
  void onStartup_whenNotDeferred_andNoManagedExecutor_registersInline() throws Exception {
    System.clearProperty(RatchetRuntimeStart.DEFER_PROPERTY);
    var schedulerService = mock(JobSchedulerService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(schedulerService.scheduleRecurring(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    var processor = newProcessor(schedulerService);

    processor.onStartup(new Object());

    // No ExecutorProvider is injected via this constructor, matching the documented plain-CDI/SE/
    // unit-test path: registration happens inline on the calling thread, not deferred.
    verify(schedulerService).scheduleRecurring(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder).submit();
  }

  @Test
  void onRuntimeStart_registersJobs_evenWhileAutoStartIsDeferred() throws Exception {
    // The realistic Quarkus scenario: the defer flag stays true for the whole process lifetime,
    // and RatchetRuntimeStart is the only thing that ever triggers registration.
    System.setProperty(RatchetRuntimeStart.DEFER_PROPERTY, "true");
    var schedulerService = mock(JobSchedulerService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(schedulerService.scheduleRecurring(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    var processor = newProcessor(schedulerService);

    processor.onRuntimeStart(new RatchetRuntimeStart());

    verify(schedulerService).scheduleRecurring(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder).submit();
  }

  private RecurringJobProcessor newProcessor(JobSchedulerService schedulerService) {
    var beanManager = mock(BeanManager.class);
    Set<Bean<?>> beans = Set.of(beanFor(RecurringBean.class));
    when(beanManager.getBeans(any(), any())).thenReturn(beans);
    return new RecurringJobProcessor(
        schedulerService,
        mock(JobBatchStatusStore.class),
        mock(RecurringAnnotationMaintenanceService.class),
        beanManager,
        mock(RecurringMethodInvoker.class),
        null,
        new RecurringRegistrationState());
  }

  static class RecurringBean {
    @Recurring(id = "leader-gate-job", cron = "0 0/5 * * * ?")
    public void run() {}
  }
}
