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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.ri.cdi.RecurringMethodInvoker;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.ri.security.PackagePrefixClassPolicy;
import run.ratchet.ri.testsupport.StubAfterCommitRegistrar;
import run.ratchet.spi.JobInvocation;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

@ExtendWith(MockitoExtension.class)
class DefaultJobCreationServiceRecurringClassPolicyTest {

  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobBulkStore jobBulkStore;
  @Mock private BatchStore batchStore;
  @Mock private TagStore tagStore;
  @Mock private WorkflowConditionStore workflowConditionStore;
  @Mock private RecurringJobStore recurringJobStore;
  @Mock private RecurringScheduler recurringScheduler;

  private static class NoopJobWakeupService extends JobWakeupService {
    @Override
    public void notify(JobPriority priority, boolean immediate, String executionTarget) {}

    @Override
    public void notifyIfNeeded(
        JobExecutionType jobType, JobPriority priority, Duration delay, String executionTarget) {}
  }

  @Test
  void recurringDispatchShimPassesEvenWhenItsOwnPackageIsNotAllowlisted() {
    when(recurringJobStore.createRecurring(any(RecurringJobDefinition.class)))
        .thenAnswer(invocation -> invocation.<RecurringJobDefinition>getArgument(0).id());
    DefaultJobCreationService service =
        new DefaultJobCreationService(
            jobBatchStatusStore,
            jobTerminalStore,
            jobCrudStore,
            jobBulkStore,
            batchStore,
            tagStore,
            workflowConditionStore,
            recurringJobStore,
            new NoopJobWakeupService(),
            recurringScheduler,
            recurringDispatchResolver(),
            new JobPayloadInputValidator(),
            null,
            null,
            null,
            new PackagePrefixClassPolicy(Set.of("run.ratchet.ri.core.")),
            null,
            null,
            Clock.fixed(Instant.parse("2026-07-01T12:00:00Z"), ZoneOffset.UTC),
            new StubAfterCommitRegistrar());
    DefaultRecurringJobBuilder builder =
        new DefaultRecurringJobBuilder(
            "0 0 12 * * ?", ZoneId.of("UTC"), AppRecurringBean::doWork, service);

    assertDoesNotThrow(() -> service.submit(builder));

    ArgumentCaptor<RecurringJobDefinition> definitionCaptor =
        ArgumentCaptor.forClass(RecurringJobDefinition.class);
    verify(recurringJobStore).createRecurring(definitionCaptor.capture());
    assertEquals(
        RecurringMethodInvoker.class.getName(), definitionCaptor.getValue().payload().target());
  }

  public static class AppRecurringBean {
    public static void doWork() {}
  }

  private static JobInvocationResolver recurringDispatchResolver() {
    return new JobInvocationResolver() {
      @Override
      public JobInvocation resolve(Serializable callback) {
        return recurringDispatchInvocation();
      }

      @Override
      public JobInvocation resolve(Serializable callback, List<Object> runtimeArguments) {
        return recurringDispatchInvocation();
      }
    };
  }

  private static JobInvocation recurringDispatchInvocation() {
    return new JobInvocation(
        RecurringMethodInvoker.class.getName(),
        "invoke",
        "(Ljava/lang/String;Ljava/lang/String;Z)V",
        false,
        List.of(AppRecurringBean.class.getName(), "doWork", Boolean.FALSE));
  }
}
