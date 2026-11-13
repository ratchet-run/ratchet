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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JakartaAfterCommitRegistrar;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

@ExtendWith(MockitoExtension.class)
class DefaultJobSchedulerServiceBusinessKeyLookupTest {

  @Mock private InternalEventPublisher eventPublisher;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobPauseStore jobPauseStore;
  @Mock private JobRetryStore jobRetryStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private BatchStore batchStore;
  @Mock private TagStore tagStore;
  @Mock private WorkflowConditionStore workflowConditionStore;
  @Mock private JobWakeupService wakeupService;
  @Mock private RecurringJobStore recurringJobStore;
  @Mock private RecurringScheduler recurringScheduler;
  @Mock private JobInvocationResolver jobInvocationResolver;
  @Mock private DefaultJobCreationService jobCreationService;
  @Mock private JobAuthorizationPolicy authorizationPolicy;

  private DefaultJobSchedulerService service;

  @BeforeEach
  void setUp() {
    CallerPrincipalProvider callerProvider =
        new CallerPrincipalProvider(null) {
          @Override
          public Optional<String> currentPrincipal() {
            return Optional.empty();
          }
        };

    service =
        new DefaultJobSchedulerService(
            eventPublisher,
            jobBatchStatusStore,
            jobPauseStore,
            jobRetryStore,
            jobTerminalStore,
            jobCrudStore,
            batchStore,
            tagStore,
            workflowConditionStore,
            recurringJobStore,
            wakeupService,
            recurringScheduler,
            jobInvocationResolver,
            jobCreationService,
            callerProvider,
            authorizationPolicy,
            null,
            null,
            null,
            Clock.systemUTC(),
            null,
            new JakartaAfterCommitRegistrar(),
            mock(BatchService.class),
            null);
  }

  @Test
  void emptyKeysDoNotReadStore() {
    assertTrue(service.findBusinessKeyHolder(null).isEmpty());
    assertTrue(service.findBusinessKeyHolder("  ").isEmpty());
    verifyNoInteractions(jobCrudStore, authorizationPolicy);
  }

  @Test
  void invalidKeysDoNotReadStore() {
    assertThrows(
        IllegalArgumentException.class, () -> service.findBusinessKeyHolder("x".repeat(256)));
    assertThrows(IllegalArgumentException.class, () -> service.findBusinessKeyHolder("caf\u00e9"));
    verifyNoInteractions(jobCrudStore, authorizationPolicy);
  }

  @Test
  void trimsKeyAndReturnsStoreResultWithoutAuthorization() {
    var result =
        Optional.of(new BusinessKeyHolder(UUID.randomUUID(), JobType.RECURRING, JobStatus.PENDING));
    when(jobCrudStore.findBusinessKeyHolder("key")).thenReturn(result);
    assertSame(result, service.findBusinessKeyHolder(" key "));
    verify(jobCrudStore).findBusinessKeyHolder("key");
    verifyNoInteractions(authorizationPolicy);
  }
}
