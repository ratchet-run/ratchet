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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.IdempotencyKeyHolder;
import run.ratchet.api.IdempotencyKeyHolder.Retention;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JakartaAfterCommitRegistrar;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ArchiveStore;
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
class DefaultJobSchedulerServiceIdempotencyKeyLookupTest {

  @Mock private InternalEventPublisher eventPublisher;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobPauseStore jobPauseStore;
  @Mock private JobRetryStore jobRetryStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private BatchStore batchStore;
  @Mock private ArchiveStore archiveStore;
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
    service = newService(archiveStore);
  }

  private DefaultJobSchedulerService newService(ArchiveStore archives) {
    return new DefaultJobSchedulerService(
        eventPublisher,
        jobBatchStatusStore,
        jobPauseStore,
        jobRetryStore,
        jobTerminalStore,
        jobCrudStore,
        batchStore,
        archives,
        tagStore,
        workflowConditionStore,
        recurringJobStore,
        wakeupService,
        recurringScheduler,
        jobInvocationResolver,
        jobCreationService,
        null,
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
    assertTrue(service.findIdempotencyKeyHolder(null).isEmpty());
    assertTrue(service.findIdempotencyKeyHolder("  ").isEmpty());
    verifyNoInteractions(jobCrudStore, archiveStore, authorizationPolicy);
  }

  @Test
  void invalidKeysDoNotReadStore() {
    assertThrows(
        IllegalArgumentException.class, () -> service.findIdempotencyKeyHolder("x".repeat(37)));
    verifyNoInteractions(jobCrudStore, archiveStore, authorizationPolicy);
  }

  @AfterEach
  void lookupNeverChecksAuthorization() {
    verifyNoInteractions(authorizationPolicy);
  }

  @Test
  void unusedKeyIsEmptyAndTrimmedBeforeLedgerRead() {
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey("key")).thenReturn(Optional.empty());
    assertTrue(service.findIdempotencyKeyHolder(" key ").isEmpty());
    verify(jobCrudStore).findOriginalJobIdByIdempotencyKey("key");
    verifyNoInteractions(archiveStore);
  }

  @Test
  void liveRowTakesPrecedenceOverArchive() {
    UUID id = ledgerEntry();
    JobEntity job = new JobEntity();
    job.setJobType(JobExecutionType.BATCH_CHILD);
    job.setStatus(JobStatus.RUNNING);
    when(jobCrudStore.findById(id)).thenReturn(Optional.of(job));
    assertEquals(
        new IdempotencyKeyHolder(id, Retention.LIVE, JobType.BATCH, JobStatus.RUNNING),
        service.findIdempotencyKeyHolder("key").orElseThrow());
    verifyNoInteractions(archiveStore);
  }

  @Test
  void archiveRowReportsFinalStatusAndPublicType() {
    UUID id = ledgerEntry();
    when(jobCrudStore.findById(id)).thenReturn(Optional.empty());
    ArchivedJobEntity job = new ArchivedJobEntity();
    job.setOriginalJobId(id);
    job.setJobType(JobExecutionType.WORKFLOW_BRANCH);
    job.setFinalStatus(JobStatus.FAILED);
    when(archiveStore.findArchivedJob(id)).thenReturn(Optional.of(job));
    assertEquals(
        new IdempotencyKeyHolder(id, Retention.ARCHIVED, JobType.WORKFLOW, JobStatus.FAILED),
        service.findIdempotencyKeyHolder("key").orElseThrow());
    verify(archiveStore).findArchivedJob(id);
  }

  @Test
  void ledgerWithoutEitherRowIsPurged() {
    UUID id = ledgerEntry();
    when(jobCrudStore.findById(id)).thenReturn(Optional.empty());
    when(archiveStore.findArchivedJob(id)).thenReturn(Optional.empty());
    assertEquals(
        new IdempotencyKeyHolder(id, Retention.PURGED, null, null),
        service.findIdempotencyKeyHolder("key").orElseThrow());
  }

  @Test
  void missingArchiveCapabilityReportsPurgedWithoutArchiveRead() {
    service = newService(null);
    UUID id = ledgerEntry();
    when(jobCrudStore.findById(id)).thenReturn(Optional.empty());
    assertEquals(
        new IdempotencyKeyHolder(id, Retention.PURGED, null, null),
        service.findIdempotencyKeyHolder("key").orElseThrow());
    verifyNoInteractions(archiveStore);
  }

  private UUID ledgerEntry() {
    UUID id = UUID.randomUUID();
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey("key")).thenReturn(Optional.of(id));
    return id;
  }
}
