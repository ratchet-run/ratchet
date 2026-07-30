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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.ri.testsupport.StubAfterCommitRegistrar;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

@ExtendWith(MockitoExtension.class)
class DefaultJobCreationServiceIdempotencyRaceTest {

  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobBulkStore jobBulkStore;
  @Mock private BatchStore batchStore;
  @Mock private TagStore tagStore;
  @Mock private WorkflowConditionStore workflowConditionStore;
  @Mock private RecurringJobStore recurringJobStore;
  @Mock private RecurringScheduler recurringScheduler;

  public static void noopTask() {}

  private static class NoopJobWakeupService extends JobWakeupService {
    @Override
    public void notify(JobPriority priority, boolean immediate, String executionTarget) {}

    @Override
    public void notifyIfNeeded(
        JobExecutionType jobType, JobPriority priority, Duration delay, String executionTarget) {}
  }

  private DefaultJobCreationService newService() {
    return new DefaultJobCreationService(
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
        new DefaultJobInvocationResolver(),
        new JobPayloadInputValidator(),
        null,
        null,
        null,
        null,
        null,
        null,
        Clock.fixed(Instant.parse("2026-05-27T12:00:00Z"), ZoneOffset.UTC),
        new StubAfterCommitRegistrar());
  }

  @Test
  void submit_convergesToExistingJob_whenIdempotencyKeyRacesOnInsert() {
    DefaultJobCreationService service = newService();
    String key = "order-42";
    UUID existingId = UUID.randomUUID();

    JobEntity existing = new JobEntity();
    existing.setId(existingId);
    existing.setIdempotencyKey(key);

    // No job is visible at the pre-insert lookup (the racer has not committed yet), so the insert
    // proceeds and loses the unique-constraint race. The store reports the collision; the
    // post-insert re-resolve now sees the winner's row.
    when(jobCrudStore.findByIdempotencyKey(key))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(existing));
    when(jobCrudStore.create(any(JobEntity.class)))
        .thenThrow(new DuplicateIdempotencyKeyException(key, new RuntimeException("dup")));

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceIdempotencyRaceTest::noopTask, Duration.ZERO);
    builder.withIdempotencyKey(key);

    JobHandle handle = service.submit(builder);

    assertEquals(existingId, handle.id());
    verify(jobCrudStore, times(1)).create(any(JobEntity.class));
    verify(jobCrudStore, times(2)).findByIdempotencyKey(eq(key));
  }

  @Test
  void submit_returnsExistingJob_whenIdempotencyKeyResolvesBeforeInsert() {
    DefaultJobCreationService service = newService();
    String key = "order-7";
    UUID existingId = UUID.randomUUID();

    JobEntity existing = new JobEntity();
    existing.setId(existingId);
    existing.setIdempotencyKey(key);

    when(jobCrudStore.findByIdempotencyKey(key)).thenReturn(Optional.of(existing));

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceIdempotencyRaceTest::noopTask, Duration.ZERO);
    builder.withIdempotencyKey(key);

    JobHandle handle = service.submit(builder);

    assertEquals(existingId, handle.id());
    // The happy-path short-circuit never reaches create().
    verify(jobCrudStore, times(0)).create(any(JobEntity.class));
  }
}
