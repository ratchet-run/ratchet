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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.transaction.Transactional;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.DuplicateBusinessKeyException;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.api.exception.ResourceNotConfiguredException;
import run.ratchet.ri.core.internal.JakartaAfterCommitRegistrar;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.DefaultErrorSanitizer;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

@ExtendWith(MockitoExtension.class)
class DefaultJobCreationServiceIdempotencyRaceTest {

  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobBulkStore jobBulkStore;
  @Mock private BatchStore batchStore;
  @Mock private TagStore tagStore;
  @Mock private WorkflowConditionStore workflowConditionStore;
  @Mock private RecurringJobStore recurringJobStore;
  @Mock private RecurringScheduler recurringScheduler;
  @Mock private BatchCompletionTransaction conflictResolutionTransaction;

  public static void noopTask() {}

  private static class NoopJobWakeupService extends JobWakeupService {
    @Override
    public void notify(JobPriority priority, boolean immediate, String executionTarget) {}

    @Override
    public void notifyIfNeeded(
        JobExecutionType jobType, JobPriority priority, Duration delay, String executionTarget) {}
  }

  private DefaultJobCreationService newService() {
    return newService(true);
  }

  private DefaultJobCreationService newService(boolean useConflictTransaction) {
    lenient()
        .when(conflictResolutionTransaction.complete(any()))
        .thenAnswer(invocation -> invocation.<Supplier<?>>getArgument(0).get());
    return new DefaultJobCreationService(
        mock(BatchService.class),
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
        true,
        null,
        null,
        new JakartaAfterCommitRegistrar(),
        new DefaultErrorSanitizer(),
        useConflictTransaction ? conflictResolutionTransaction : null);
  }

  @Test
  void submit_retryResolvesOriginalAfterInsertCollision() {
    DefaultJobCreationService service = newService();
    String key = "order-42";
    UUID existingId = UUID.randomUUID();

    JobEntity existing = new JobEntity();
    existing.setId(existingId);
    existing.setIdempotencyKey(key);

    // No job is visible at the pre-insert lookup (the racer has not committed yet), so the insert
    // proceeds and loses the unique-constraint race. The store reports the collision; the
    // fresh retry sees the winner's permanent reservation.
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey(key))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(existing.getId()));
    when(jobCrudStore.create(any(JobEntity.class)))
        .thenThrow(new DuplicateIdempotencyKeyException(key, new RuntimeException("dup")));

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceIdempotencyRaceTest::noopTask, Duration.ZERO);
    builder.withIdempotencyKey(key);

    Assertions.assertThrows(DuplicateIdempotencyKeyException.class, () -> service.submit(builder));
    JobHandle handle = service.submit(builder);

    assertEquals(existingId, handle.id());
    verify(jobCrudStore, times(1)).create(any(JobEntity.class));
    verify(jobCrudStore, times(2)).findOriginalJobIdByIdempotencyKey(eq(key));
  }

  @Test
  void submit_returnsExistingJob_whenRacerCommitsBetweenIdempotencyAndBusinessKeyLookups() {
    DefaultJobCreationService service = newService();
    String key = "runtime-raced";
    UUID existingId = UUID.randomUUID();

    // The racer commits after the idempotency lookup misses but before the business-key lookup,
    // so only the second lookup sees its job.
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey(key))
        .thenReturn(Optional.empty(), Optional.of(existingId));
    when(jobCrudStore.findBusinessKeyHolder("raced"))
        .thenReturn(
            Optional.of(new BusinessKeyHolder(existingId, JobType.SINGLE, JobStatus.PENDING)));

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceIdempotencyRaceTest::noopTask, Duration.ZERO);
    builder.withIdempotencyKey(key).withBusinessKey("raced");

    JobHandle handle = service.submit(builder);

    assertEquals(existingId, handle.id());
    verify(jobCrudStore, times(0)).create(any(JobEntity.class));
  }

  @Test
  void submit_rejectsRecurringHolderBeforeCreate() {
    DefaultJobCreationService service = newService();
    UUID holderId = UUID.randomUUID();
    when(jobCrudStore.findBusinessKeyHolder("raced"))
        .thenReturn(
            Optional.of(new BusinessKeyHolder(holderId, JobType.RECURRING, JobStatus.PENDING)));
    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceIdempotencyRaceTest::noopTask, Duration.ZERO);
    builder.withIdempotencyKey("mine").withBusinessKey("raced");
    DuplicateBusinessKeyException failure =
        Assertions.assertThrows(DuplicateBusinessKeyException.class, () -> service.submit(builder));
    assertTrue(failure.getMessage().contains("(jobId=" + holderId + ")"));
    verify(jobCrudStore, times(0)).create(any(JobEntity.class));
  }

  @Test
  void submit_rejectsHolder_whenIdempotencyKeyMapsToAnotherJob() {
    DefaultJobCreationService service = newService();
    UUID holderId = UUID.randomUUID();
    // The idempotency key resolves on the second lookup, but to a job other than the holder.
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey("mine"))
        .thenReturn(Optional.empty(), Optional.of(UUID.randomUUID()));
    when(jobCrudStore.findBusinessKeyHolder("raced"))
        .thenReturn(
            Optional.of(new BusinessKeyHolder(holderId, JobType.SINGLE, JobStatus.PENDING)));
    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceIdempotencyRaceTest::noopTask, Duration.ZERO);
    builder.withIdempotencyKey("mine").withBusinessKey("raced");
    DuplicateBusinessKeyException failure =
        Assertions.assertThrows(DuplicateBusinessKeyException.class, () -> service.submit(builder));
    assertTrue(failure.getMessage().contains("(jobId=" + holderId + ")"));
    verify(jobCrudStore, times(2)).findOriginalJobIdByIdempotencyKey("mine");
    verify(jobCrudStore, times(0)).create(any(JobEntity.class));
  }

  @Test
  void submit_rejectsActiveBusinessKey_whenIdempotencyKeyDiffers() {
    DefaultJobCreationService service = newService();

    UUID existingId = UUID.randomUUID();

    when(jobCrudStore.findOriginalJobIdByIdempotencyKey("mine")).thenReturn(Optional.empty());
    when(jobCrudStore.findBusinessKeyHolder("raced"))
        .thenReturn(
            Optional.of(new BusinessKeyHolder(existingId, JobType.SINGLE, JobStatus.PENDING)));

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceIdempotencyRaceTest::noopTask, Duration.ZERO);
    builder.withIdempotencyKey("mine").withBusinessKey("raced");

    DuplicateBusinessKeyException thrown =
        Assertions.assertThrows(DuplicateBusinessKeyException.class, () -> service.submit(builder));

    assertEquals("raced", thrown.businessKey());
    assertTrue(thrown.getMessage().contains("business key 'raced'"));
    assertTrue(thrown.getMessage().contains(existingId.toString()));
    verify(jobCrudStore, times(0)).create(any(JobEntity.class));
  }

  @Test
  void submit_businessKeyConflictWithMatchingExplicitKey_returnsOriginal() {
    DefaultJobCreationService service = newService();
    UUID originalId = UUID.randomUUID();
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey("mine"))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(originalId));
    when(jobCrudStore.create(any(JobEntity.class)))
        .thenThrow(new BusinessKeyReservationConflictException("raced"));

    assertEquals(originalId, service.submit(racingBuilder(service, true)).id());
    verify(conflictResolutionTransaction).complete(any());
    verify(jobCrudStore, times(1)).findBusinessKeyHolder("raced");
  }

  @Test
  void submit_businessKeyConflictWithoutTransactionHelper_resolvesOriginalDirectly() {
    DefaultJobCreationService service = newService(false);
    UUID originalId = UUID.randomUUID();
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey("mine"))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(originalId));
    when(jobCrudStore.create(any(JobEntity.class)))
        .thenThrow(new BusinessKeyReservationConflictException("raced"));

    assertEquals(originalId, service.submit(racingBuilder(service, true)).id());
    verify(jobCrudStore, times(2)).findOriginalJobIdByIdempotencyKey("mine");
    verify(conflictResolutionTransaction, times(0)).complete(any());
  }

  @Test
  void submit_businessKeyConflictWithUnmatchedExplicitKey_throwsDuplicate() {
    DefaultJobCreationService service = newService();
    BusinessKeyReservationConflictException collision =
        new BusinessKeyReservationConflictException("raced");
    when(jobCrudStore.create(any(JobEntity.class))).thenThrow(collision);

    DuplicateBusinessKeyException thrown =
        Assertions.assertThrows(
            DuplicateBusinessKeyException.class,
            () -> service.submit(racingBuilder(service, true)));

    assertSame(collision, thrown.getCause());
    assertEquals("raced", thrown.businessKey());
    verify(jobCrudStore, times(2)).findOriginalJobIdByIdempotencyKey("mine");
    verify(conflictResolutionTransaction).complete(any());
    verify(jobCrudStore, times(1)).findBusinessKeyHolder("raced");
  }

  @Test
  void submit_businessKeyConflictWithGeneratedKey_throwsDuplicateWithoutResolutionLookup() {
    DefaultJobCreationService service = newService();
    BusinessKeyReservationConflictException collision =
        new BusinessKeyReservationConflictException("raced");
    when(jobCrudStore.create(any(JobEntity.class))).thenThrow(collision);
    DefaultJobBuilder builder = racingBuilder(service, false);
    builder.withIdempotencyKey(null).withIdempotencyKey(" ");
    assertEquals(builder.idempotencyKey(), UUID.fromString(builder.idempotencyKey()).toString());

    DuplicateBusinessKeyException thrown =
        Assertions.assertThrows(DuplicateBusinessKeyException.class, () -> service.submit(builder));

    assertSame(collision, thrown.getCause());
    verify(jobCrudStore, times(1)).findOriginalJobIdByIdempotencyKey(builder.idempotencyKey());
    verify(jobCrudStore, times(1)).findBusinessKeyHolder("raced");
    verify(conflictResolutionTransaction, times(0)).complete(any());
  }

  @Test
  void submit_businessKeyConflictLookupFailure_rethrowsConflictWithSuppressedFailure() {
    DefaultJobCreationService service = newService();
    BusinessKeyReservationConflictException collision =
        new BusinessKeyReservationConflictException("raced");
    RuntimeException lookupFailure = new IllegalStateException("lookup unavailable");
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey("mine"))
        .thenReturn(Optional.empty())
        .thenThrow(lookupFailure);
    when(jobCrudStore.create(any(JobEntity.class))).thenThrow(collision);
    DefaultJobBuilder builder = racingBuilder(service, true);
    builder.withIdempotencyKey(" ");

    assertSame(
        collision,
        Assertions.assertThrows(
            BusinessKeyReservationConflictException.class, () -> service.submit(builder)));
    assertEquals(1, collision.getSuppressed().length);
    assertSame(lookupFailure, collision.getSuppressed()[0]);
    verify(conflictResolutionTransaction).complete(any());
    verify(jobCrudStore, times(1)).findBusinessKeyHolder("raced");
  }

  @Test
  void submit_exemptsOnlyDuplicateBusinessKeyAndMissingLimitFromRollback() throws Exception {
    Transactional annotation =
        DefaultJobCreationService.class
            .getMethod("submit", JobBuilder.class)
            .getAnnotation(Transactional.class);

    assertEquals(
        Set.of(DuplicateBusinessKeyException.class, ResourceNotConfiguredException.class),
        Set.of(annotation.dontRollbackOn()));
  }

  private DefaultJobBuilder racingBuilder(DefaultJobCreationService service, boolean explicit) {
    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceIdempotencyRaceTest::noopTask, Duration.ZERO);
    builder.withBusinessKey("raced");
    if (explicit) {
      builder.withIdempotencyKey("mine");
    }
    return builder;
  }

  @Test
  void submit_rethrowsNonReservationTransientFailure_withoutRepeatingLookup() {
    DefaultJobCreationService service = newService();
    // Even a reservation-looking message must not trigger business-key translation.
    RatchetTransientStoreException failure =
        new RatchetTransientStoreException("Active business key in use: raced");
    when(jobCrudStore.findBusinessKeyHolder("raced")).thenReturn(Optional.empty());
    when(jobCrudStore.create(any(JobEntity.class))).thenThrow(failure);

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceIdempotencyRaceTest::noopTask, Duration.ZERO);
    builder.withIdempotencyKey("mine").withBusinessKey("raced");

    assertSame(
        failure,
        Assertions.assertThrows(
            RatchetTransientStoreException.class, () -> service.submit(builder)));
    verify(jobCrudStore, times(1)).findBusinessKeyHolder("raced");
    verify(conflictResolutionTransaction, times(0)).complete(any());
  }

  @Test
  void submit_returnsExistingJob_whenIdempotencyKeyResolvesBeforeInsert() {
    DefaultJobCreationService service = newService();
    String key = "order-7";
    UUID existingId = UUID.randomUUID();

    JobEntity existing = new JobEntity();
    existing.setId(existingId);
    existing.setIdempotencyKey(key);

    when(jobCrudStore.findOriginalJobIdByIdempotencyKey(key))
        .thenReturn(Optional.of(existing.getId()));

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
