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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.ri.core.internal.JakartaAfterCommitRegistrar;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.DefaultErrorSanitizer;
import run.ratchet.ri.security.JobPayloadInputValidator;
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
class DefaultJobCreationServiceBatchIdempotencyTest {

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

  public static void consumeString(String value) {}

  public static void recordProgress(BatchContext context) {}

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
        true,
        true,
        null,
        new JakartaAfterCommitRegistrar(),
        new DefaultErrorSanitizer());
  }

  @Test
  void batchDuplicateReturnsOriginalWithoutWrites() {
    DefaultJobCreationService service = newService();
    UUID existingId = UUID.randomUUID();
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey("batch-42"))
        .thenReturn(Optional.of(existingId));
    DefaultBatchBuilder builder = batchBuilder(service);
    builder.withIdempotencyKey(" batch-42 ");
    builder.onProgress(DefaultJobCreationServiceBatchIdempotencyTest::recordProgress);
    builder.thenOnBatchSuccess(DefaultJobCreationServiceBatchIdempotencyTest::noopTask);

    assertEquals(existingId, builder.submit().id());

    assertOnlyLookup("batch-42");
  }

  @Test
  void batchNewKeyIsStoredOnParent() {
    DefaultJobCreationService service = newService();
    stubCreate();
    DefaultBatchBuilder builder = batchBuilder(service);
    builder.withIdempotencyKey(" batch-42 ");

    JobHandle handle = builder.submit();

    assertParentKey(handle, "batch-42");
    verify(jobCrudStore).findOriginalJobIdByIdempotencyKey("batch-42");
  }

  @Test
  void batchWithoutKeyKeepsGeneratedParentKeyAndSkipsLookup() {
    DefaultJobCreationService service = newService();
    stubCreate();

    JobHandle handle = batchBuilder(service).submit();

    ArgumentCaptor<JobEntity> parent = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).create(parent.capture());
    UUID.fromString(parent.getValue().getIdempotencyKey());
    assertEquals(handle.id(), parent.getValue().getId());
    verify(jobCrudStore, never()).findOriginalJobIdByIdempotencyKey(anyString());
  }

  @Test
  void streamingDuplicateClosesStreamWithoutReadingOrWriting() {
    DefaultJobCreationService service = newService();
    UUID existingId = UUID.randomUUID();
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey("stream-42"))
        .thenReturn(Optional.of(existingId));
    AtomicBoolean closed = new AtomicBoolean();
    Stream<String> source =
        Stream.of("one")
            .peek(
                item -> {
                  throw new AssertionError("Duplicate submission must not read the stream");
                })
            .onClose(() -> closed.set(true));
    @SuppressWarnings("unchecked")
    Stream<String> stream = mock(Stream.class, delegatesTo(source));
    DefaultStreamingBatchBuilder<String> builder = streamingBuilder(service, stream);
    builder.withIdempotencyKey(" stream-42 ");
    builder.onBatchProgress(DefaultJobCreationServiceBatchIdempotencyTest::recordProgress);
    builder.onProgress(
        context -> {
          throw new AssertionError("Unexpected streaming progress");
        });
    builder.thenOnBatchSuccess(DefaultJobCreationServiceBatchIdempotencyTest::noopTask);

    assertEquals(existingId, builder.start().id());

    assertTrue(closed.get());
    verify(stream).close();
    verifyNoMoreInteractions(stream);
    assertOnlyLookup("stream-42");
  }

  @Test
  void streamingNewKeyIsStoredOnParent() {
    DefaultJobCreationService service = newService();
    stubCreate();
    DefaultStreamingBatchBuilder<String> builder = streamingBuilder(service, Stream.of("one"));
    builder.withIdempotencyKey(" stream-42 ");

    JobHandle handle = builder.start();

    assertParentKey(handle, "stream-42");
    verify(jobCrudStore).findOriginalJobIdByIdempotencyKey("stream-42");
  }

  @Test
  void streamingWithoutKeyKeepsGeneratedParentKeyAndSkipsLookup() {
    DefaultJobCreationService service = newService();
    stubCreate();

    streamingBuilder(service, Stream.of("one")).start();

    ArgumentCaptor<JobEntity> parent = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).create(parent.capture());
    UUID.fromString(parent.getValue().getIdempotencyKey());
    verify(jobCrudStore, never()).findOriginalJobIdByIdempotencyKey(anyString());
  }

  @Test
  void streamingRequiresReadyBuilderBeforeLookingUpKey() {
    DefaultStreamingBatchBuilder<String> builder =
        new DefaultStreamingBatchBuilder<>("batch", newService());
    @SuppressWarnings("unchecked")
    Stream<String> stream = mock(Stream.class);
    builder.fromStream(stream);
    builder.withIdempotencyKey("stream-42");

    assertThrows(IllegalStateException.class, builder::start);

    verify(stream).close();
    verifyNoMoreInteractions(stream);
    verifyNoInteractions(jobCrudStore, jobBulkStore, batchStore);
  }

  @Test
  void parentInsertRacePropagatesBeforeChildrenOrBatchWrites() {
    DefaultJobCreationService service = newService();
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey("batch-42")).thenReturn(Optional.empty());
    DuplicateIdempotencyKeyException collision =
        new DuplicateIdempotencyKeyException("batch-42", new RuntimeException("duplicate"));
    when(jobCrudStore.create(any(JobEntity.class))).thenThrow(collision);
    DefaultBatchBuilder builder = batchBuilder(service);
    builder.withIdempotencyKey("batch-42");

    assertSame(collision, assertThrows(DuplicateIdempotencyKeyException.class, builder::submit));

    verifyNoInteractions(jobBulkStore, batchStore, workflowConditionStore);
  }

  @Test
  void streamingParentInsertRaceClosesStreamWithoutReadingIt() {
    DefaultJobCreationService service = newService();
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey("stream-42")).thenReturn(Optional.empty());
    DuplicateIdempotencyKeyException collision =
        new DuplicateIdempotencyKeyException("stream-42", new RuntimeException("duplicate"));
    when(jobCrudStore.create(any(JobEntity.class))).thenThrow(collision);
    @SuppressWarnings("unchecked")
    Stream<String> stream = mock(Stream.class);
    DefaultStreamingBatchBuilder<String> builder = streamingBuilder(service, stream);
    builder.withIdempotencyKey("stream-42");

    assertSame(collision, assertThrows(DuplicateIdempotencyKeyException.class, builder::start));

    verify(stream).close();
    verifyNoMoreInteractions(stream);
    verifyNoInteractions(jobBulkStore, batchStore, workflowConditionStore);
  }

  @Test
  void streamCloseFailureDoesNotHideSubmitFailure() {
    DefaultJobCreationService service = newService();
    when(jobCrudStore.findOriginalJobIdByIdempotencyKey("stream-42")).thenReturn(Optional.empty());
    DuplicateIdempotencyKeyException collision =
        new DuplicateIdempotencyKeyException("stream-42", new RuntimeException("duplicate"));
    when(jobCrudStore.create(any(JobEntity.class))).thenThrow(collision);
    IllegalStateException closeFailure = new IllegalStateException("close failed");
    Stream<String> stream =
        Stream.of("one")
            .onClose(
                () -> {
                  throw closeFailure;
                });
    DefaultStreamingBatchBuilder<String> builder = streamingBuilder(service, stream);
    builder.withIdempotencyKey("stream-42");

    assertSame(collision, assertThrows(DuplicateIdempotencyKeyException.class, builder::start));

    assertSame(closeFailure, collision.getSuppressed()[0]);
  }

  private static DefaultBatchBuilder batchBuilder(DefaultJobCreationService service) {
    DefaultBatchBuilder builder = new DefaultBatchBuilder("batch", service);
    builder.forEach(
        List.of("one", "two"), DefaultJobCreationServiceBatchIdempotencyTest::consumeString);
    return builder;
  }

  private static DefaultStreamingBatchBuilder<String> streamingBuilder(
      DefaultJobCreationService service, Stream<String> stream) {
    DefaultStreamingBatchBuilder<String> builder =
        new DefaultStreamingBatchBuilder<>("batch", service);
    builder.fromStream(stream);
    builder.process(DefaultJobCreationServiceBatchIdempotencyTest::consumeString);
    return builder;
  }

  private void stubCreate() {
    when(jobCrudStore.create(any(JobEntity.class)))
        .thenAnswer(
            invocation -> {
              JobEntity job = invocation.getArgument(0);
              job.setId(UUID.randomUUID());
              return job;
            });
  }

  private void assertParentKey(JobHandle handle, String key) {
    ArgumentCaptor<JobEntity> parent = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).create(parent.capture());
    assertEquals(JobExecutionType.BATCH_PARENT, parent.getValue().getJobType());
    assertEquals(key, parent.getValue().getIdempotencyKey());
    assertEquals(handle.id(), parent.getValue().getId());
  }

  private void assertOnlyLookup(String key) {
    verify(jobCrudStore).findOriginalJobIdByIdempotencyKey(key);
    verifyNoMoreInteractions(jobCrudStore);
    verifyNoInteractions(
        jobBulkStore,
        batchStore,
        workflowConditionStore,
        jobBatchStatusStore,
        jobTerminalStore,
        tagStore,
        recurringJobStore);
  }
}
