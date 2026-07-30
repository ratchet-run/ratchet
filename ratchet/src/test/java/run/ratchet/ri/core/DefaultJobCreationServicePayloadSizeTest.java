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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.PayloadTooLargeException;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.ri.testsupport.StubAfterCommitRegistrar;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.converter.JobPayloadConverter;
import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

@ExtendWith(MockitoExtension.class)
class DefaultJobCreationServicePayloadSizeTest {

  private static final String OVERSIZED_ARGUMENT = "é".repeat(600);

  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobBulkStore jobBulkStore;
  @Mock private BatchStore batchStore;
  @Mock private TagStore tagStore;
  @Mock private WorkflowConditionStore workflowConditionStore;
  @Mock private RecurringJobStore recurringJobStore;
  @Mock private RecurringScheduler recurringScheduler;

  private CountingSerializer serializer;
  private DefaultJobCreationService service;

  @BeforeEach
  void setUp() {
    serializer = new CountingSerializer();
    PayloadSerializerHolder.set(serializer);
    service = newService();
  }

  @AfterEach
  void resetSerializer() {
    PayloadSerializerHolder.set(null);
  }

  @Test
  void oversizedOneOffFailsBeforeCreate() {
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    serializer.oversizeAtCall = 1;

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServicePayloadSizeTest::noop, Duration.ofMinutes(5));

    assertThrows(PayloadTooLargeException.class, () -> service.submit(builder));
    verify(jobCrudStore, never()).create(any());
  }

  @Test
  void oversizedCallbackFailsBeforeCreate() {
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    serializer.oversizeAtCall = 2;

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                    service, DefaultJobCreationServicePayloadSizeTest::noop, Duration.ZERO)
                .onSuccess(DefaultJobCreationServicePayloadSizeTest::callback);

    assertThrows(PayloadTooLargeException.class, () -> service.submit(builder));
    verify(jobCrudStore, never()).create(any());
  }

  @Test
  void oversizedBatchChildIsNeverBulkInserted() {
    when(jobCrudStore.create(any())).thenAnswer(invocation -> saved(invocation.getArgument(0)));

    DefaultBatchBuilder builder = new DefaultBatchBuilder("payload-limit", service);
    builder.forEach(List.of(OVERSIZED_ARGUMENT), DefaultJobCreationServicePayloadSizeTest::consume);

    assertThrows(PayloadTooLargeException.class, () -> service.submit(builder));
    verify(jobBulkStore, never()).bulkInsert(any());
  }

  @Test
  void oversizedStreamingChildIsNeverBulkInserted() {
    when(jobCrudStore.create(any())).thenAnswer(invocation -> saved(invocation.getArgument(0)));

    DefaultStreamingBatchBuilder<String> builder =
        new DefaultStreamingBatchBuilder<>("payload-limit", service);
    builder.fromStream(Stream.of(OVERSIZED_ARGUMENT));
    builder.process(DefaultJobCreationServicePayloadSizeTest::consume);

    assertThrows(PayloadTooLargeException.class, () -> service.submit(builder));
    verify(jobBulkStore, never()).bulkInsert(any());
  }

  @Test
  void oversizedRecurringDefinitionFailsBeforeCreate() {
    serializer.oversizeAtCall = 1;
    DefaultRecurringJobBuilder builder =
        new DefaultRecurringJobBuilder(
            "0 0 * * * ?",
            ZoneId.of("UTC"),
            DefaultJobCreationServicePayloadSizeTest::noop,
            service);

    assertThrows(PayloadTooLargeException.class, () -> service.submit(builder));
    verify(recurringJobStore, never()).createRecurring(any());
  }

  @Test
  void acceptedPayloadUsesPreparedJsonExactlyOnce() {
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    when(jobCrudStore.create(any()))
        .thenAnswer(
            invocation -> {
              JobEntity job = invocation.getArgument(0);
              new JobPayloadConverter().convertToDatabaseColumn(job.getPayload());
              return saved(job);
            });

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServicePayloadSizeTest::noop, Duration.ZERO);

    service.submit(builder);

    assertEquals(1, serializer.serializeCount);
  }

  @Test
  void failedPersistenceCannotLeavePreparedJsonForRetry() {
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    JobPayload[] captured = new JobPayload[1];
    when(jobCrudStore.create(any()))
        .thenAnswer(
            invocation -> {
              JobEntity job = invocation.getArgument(0);
              captured[0] = job.getPayload();
              new JobPayloadConverter().convertToDatabaseColumn(captured[0]);
              throw new IllegalStateException("write failed");
            });

    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServicePayloadSizeTest::noop, Duration.ZERO);

    assertThrows(IllegalStateException.class, () -> service.submit(builder));
    new JobPayloadConverter().convertToDatabaseColumn(captured[0]);

    assertEquals(2, serializer.serializeCount);
  }

  private DefaultJobCreationService newService() {
    RatchetOptions options =
        RatchetOptions.builder().payload(payload -> payload.maxPayloadKb(1)).build();
    return new DefaultJobCreationService(
        jobBatchStatusStore,
        jobTerminalStore,
        jobCrudStore,
        jobBulkStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        recurringJobStore,
        new NoopWakeupService(),
        recurringScheduler,
        new DefaultJobInvocationResolver(),
        new JobPayloadInputValidator(options),
        null,
        null,
        null,
        null,
        null,
        null,
        Clock.systemUTC(),
        new StubAfterCommitRegistrar());
  }

  private static JobEntity saved(JobEntity job) {
    if (job.getId() == null) {
      job.setId(UUID.randomUUID());
    }
    return job;
  }

  public static void noop() {}

  public static void consume(String value) {}

  public static void callback(JobContext context) {}

  private static final class NoopWakeupService extends JobWakeupService {

    @Override
    public void notify(JobPriority priority, boolean immediate, String executionTarget) {}

    @Override
    public void notifyIfNeeded(
        JobExecutionType jobType, JobPriority priority, Duration delay, String executionTarget) {}
  }

  private static final class CountingSerializer implements PayloadSerializer {

    private final PayloadSerializer delegate = new JsonbTestPayloadSerializer();
    private int serializeCount;
    private int oversizeAtCall = -1;

    @Override
    public String serialize(Object payload) {
      serializeCount++;
      if (serializeCount == oversizeAtCall) {
        return "x".repeat(1025);
      }
      return delegate.serialize(payload);
    }

    @Override
    public <T> T deserialize(String json, Class<T> type) {
      return delegate.deserialize(json, type);
    }
  }
}
