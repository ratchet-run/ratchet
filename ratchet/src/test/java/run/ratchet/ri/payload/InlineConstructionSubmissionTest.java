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
package run.ratchet.ri.payload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.exception.PayloadTooLargeException;
import run.ratchet.ri.core.DefaultJobCreationService;
import run.ratchet.ri.core.DefaultJobSchedulerService;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.internal.JakartaAfterCommitRegistrar;
import run.ratchet.ri.core.internal.JobPayloadInvoker;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.ClassPolicy;
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

public class InlineConstructionSubmissionTest {
  private final JsonbTestPayloadSerializer serializer = new JsonbTestPayloadSerializer();
  private final JobCrudStore store = mock(JobCrudStore.class);
  private JobPayload stored;

  public static final class Order {
    static int constructions;
    private String id;
    private int quantity;

    public Order() {}

    public Order(String id, int quantity) {
      constructions++;
      this.id = id;
      this.quantity = quantity;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public int getQuantity() {
      return quantity;
    }

    public void setQuantity(int quantity) {
      this.quantity = quantity;
    }
  }

  public static String receive(Order order) {
    return order.getId() + ":" + order.getQuantity();
  }

  public enum Mode {
    FAST
  }

  public static String receiveMode(Mode mode) {
    return mode.name();
  }

  @BeforeEach
  void setup() {
    Order.constructions = 0;
    PayloadSerializerHolder.set(serializer);
  }

  @AfterEach
  void cleanup() {
    PayloadSerializerHolder.set(null);
  }

  @Test
  void submitPersistsAndInvokesTheConstructedValue() throws Exception {
    when(store.create(any(JobEntity.class)))
        .thenAnswer(
            call -> {
              JobEntity job = call.getArgument(0);
              job.setId(UUID.randomUUID());
              JobPayloadConverter converter = new JobPayloadConverter();
              stored =
                  converter.convertToEntityAttribute(
                      converter.convertToDatabaseColumn(job.getPayload()));
              return job;
            });
    DefaultJobCreationService service = service(name -> true);
    JobBuilder builder = scheduler(service).enqueue(() -> receive(new Order("o-1", 3)));
    assertEquals(0, Order.constructions);
    service.submit(builder);
    assertEquals(1, Order.constructions);
    JobPayloadInvoker invoker = new JobPayloadInvoker(null, name -> true);
    assertEquals("o-1:3", invoker.invoke(invoker.materializeArguments(stored, serializer)));
  }

  @Test
  void deniedTargetDoesNotRunTheConstructor() {
    DefaultJobCreationService service = service(name -> false);
    JobBuilder builder = scheduler(service).enqueue(() -> receive(new Order("denied", 1)));
    assertThrows(SecurityException.class, () -> service.submit(builder));
    assertEquals(0, Order.constructions);
  }

  @Test
  void symbolicEnumUsesTheExistingPayloadFormatAtExecution() throws Exception {
    JobPayload payload =
        JobPayloadFactory.fromLambda((SerializableCheckedRunnable) () -> receiveMode(Mode.FAST));
    new JobPayloadInputValidator().validateAtCreation(payload);
    JobPayloadConverter converter = new JobPayloadConverter();
    JobPayload persisted =
        converter.convertToEntityAttribute(converter.convertToDatabaseColumn(payload));
    JobPayloadInvoker invoker = new JobPayloadInvoker(null, name -> true);
    assertEquals("FAST", invoker.invoke(invoker.materializeArguments(persisted, serializer)));
  }

  @Test
  void constructedArgumentsUseTheSamePayloadSizeValidationAsCapturedValues() {
    String oversized = "x".repeat(2000);
    DefaultJobCreationService service = service(name -> true);
    JobBuilder builder = scheduler(service).enqueue(() -> receive(new Order(oversized, 3)));
    assertThrows(PayloadTooLargeException.class, builder::submit);
    assertEquals(1, Order.constructions);
  }

  @Test
  void capturedAndInlineEnumsSerializeToTheSameShape() {
    Mode captured = Mode.FAST;
    JobPayload inline =
        JobPayloadFactory.fromLambda((SerializableCheckedRunnable) () -> receiveMode(Mode.FAST));
    JobPayload capturedPayload =
        JobPayloadFactory.fromLambda((SerializableCheckedRunnable) () -> receiveMode(captured));
    assertEquals(serializer.serialize(capturedPayload), serializer.serialize(inline));
  }

  private DefaultJobSchedulerService scheduler(DefaultJobCreationService service) {
    return new DefaultJobSchedulerService(
        null,
        null,
        null,
        null,
        null,
        store,
        null,
        null,
        null,
        null,
        null,
        null,
        new DefaultJobInvocationResolver(),
        service,
        null,
        null,
        null,
        serializer,
        null,
        Clock.systemUTC(),
        null,
        new JakartaAfterCommitRegistrar());
  }

  private DefaultJobCreationService service(ClassPolicy policy) {
    return new DefaultJobCreationService(
        mock(JobBatchStatusStore.class),
        mock(JobTerminalStore.class),
        store,
        mock(JobBulkStore.class),
        mock(BatchStore.class),
        mock(TagStore.class),
        mock(WorkflowConditionStore.class),
        mock(RecurringJobStore.class),
        new NoopWakeup(),
        mock(RecurringScheduler.class),
        new DefaultJobInvocationResolver(),
        new JobPayloadInputValidator(
            RatchetOptions.builder().payload(p -> p.maxPayloadKb(1)).build()),
        null,
        null,
        null,
        policy,
        null,
        null,
        Clock.systemUTC(),
        true,
        true,
        null,
        new JakartaAfterCommitRegistrar());
  }

  private static final class NoopWakeup extends JobWakeupService {
    @Override
    public void notify(JobPriority priority, boolean immediate, String target) {}

    @Override
    public void notifyIfNeeded(
        JobExecutionType type, JobPriority priority, Duration delay, String target) {}
  }
}
