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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.event.JobCallbackFailedEvent;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;

class LifecycleCallbackInvokerTest {
  @Test
  void missingContextStillInvokesCallbackWithNull() throws Exception {
    JobMdcContext.clear();
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    JobPayload callback =
        new JobPayload(
            "Target",
            "success",
            "(Ljava/lang/Object;)V",
            true,
            Arrays.asList((Object) null),
            List.of(0));
    job.setOnSuccessPayload(callback);
    PreExecutionValidator validator = mock(PreExecutionValidator.class);
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    PayloadSerializer serializer = mock(PayloadSerializer.class);
    ExecutionObserver observer = mock(ExecutionObserver.class);
    when(invoker.materializeArguments(any(), eq(serializer))).thenAnswer(inv -> inv.getArgument(0));
    new LifecycleCallbackInvoker(validator, invoker, serializer, observer, Clock.systemUTC())
        .invokeOnSuccess(job);
    ArgumentCaptor<JobPayload> invoked = ArgumentCaptor.forClass(JobPayload.class);
    verify(invoker).invoke(invoked.capture());
    assertEquals(Arrays.asList((Object) null), invoked.getValue().args());
    verify(observer, never()).recordCallbackFailure(any(), any(), eq(1));
  }

  @Test
  void materializeSeesBoundRuntimeValues() throws Exception {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setOnFailurePayload(
        new JobPayload(
            "Target",
            "failure",
            "(Ljava/lang/Object;Ljava/lang/Throwable;)V",
            true,
            Arrays.asList(null, null),
            List.of(0, 1)));
    PreExecutionValidator validator = mock(PreExecutionValidator.class);
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    PayloadSerializer serializer = mock(PayloadSerializer.class);
    ExecutionObserver observer = mock(ExecutionObserver.class);
    when(invoker.materializeArguments(any(), eq(serializer))).thenAnswer(inv -> inv.getArgument(0));
    RuntimeException ex = new RuntimeException("original");

    new LifecycleCallbackInvoker(validator, invoker, serializer, observer, Clock.systemUTC())
        .invokeOnFailure(job, ex);

    ArgumentCaptor<JobPayload> materialized = ArgumentCaptor.forClass(JobPayload.class);
    verify(invoker).materializeArguments(materialized.capture(), eq(serializer));
    assertSame(ex, materialized.getValue().args().get(1));
  }

  @AfterEach
  void clearContext() {
    JobMdcContext.clear();
  }

  @Test
  void unavailableRuntimeParameterReportsCallbackFailureWithoutInvokingTarget() throws Exception {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    JobPayload callback =
        new JobPayload(
            "Target",
            "success",
            "(Ljava/lang/Object;)V",
            true,
            Arrays.asList((Object) null),
            List.of(1));
    job.setOnSuccessPayload(callback);
    PreExecutionValidator validator = mock(PreExecutionValidator.class);
    JobPayloadInvoker invoker = mock(JobPayloadInvoker.class);
    PayloadSerializer serializer = mock(PayloadSerializer.class);
    ExecutionObserver observer = mock(ExecutionObserver.class);
    // Binding rejects the unavailable slot before materialization.

    new LifecycleCallbackInvoker(validator, invoker, serializer, observer, Clock.systemUTC())
        .invokeOnSuccess(job);

    verify(validator).validateSecurity(callback);
    verify(invoker, never()).invoke(any());
    verify(invoker, never()).materializeArguments(any(), any());
    ArgumentCaptor<Throwable> failure = ArgumentCaptor.forClass(Throwable.class);
    verify(observer).recordCallbackFailure(eq(job), failure.capture(), eq(1));
    assertEquals(IllegalStateException.class, failure.getValue().getClass());
    assertEquals(
        "Job "
            + job.getId()
            + " onSuccess callback expects runtime parameter 1 but only 1 is supplied",
        failure.getValue().getMessage());
    ArgumentCaptor<JobCallbackFailedEvent> event =
        ArgumentCaptor.forClass(JobCallbackFailedEvent.class);
    verify(observer).publishEvent(event.capture());
    assertEquals(
        JobCallbackFailedEvent.CallbackType.ON_SUCCESS, event.getValue().getCallbackType());
  }
}
