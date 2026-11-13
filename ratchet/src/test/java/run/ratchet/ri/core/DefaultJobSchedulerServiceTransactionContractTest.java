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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.transaction.Transactional;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobOptions;
import run.ratchet.api.ResourceLimit;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SignalDecision;
import run.ratchet.ri.core.internal.DefaultRecurringAnnotationMaintenanceService;

class DefaultJobSchedulerServiceTransactionContractTest {

  @Test
  void businessKeyLookupSupportsCallerTransactionAndInvalidKeysDoNotRollBack()
      throws NoSuchMethodException {
    assertSupports("findBusinessKeyHolder", String.class);
    Transactional boundary =
        DefaultJobSchedulerService.class
            .getMethod("findBusinessKeyHolder", String.class)
            .getAnnotation(Transactional.class);
    assertTrue(Arrays.asList(boundary.dontRollbackOn()).contains(IllegalArgumentException.class));
  }

  @Test
  void listenerRegistration_isNotSupportedTransactionAttribute() throws NoSuchMethodException {
    assertNotSupported("addEventListener");
    assertNotSupported("removeEventListener");
  }

  @Test
  void builderFactories_areSupportsTransactionAttribute() throws NoSuchMethodException {
    assertSupports("enqueue", SerializableCheckedRunnable.class);
    assertSupports("schedule", Duration.class, SerializableCheckedRunnable.class);
    assertSupports("enqueueBatch", String.class);
    assertSupports("streamingBatch", String.class);
    assertSupports(
        "scheduleRecurring", String.class, ZoneId.class, SerializableCheckedRunnable.class);
  }

  @Test
  void mutatingMethods_areRequiredTransactionAttribute() throws NoSuchMethodException {
    assertRequired("cancelJob", UUID.class);
    assertRequired("pauseJob", UUID.class);
    assertRequired("resumeJob", UUID.class);
    assertRequired("retryJob", UUID.class);
    assertRequired("deliverSignal", UUID.class, Serializable.class);
    assertRequired("deliverSignal", UUID.class, SignalDecision.class);
    assertRequired("deliverSignal", String.class, Serializable.class);
    assertRequired("deliverSignal", String.class, SignalDecision.class);
    assertRequired("cancelJobsByTag", String.class);
    assertRequired("cancelRecurringJobsByTag", String.class);
    assertRequired("cancelRecurringJobByBusinessKey", String.class);
    assertEquals(
        Transactional.TxType.REQUIRED,
        DefaultRecurringAnnotationMaintenanceService.class
            .getMethod("cancelOrphanedRecurringAnnotationJobs", Set.class, Instant.class)
            .getAnnotation(Transactional.class)
            .value());
    assertRequired(
        "replace", UUID.class, Duration.class, SerializableCheckedRunnable.class, JobOptions.class);
  }

  @Test
  void resourceLimitMethods_declareTheirTransactionAttributes() throws NoSuchMethodException {
    // An argument or capability error is thrown before any write, so it must not mark the
    // caller's transaction rollback-only.
    Transactional set = transactional("setResourceLimit", ResourceLimit.class);
    assertEquals(Transactional.TxType.REQUIRED, set.value());
    assertEquals(
        Set.of(IllegalArgumentException.class, UnsupportedOperationException.class),
        Set.of(set.dontRollbackOn()));

    Transactional find = transactional("findResourceLimit", String.class);
    assertEquals(Transactional.TxType.SUPPORTS, find.value());
    assertEquals(Set.of(IllegalArgumentException.class), Set.of(find.dontRollbackOn()));

    assertSupports("listResourceLimits");
  }

  private static Transactional transactional(String methodName, Class<?>... parameterTypes)
      throws NoSuchMethodException {
    Method method = DefaultJobSchedulerService.class.getMethod(methodName, parameterTypes);
    Transactional transactional = method.getAnnotation(Transactional.class);
    assertNotNull(transactional, methodName + " must declare the public API TX attribute");
    return transactional;
  }

  private static void assertNotSupported(String methodName) throws NoSuchMethodException {
    Method method = DefaultJobSchedulerService.class.getMethod(methodName, Consumer.class);
    Transactional transactional = method.getAnnotation(Transactional.class);

    assertNotNull(transactional, methodName + " must declare the public API TX attribute");
    assertEquals(Transactional.TxType.NOT_SUPPORTED, transactional.value());
  }

  private static void assertSupports(String methodName, Class<?>... parameterTypes)
      throws NoSuchMethodException {
    Method method = DefaultJobSchedulerService.class.getMethod(methodName, parameterTypes);
    Transactional transactional = method.getAnnotation(Transactional.class);

    assertNotNull(transactional, methodName + " must declare the public API TX attribute");
    assertEquals(Transactional.TxType.SUPPORTS, transactional.value());
  }

  private static void assertRequired(String methodName, Class<?>... parameterTypes)
      throws NoSuchMethodException {
    Method method = DefaultJobSchedulerService.class.getMethod(methodName, parameterTypes);
    Transactional transactional = method.getAnnotation(Transactional.class);

    assertNotNull(transactional, methodName + " must declare the public API TX attribute");
    assertEquals(Transactional.TxType.REQUIRED, transactional.value());
  }
}
