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
package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Priority;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.cdi.internal.DefaultRatchetLifecycle;

/**
 * CDI fires multiple observers of the same event in ascending {@code @Priority} order. On
 * build-time-CDI runtimes (Quarkus), {@code RatchetRuntimeStart} must preserve the same effective
 * startup order as plain Jakarta EE: install encryption before the poller can claim encrypted jobs,
 * run lifecycle {@code beforeStart} hooks before any store-writing startup work, and register
 * {@code @Recurring} jobs after the schema is initialized. This test fails loudly if a future edit
 * changes one of the three priorities without updating the others.
 */
class RatchetRuntimeStartObserverOrderingTest {

  @Test
  void onRuntimeStartObservers_installEncryptionAndStartLifecycleBeforeRecurringRegistration()
      throws NoSuchMethodException {
    int recurringJobProcessorPriority = priorityOf(RecurringJobProcessor.class, "onRuntimeStart");
    int encryptionInstallerPriority = priorityOf(EncryptionInstaller.class, "onRuntimeStart");
    int defaultRatchetLifecyclePriority =
        priorityOf(DefaultRatchetLifecycle.class, "onRuntimeStart");

    assertTrue(
        encryptionInstallerPriority < defaultRatchetLifecyclePriority,
        "EncryptionInstaller.onRuntimeStart must fire before"
            + " DefaultRatchetLifecycle.onRuntimeStart so encryption is installed before the"
            + " poller starts");
    assertTrue(
        defaultRatchetLifecyclePriority < recurringJobProcessorPriority,
        "DefaultRatchetLifecycle.onRuntimeStart must fire before"
            + " RecurringJobProcessor.onRuntimeStart so schema migration runs before recurring"
            + " registration"
            + " (lower @Priority value = fires first)");
  }

  private static int priorityOf(Class<?> declaringClass, String methodName)
      throws NoSuchMethodException {
    Method method = declaringClass.getDeclaredMethod(methodName, RatchetRuntimeStart.class);
    for (Parameter parameter : method.getParameters()) {
      Priority priority = parameter.getAnnotation(Priority.class);
      if (priority != null) {
        return priority.value();
      }
    }
    throw new AssertionError(
        declaringClass.getSimpleName()
            + "."
            + methodName
            + " has no @Priority on its event"
            + " parameter");
  }
}
