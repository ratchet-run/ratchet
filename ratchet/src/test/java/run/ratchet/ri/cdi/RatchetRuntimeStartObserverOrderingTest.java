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
 * build-time-CDI runtimes (Quarkus), all three {@code onRuntimeStart(RatchetRuntimeStart)}
 * observers fire in that order, which is the REVERSE of how they run on a plain Jakarta EE server
 * (where {@link EncryptionInstaller#onStartup} installs encryption before {@link
 * DefaultRatchetLifecycle#onStartup} starts the poller, and {@link RecurringJobProcessor#onStartup}
 * is deferred ~500ms after both). Today RecurringJobProcessor's registration has no dependency on
 * encryption or the poller being ready, so the reversed order is harmless — but that's an
 * invariant, not an accident, and nothing else guards it. This test fails loudly if a future edit
 * changes one of the three priorities without updating the others, since that's exactly the kind of
 * change that would silently corrupt or drop an encrypted recurring-job payload only on Quarkus.
 */
class RatchetRuntimeStartObserverOrderingTest {

  @Test
  void onRuntimeStartObservers_fireEncryptionAndPollerBeforeRecurringRegistration()
      throws NoSuchMethodException {
    int recurringJobProcessorPriority = priorityOf(RecurringJobProcessor.class, "onRuntimeStart");
    int encryptionInstallerPriority = priorityOf(EncryptionInstaller.class, "onRuntimeStart");
    int defaultRatchetLifecyclePriority =
        priorityOf(DefaultRatchetLifecycle.class, "onRuntimeStart");

    assertTrue(
        recurringJobProcessorPriority < encryptionInstallerPriority,
        "RecurringJobProcessor.onRuntimeStart must fire before EncryptionInstaller.onRuntimeStart"
            + " (lower @Priority value = fires first)");
    assertTrue(
        encryptionInstallerPriority < defaultRatchetLifecyclePriority,
        "EncryptionInstaller.onRuntimeStart must fire before"
            + " DefaultRatchetLifecycle.onRuntimeStart, per EncryptionInstaller's own ordering"
            + " comment about installing encryption before the poller starts");
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
