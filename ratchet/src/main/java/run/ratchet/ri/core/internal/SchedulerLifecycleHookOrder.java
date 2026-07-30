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

import jakarta.annotation.Priority;
import java.util.Comparator;
import run.ratchet.spi.SchedulerLifecycleHook;

/** Shared deterministic ordering for scheduler lifecycle hooks. */
public final class SchedulerLifecycleHookOrder {

  private static final Comparator<SchedulerLifecycleHook> ORDER =
      Comparator.comparingInt(SchedulerLifecycleHookOrder::priorityValue)
          .thenComparing(hook -> hook.getClass().getName());

  private SchedulerLifecycleHookOrder() {}

  /** Returns the shared lifecycle-hook comparator. */
  public static Comparator<SchedulerLifecycleHook> comparator() {
    return ORDER;
  }

  private static int priorityValue(SchedulerLifecycleHook hook) {
    Priority priority = findPriority(hook.getClass());
    return priority == null ? Integer.MAX_VALUE : priority.value();
  }

  private static Priority findPriority(Class<?> type) {
    for (Class<?> current = type;
        current != null && current != Object.class;
        current = current.getSuperclass()) {
      Priority priority = current.getAnnotation(Priority.class);
      if (priority != null) {
        return priority;
      }
      priority = findInterfacePriority(current);
      if (priority != null) {
        return priority;
      }
    }
    return null;
  }

  private static Priority findInterfacePriority(Class<?> type) {
    for (Class<?> iface : type.getInterfaces()) {
      Priority priority = iface.getAnnotation(Priority.class);
      if (priority != null) {
        return priority;
      }
      priority = findInterfacePriority(iface);
      if (priority != null) {
        return priority;
      }
    }
    return null;
  }
}
