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

import run.ratchet.api.JobPriority;
import run.ratchet.api.Recurring;

/**
 * Parses {@code @Recurring} annotation values: enabled flag, numeric-to-{@link JobPriority}
 * mapping, and job ID generation.
 *
 * @see RecurringJobProcessor
 */
final class RecurringAnnotationParser {

  private RecurringAnnotationParser() {}

  /** Returns the explicit annotation ID if set, otherwise {@code className.methodName}. */
  static String generateJobId(Recurring annotation, String className, String methodName) {
    if (!annotation.id().isEmpty()) {
      return annotation.id();
    }
    return className + "." + methodName;
  }

  /** Returns true if the job should be registered. */
  static boolean isEnabled(Recurring annotation) {
    return annotation.enabled();
  }

  /**
   * Maps annotation priority (1-10) to {@link JobPriority}: 1-2 LOWEST, 3-4 LOW, 5-6 NORMAL, 7-8
   * HIGH, 9-10 CRITICAL.
   */
  static JobPriority mapPriority(int priority) {
    if (priority < 1 || priority > 10) {
      throw new IllegalArgumentException(
          "@Recurring priority must be between 1 and 10: " + priority);
    }
    if (priority <= 2) {
      return JobPriority.LOWEST;
    } else if (priority <= 4) {
      return JobPriority.LOW;
    } else if (priority <= 6) {
      return JobPriority.NORMAL;
    } else if (priority <= 8) {
      return JobPriority.HIGH;
    } else {
      return JobPriority.CRITICAL;
    }
  }
}
