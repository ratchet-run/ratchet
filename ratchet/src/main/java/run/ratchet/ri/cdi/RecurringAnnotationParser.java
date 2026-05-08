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
