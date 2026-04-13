package run.ratchet.ri.cdi;

import run.ratchet.api.JobPriority;
import run.ratchet.api.Recurring;

/**
 * Parses {@code @Recurring} annotation values: enabled flag (with {@code ${prop}} resolution),
 * numeric-to-{@link JobPriority} mapping, and job ID generation.
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

  /**
   * Returns true if the job should be registered. Supports {@code "${prop}"} and {@code
   * "${prop:default}"} placeholder syntax resolved from system properties then env vars.
   */
  static boolean isEnabled(Recurring annotation) {
    String value = annotation.enabled();

    if (value.startsWith("${") && value.endsWith("}")) {
      return resolvePropertyPlaceholder(value);
    }

    return Boolean.parseBoolean(value);
  }

  /**
   * Maps annotation priority (1-10) to {@link JobPriority}: 1-2 LOWEST, 3-4 LOW, 5-6 NORMAL, 7-8
   * HIGH, 9-10 CRITICAL.
   */
  static JobPriority mapPriority(int priority) {
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

  private static boolean resolvePropertyPlaceholder(String placeholder) {
    String expr = placeholder.substring(2, placeholder.length() - 1);
    int colonIdx = expr.lastIndexOf(':');

    String propName = colonIdx > 0 ? expr.substring(0, colonIdx) : expr;
    String defaultVal = colonIdx > 0 ? expr.substring(colonIdx + 1) : "true";

    // Try system property first
    String resolved = System.getProperty(propName);

    // Fall back to environment variable (dots -> underscores, uppercase)
    if (resolved == null) {
      String envName = propName.replace('.', '_').toUpperCase();
      resolved = System.getenv(envName);
    }

    return Boolean.parseBoolean(resolved != null ? resolved : defaultVal);
  }
}
