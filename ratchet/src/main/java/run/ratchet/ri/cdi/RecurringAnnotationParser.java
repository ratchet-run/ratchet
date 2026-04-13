package run.ratchet.ri.cdi;

import run.ratchet.api.JobPriority;
import run.ratchet.api.Recurring;

/**
 * Parses {@code @Recurring} annotation values and resolves configuration.
 *
 * <p>Provides focused, testable parsing of recurring job configuration including:
 *
 * <ul>
 *   <li>Property placeholder resolution for the enabled field
 *   <li>Priority mapping from numeric values (1-10) to {@link JobPriority} enum
 *   <li>Job ID generation from annotation or class/method names
 * </ul>
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
   * Determines if a recurring job is enabled based on its annotation configuration.
   *
   * <p>Supports property placeholder syntax for runtime configuration:
   *
   * <ul>
   *   <li>{@code "true"} or {@code "false"} - parsed directly
   *   <li>{@code "${property.name}"} - reads system property or environment variable
   *   <li>{@code "${property.name:default}"} - reads property with fallback default
   * </ul>
   *
   * @param annotation the @Recurring annotation to check
   * @return true if the job should be registered, false to skip
   */
  static boolean isEnabled(Recurring annotation) {
    String value = annotation.enabled();

    if (value.startsWith("${") && value.endsWith("}")) {
      return resolvePropertyPlaceholder(value);
    }

    return Boolean.parseBoolean(value);
  }

  /**
   * Maps a numeric priority value (1-10) to a {@link JobPriority} enum value.
   *
   * <p>The mapping quantizes the 1-10 range into 5 discrete priority levels:
   *
   * <ul>
   *   <li>1-2: {@link JobPriority#LOWEST}
   *   <li>3-4: {@link JobPriority#LOW}
   *   <li>5-6: {@link JobPriority#NORMAL}
   *   <li>7-8: {@link JobPriority#HIGH}
   *   <li>9-10: {@link JobPriority#CRITICAL}
   * </ul>
   *
   * @param priority the numeric priority from the annotation (1-10)
   * @return the corresponding JobPriority enum value
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
