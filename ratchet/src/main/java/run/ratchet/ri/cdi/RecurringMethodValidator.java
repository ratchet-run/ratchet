package run.ratchet.ri.cdi;

import run.ratchet.api.JobContext;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Validates method signatures for {@code @Recurring} annotation compatibility.
 *
 * <p>Recurring methods must meet these requirements:
 *
 * <ul>
 *   <li>Must be public (accessible for reflective invocation)
 *   <li>Must have zero or one parameter
 *   <li>If one parameter, it must be assignable to {@link JobContext}
 * </ul>
 *
 * @see RecurringJobProcessor
 */
final class RecurringMethodValidator {

  private RecurringMethodValidator() {}

  /**
   * Validates that a method has a compatible signature for @Recurring.
   *
   * @throws IllegalArgumentException if the method signature is invalid
   */
  static void validate(Method method) {
    if (!Modifier.isPublic(method.getModifiers())) {
      throw new IllegalArgumentException(
          "@Recurring method must be public: " + formatMethodName(method));
    }
    if (Modifier.isStatic(method.getModifiers())) {
      throw new IllegalArgumentException(
          "@Recurring method must not be static: " + formatMethodName(method));
    }
    Class<?>[] paramTypes = method.getParameterTypes();
    if (paramTypes.length > 1) {
      throw new IllegalArgumentException(
          "@Recurring method must have no parameters or a single JobContext parameter: "
              + formatMethodName(method));
    }
    if (paramTypes.length == 1 && !JobContext.class.isAssignableFrom(paramTypes[0])) {
      throw new IllegalArgumentException(
          "@Recurring method parameter must be of type JobContext: " + formatMethodName(method));
    }
  }

  private static String formatMethodName(Method method) {
    return method.getDeclaringClass().getName() + "." + method.getName();
  }
}
