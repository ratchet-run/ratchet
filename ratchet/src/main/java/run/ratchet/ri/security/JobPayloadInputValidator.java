package run.ratchet.ri.security;

import run.ratchet.store.entity.JobPayload;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.objectweb.asm.Type;

/**
 * Validates job payload input at creation and execution time to prevent runtime failures and ensure
 * type safety.
 *
 * <p>This validator performs comprehensive checks on job payloads before they are persisted to the
 * job queue. By catching errors early at job creation time, we avoid wasting resources on jobs that
 * would inevitably fail at execution time. This "fail-fast" approach provides immediate feedback to
 * developers and prevents the accumulation of invalid jobs in the system.
 *
 * <p>This validator performs comprehensive checks on job payloads:
 *
 * <ul>
 *   <li>Target class exists and is loadable via {@link Class#forName(String)}
 *   <li>Method exists with correct signature matching the method descriptor
 *   <li>Argument types match method parameter types (including autoboxing support)
 *   <li>Argument count matches method parameter count exactly
 * </ul>
 *
 * <p><b>Note:</b> This validator focuses on structural correctness and type safety. For
 * security-related validation (class policy, method visibility), see {@link JobSecurityValidator}.
 *
 * @see JobSecurityValidator for security-related validation
 * @see JobPayload for the payload structure being validated
 */
public class JobPayloadInputValidator {

  private static final Logger log = Logger.getLogger(JobPayloadInputValidator.class.getName());

  /** Map of primitive types to their corresponding wrapper types. */
  private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER =
      Map.of(
          boolean.class, Boolean.class,
          byte.class, Byte.class,
          char.class, Character.class,
          short.class, Short.class,
          int.class, Integer.class,
          long.class, Long.class,
          float.class, Float.class,
          double.class, Double.class);

  /**
   * Validates a job payload at creation time.
   *
   * @param payload the job payload to validate
   * @throws IllegalArgumentException if validation fails with a clear error message
   */
  public void validateAtCreation(JobPayload payload) {
    if (payload == null) {
      throw new IllegalArgumentException("Job payload cannot be null");
    }

    List<String> errors = new ArrayList<>();
    validateTargetClass(payload, errors);
    validateMethodName(payload, errors);
    validateMethodDescriptor(payload, errors);
    validateSignatureIfPossible(payload, errors);
    throwIfErrors(errors, payload);
  }

  /**
   * Finds a public method in the given class matching the payload specification.
   *
   * @param clazz the class to search for the method
   * @param payload the job payload containing the method name and ASM descriptor
   * @return the matching Method object, or null if no public method matches
   */
  private Method findMethod(Class<?> clazz, JobPayload payload) {
    for (Method m : clazz.getMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
        return m;
      }
    }
    return null;
  }

  /**
   * Finds a method of any visibility in the given class matching the payload specification.
   *
   * @param clazz the class to search for the method
   * @param payload the job payload containing the method name and ASM descriptor
   * @return the matching Method object regardless of visibility, or null if not found at all
   */
  private Method findDeclaredMethod(Class<?> clazz, JobPayload payload) {
    for (Method m : clazz.getDeclaredMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
        return m;
      }
    }
    return null;
  }

  /**
   * Checks if a string is null or empty.
   *
   * @param value the string to check
   * @return true if null or empty, false otherwise
   */
  private boolean isNullOrEmpty(String value) {
    return value == null || value.isEmpty();
  }

  /**
   * Checks if two types are compatible, accounting for primitive/wrapper autoboxing.
   *
   * @param expected the expected type (from method signature)
   * @param provided the provided type (from payload)
   * @return true if the types are compatible for method invocation
   */
  private boolean isTypeCompatible(Class<?> expected, Class<?> provided) {
    if (expected.isAssignableFrom(provided)) {
      return true;
    }

    if (expected.isPrimitive()) {
      Class<?> wrapper = PRIMITIVE_TO_WRAPPER.get(expected);
      return wrapper != null && wrapper.isAssignableFrom(provided);
    }
    if (provided.isPrimitive()) {
      Class<?> wrapper = PRIMITIVE_TO_WRAPPER.get(provided);
      return wrapper != null && expected.isAssignableFrom(wrapper);
    }

    return false;
  }

  /**
   * Throws an exception if any validation errors were accumulated.
   *
   * @param errors list of validation error messages
   * @param payload the payload being validated (for logging)
   * @throws IllegalArgumentException if errors is not empty
   */
  private void throwIfErrors(List<String> errors, JobPayload payload) {
    if (!errors.isEmpty()) {
      String errorMessage = "Job payload validation failed:\n" + String.join("\n", errors);
      log.severe(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
    log.fine("Job payload validated successfully: " + payload.target() + "." + payload.method());
  }

  /**
   * Validates that the method descriptor is not null or empty.
   *
   * @param payload the job payload containing the method descriptor
   * @param errors list to accumulate validation errors
   */
  private void validateMethodDescriptor(JobPayload payload, List<String> errors) {
    if (isNullOrEmpty(payload.methodDescriptor())) {
      errors.add("Method descriptor cannot be null or empty");
    }
  }

  /**
   * Validates that the method name is not null or empty.
   *
   * @param payload the job payload containing the method name
   * @param errors list to accumulate validation errors
   */
  private void validateMethodName(JobPayload payload, List<String> errors) {
    if (isNullOrEmpty(payload.method())) {
      errors.add("Method name cannot be null or empty");
    }
  }

  /**
   * Validates that the method exists and arguments match the method signature.
   *
   * @param clazz the target class containing the method
   * @param payload the job payload specifying method name, descriptor, and arguments
   * @param errors mutable list to accumulate validation error messages
   */
  private void validateMethodSignature(Class<?> clazz, JobPayload payload, List<String> errors) {
    try {
      Method method = findMethod(clazz, payload);
      if (method == null) {
        Method nonPublic = findDeclaredMethod(clazz, payload);
        if (nonPublic != null) {
          String visibility =
              Modifier.isPrivate(nonPublic.getModifiers())
                  ? "private"
                  : Modifier.isProtected(nonPublic.getModifiers())
                      ? "protected"
                      : "package-private";
          errors.add(
              "Method "
                  + payload.method()
                  + " in class "
                  + payload.target()
                  + " is "
                  + visibility
                  + " -- only public methods can be scheduled as jobs. "
                  + "Change the method visibility to public.");
        } else {
          errors.add(
              "Method "
                  + payload.method()
                  + " with descriptor "
                  + payload.methodDescriptor()
                  + " not found in class "
                  + payload.target());
        }
        return;
      }

      Class<?>[] paramTypes = payload.parameterTypes();
      Class<?>[] methodParamTypes = method.getParameterTypes();

      if (paramTypes.length != methodParamTypes.length) {
        errors.add(
            "Argument count mismatch: expected "
                + methodParamTypes.length
                + " parameters, but payload has "
                + paramTypes.length);
        return;
      }

      List<Object> args = payload.args();
      for (int i = 0; i < paramTypes.length; i++) {
        Class<?> expectedType = methodParamTypes[i];
        Class<?> providedType = paramTypes[i];

        if (!isTypeCompatible(expectedType, providedType)) {
          errors.add(
              "Argument type mismatch at position "
                  + i
                  + ": expected "
                  + expectedType.getName()
                  + ", but payload has "
                  + providedType.getName());
        }

        if (i < args.size()) {
          Object arg = args.get(i);
          if (arg != null && !isTypeCompatible(expectedType, arg.getClass())) {
            errors.add(
                "Argument value at position "
                    + i
                    + " is of type "
                    + arg.getClass().getName()
                    + ", but method expects "
                    + expectedType.getName());
          }
        }
      }
    } catch (Exception e) {
      errors.add("Failed to validate method signature: " + e.getMessage());
    }
  }

  /**
   * Validates method signature if all required fields are present.
   *
   * @param payload the job payload with target, method, and descriptor
   * @param errors list to accumulate validation errors
   */
  private void validateSignatureIfPossible(JobPayload payload, List<String> errors) {
    if (isNullOrEmpty(payload.target())
        || isNullOrEmpty(payload.method())
        || isNullOrEmpty(payload.methodDescriptor())) {
      return;
    }
    try {
      Class<?> clazz = Class.forName(payload.target());
      validateMethodSignature(clazz, payload, errors);
    } catch (ClassNotFoundException e) {
      // Already reported by validateTargetClass
    } catch (Exception e) {
      errors.add("Failed to validate method signature: " + e.getMessage());
    }
  }

  /**
   * Validates that the target class exists and is loadable.
   *
   * @param payload the job payload containing the target class name
   * @param errors list to accumulate validation errors
   */
  private void validateTargetClass(JobPayload payload, List<String> errors) {
    if (isNullOrEmpty(payload.target())) {
      errors.add("Target class cannot be null or empty");
      return;
    }
    try {
      Class.forName(payload.target());
    } catch (ClassNotFoundException e) {
      errors.add("Target class not found: " + payload.target() + " - " + e.getMessage());
    } catch (Exception e) {
      errors.add("Cannot load target class " + payload.target() + ": " + e.getMessage());
    }
  }
}
