package run.ratchet.ri.security;

import jakarta.enterprise.context.ApplicationScoped;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.jboss.logging.Logger;
import run.ratchet.store.entity.JobPayload;

/**
 * Validates job payload structure and type safety at creation time (fail-fast). Checks class
 * loadability, method signature, argument count, and type compatibility including autoboxing.
 *
 * <p>For security validation (class policy, method visibility), see {@link JobSecurityValidator}.
 *
 * @see JobSecurityValidator
 */
@ApplicationScoped
public class JobPayloadInputValidator {

  private static final Logger log = Logger.getLogger(JobPayloadInputValidator.class);
  private static final int MAX_CLASS_NAME_LENGTH = 512;
  private static final Pattern CLASS_NAME =
      Pattern.compile(
          "[\\p{javaJavaIdentifierStart}][\\p{javaJavaIdentifierPart}]*"
              + "(\\.[\\p{javaJavaIdentifierStart}][\\p{javaJavaIdentifierPart}]*)*");

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

  private boolean isNullOrEmpty(String value) {
    return value == null || value.isEmpty();
  }

  private boolean isTypeIncompatible(Class<?> expected, Class<?> provided) {
    if (expected.isAssignableFrom(provided)) {
      return false;
    }

    if (expected.isPrimitive()) {
      Class<?> wrapper = PRIMITIVE_TO_WRAPPER.get(expected);
      return wrapper == null || !wrapper.isAssignableFrom(provided);
    }
    if (provided.isPrimitive()) {
      Class<?> wrapper = PRIMITIVE_TO_WRAPPER.get(provided);
      return wrapper == null || !expected.isAssignableFrom(wrapper);
    }

    return true;
  }

  private void throwIfErrors(List<String> errors, JobPayload payload) {
    if (!errors.isEmpty()) {
      String errorMessage = "Job payload validation failed:\n" + String.join("\n", errors);
      log.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
    log.debugf("Job payload validated successfully: %s.%s", payload.target(), payload.method());
  }

  private void validateMethodDescriptor(JobPayload payload, List<String> errors) {
    if (isNullOrEmpty(payload.methodDescriptor())) {
      errors.add("Method descriptor cannot be null or empty");
    }
  }

  private void validateMethodName(JobPayload payload, List<String> errors) {
    if (isNullOrEmpty(payload.method())) {
      errors.add("Method name cannot be null or empty");
    }
  }

  private void validateMethodSignature(Class<?> clazz, JobPayload payload, List<String> errors) {
    try {
      Method method = MethodLookup.findMethod(clazz, payload);
      if (method == null) {
        Method nonPublic = MethodLookup.findDeclaredMethod(clazz, payload);
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
      List<Object> args = payload.args();

      if (args == null) {
        errors.add("Arguments cannot be null");
        return;
      }

      if (args.size() != methodParamTypes.length) {
        errors.add(
            "Argument count mismatch: expected "
                + methodParamTypes.length
                + " parameters, but payload has "
                + args.size());
        return;
      }

      if (paramTypes.length != methodParamTypes.length) {
        errors.add(
            "Argument count mismatch: expected "
                + methodParamTypes.length
                + " parameters, but payload has "
                + paramTypes.length);
        return;
      }

      for (int i = 0; i < paramTypes.length; i++) {
        Class<?> expectedType = methodParamTypes[i];
        Class<?> providedType = paramTypes[i];

        if (isTypeIncompatible(expectedType, providedType)) {
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
          if (arg != null && isTypeIncompatible(expectedType, arg.getClass())) {
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
      log.errorf(
          e, "Failed to validate method signature for %s.%s", payload.target(), payload.method());
      errors.add("Failed to validate method signature: " + e.getMessage());
    }
  }

  private void validateSignatureIfPossible(JobPayload payload, List<String> errors) {
    if (isNullOrEmpty(payload.target())
        || isNullOrEmpty(payload.method())
        || isNullOrEmpty(payload.methodDescriptor())) {
      return;
    }
    String classNameError = classNameError(payload.target());
    if (classNameError != null) {
      return;
    }
    try {
      Class<?> clazz =
          Class.forName(payload.target(), false, Thread.currentThread().getContextClassLoader());
      validateMethodSignature(clazz, payload, errors);
    } catch (ClassNotFoundException e) {
      // Already reported by validateTargetClass
    } catch (Exception e) {
      log.errorf(
          e, "Failed to validate method signature for %s.%s", payload.target(), payload.method());
      errors.add("Failed to validate method signature: " + e.getMessage());
    }
  }

  private void validateTargetClass(JobPayload payload, List<String> errors) {
    if (isNullOrEmpty(payload.target())) {
      errors.add("Target class cannot be null or empty");
      return;
    }
    String classNameError = classNameError(payload.target());
    if (classNameError != null) {
      errors.add(classNameError);
      return;
    }
    try {
      Class.forName(payload.target(), false, Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException e) {
      errors.add("Target class not found: " + payload.target() + " - " + e.getMessage());
    } catch (Exception e) {
      errors.add("Cannot load target class " + payload.target() + ": " + e.getMessage());
    }
  }

  private String classNameError(String className) {
    if (className.length() > MAX_CLASS_NAME_LENGTH) {
      return "Target class name is too long: " + className.length() + " characters";
    }
    if (className.indexOf('\0') >= 0
        || className.contains("..")
        || !CLASS_NAME.matcher(className).matches()) {
      return "Target class name contains invalid characters: " + className;
    }
    return null;
  }
}
