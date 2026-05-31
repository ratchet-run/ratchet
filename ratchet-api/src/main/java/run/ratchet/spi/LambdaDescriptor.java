package run.ratchet.spi;

import java.util.Arrays;
import java.util.Objects;
import run.ratchet.api.Incubating;

/**
 * Describes a lambda expression's target method for serialization and execution.
 *
 * @param targetClass fully qualified class name containing the target method
 * @param methodName target method name
 * @param methodDescriptor JVM method descriptor for overload resolution
 * @param isStatic whether the target method is static
 * @param capturedArgs captured lambda arguments; may be {@code null}. A non-null array is
 *     defensively copied, and an empty array is distinct from {@code null}.
 */
@Incubating
public record LambdaDescriptor(
    String targetClass,
    String methodName,
    String methodDescriptor,
    boolean isStatic,
    Object[] capturedArgs) {
  public LambdaDescriptor {
    capturedArgs = capturedArgs == null ? null : Arrays.copyOf(capturedArgs, capturedArgs.length);
  }

  @Override
  public Object[] capturedArgs() {
    return capturedArgs == null ? null : Arrays.copyOf(capturedArgs, capturedArgs.length);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof LambdaDescriptor that)) return false;
    return isStatic() == that.isStatic()
        && Objects.equals(methodName(), that.methodName())
        && Objects.equals(targetClass(), that.targetClass())
        && Objects.deepEquals(capturedArgs(), that.capturedArgs())
        && Objects.equals(methodDescriptor(), that.methodDescriptor());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        targetClass(),
        methodName(),
        methodDescriptor(),
        isStatic(),
        Arrays.deepHashCode(capturedArgs()));
  }

  @Override
  public String toString() {
    return "LambdaDescriptor{"
        + "targetClass='"
        + targetClass
        + '\''
        + ", methodName='"
        + methodName
        + '\''
        + ", methodDescriptor='"
        + methodDescriptor
        + '\''
        + ", isStatic="
        + isStatic
        + ", capturedArgs="
        + Arrays.toString(capturedArgs)
        + '}';
  }
}
