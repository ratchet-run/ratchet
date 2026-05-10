package run.ratchet.spi;

import java.util.Arrays;
import java.util.Objects;
import run.ratchet.api.Incubating;

/** Describes a lambda expression's target method for serialization and execution. */
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
        Arrays.hashCode(capturedArgs()));
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
