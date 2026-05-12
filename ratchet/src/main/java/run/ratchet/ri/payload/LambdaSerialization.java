package run.ratchet.ri.payload;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;

final class LambdaSerialization {

  private LambdaSerialization() {}

  /**
   * Converts a JVM-generated serializable lambda to its {@link SerializedLambda} metadata. This is
   * not a general-purpose {@link Serializable#writeReplace()} adapter.
   */
  @SuppressWarnings("java:S3011")
  static SerializedLambda toSerializedLambda(Serializable lambda, String failureMessage) {
    Object replacement = invokeWriteReplace(lambda, failureMessage);
    if (replacement instanceof SerializedLambda serializedLambda) {
      return serializedLambda;
    }
    String replacementType = replacement == null ? "null" : replacement.getClass().getName();
    ClassCastException cause =
        new ClassCastException(
            "writeReplace returned " + replacementType + " instead of SerializedLambda");
    throw new IllegalStateException(failureMessage + ": " + cause.getMessage(), cause);
  }

  private static Object invokeWriteReplace(Serializable lambda, String failureMessage) {
    try {
      Method method = lambda.getClass().getDeclaredMethod("writeReplace");
      method.setAccessible(true);
      return method.invoke(lambda);
    } catch (ReflectiveOperationException | RuntimeException e) {
      throw new IllegalStateException(failureMessage, e);
    }
  }
}
