package run.ratchet.ri.payload;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;

final class LambdaSerialization {

  private LambdaSerialization() {}

  @SuppressWarnings("java:S3011")
  static SerializedLambda toSerializedLambda(Serializable lambda, String failureMessage) {
    try {
      Method method = lambda.getClass().getDeclaredMethod("writeReplace");
      method.setAccessible(true);
      return (SerializedLambda) method.invoke(lambda);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(failureMessage, e);
    }
  }
}
