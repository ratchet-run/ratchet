package run.ratchet.ri.payload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.store.entity.JobPayload;

class JobPayloadFactoryTest {

  @Test
  void constructorArgumentDoesNotShiftEarlierCapturedArguments() {
    String first = "first";

    JobPayload payload =
        JobPayloadFactory.fromLambda(
            (SerializableCheckedRunnable)
                () -> PayloadTarget.capture(first, new String("constructed")),
            List.of("second"));

    assertEquals(PayloadTarget.class.getName(), payload.target());
    assertEquals("capture", payload.method());
    assertEquals(List.of(first, "second"), payload.args());
  }

  @Test
  void reflectionLookupsAreCachedAcrossRepeatedConversions() throws Exception {
    clearCache("VISIBILITY_CACHE");
    clearCache("FUNCTIONAL_INTERFACE_METHOD_CACHE");
    StringFunction target = PayloadTarget::uppercase;
    StringFunction wrapper = value -> target.apply("cached");

    JobPayload first = JobPayloadFactory.fromLambda(wrapper);
    JobPayload second = JobPayloadFactory.fromLambda(wrapper);

    assertEquals(PayloadTarget.class.getName(), first.target());
    assertEquals(List.of("cached"), first.args());
    assertEquals(List.of("cached"), second.args());
    assertEquals(1, cacheSize("VISIBILITY_CACHE"));
    assertEquals(1, cacheSize("FUNCTIONAL_INTERFACE_METHOD_CACHE"));
  }

  @Test
  void lambdaSerializationRejectsSerializableReplacementObjectsWithClearMessage() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                LambdaSerialization.toSerializedLambda(
                    new SerializableReplacement(), "Expected a serializable lambda"));

    assertTrue(thrown.getMessage().contains("Expected a serializable lambda"));
    assertInstanceOf(ClassCastException.class, thrown.getCause());
  }

  @SuppressWarnings("unchecked")
  private static Map<?, ?> cache(String name) throws ReflectiveOperationException {
    Field field = JobPayloadFactory.class.getDeclaredField(name);
    field.setAccessible(true);
    return (Map<?, ?>) field.get(null);
  }

  private static int cacheSize(String name) throws ReflectiveOperationException {
    return cache(name).size();
  }

  private static void clearCache(String name) throws ReflectiveOperationException {
    cache(name).clear();
  }

  @FunctionalInterface
  interface StringFunction extends Serializable {
    String apply(String value);
  }

  private static final class SerializableReplacement implements Serializable {
    @SuppressWarnings("unused")
    private Object writeReplace() {
      return "replacement";
    }
  }

  public static final class PayloadTarget {
    public static void capture(String first, String second) {}

    public static String uppercase(String value) {
      return value.toUpperCase();
    }
  }
}
