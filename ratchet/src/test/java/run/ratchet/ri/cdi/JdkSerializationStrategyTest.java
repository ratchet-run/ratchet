package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;

class JdkSerializationStrategyTest {

  private final JdkSerializationStrategy strategy = new JdkSerializationStrategy();

  @Test
  void roundtripString() {
    byte[] data = strategy.serialize("hello");
    String result = strategy.deserialize(data, String.class);
    assertEquals("hello", result);
  }

  @Test
  void roundtripInteger() {
    byte[] data = strategy.serialize(42);
    Integer result = strategy.deserialize(data, Integer.class);
    assertEquals(42, result);
  }

  @Test
  void roundtripList() {
    List<String> original = List.of("a", "b", "c");
    byte[] data = strategy.serialize(original);
    @SuppressWarnings("unchecked")
    List<String> result = strategy.deserialize(data, List.class);
    assertEquals(original, result);
  }

  @Test
  void blockedClassThrowsOnDeserialize() {
    // Serialize a javax.management object which is NOT in the allowed filter
    ObjectName blocked;
    try {
      blocked = new ObjectName("test:type=Test");
    } catch (Exception e) {
      fail("Failed to create test ObjectName: " + e.getMessage());
      return;
    }
    byte[] data = strategy.serialize(blocked);
    assertThrows(RuntimeException.class, () -> strategy.deserialize(data, ObjectName.class));
  }

  @Test
  void nonSerializableInputThrows() {
    Object notSerializable = new Object();
    assertThrows(RuntimeException.class, () -> strategy.serialize(notSerializable));
  }
}
