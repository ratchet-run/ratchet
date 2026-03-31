package run.ratchet.store.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonObjectMapConverterTest {

  private final JsonObjectMapConverter converter = new JsonObjectMapConverter();

  @Test
  void roundtrip_preservesMixedTypes() {
    Map<String, Object> original = Map.of("name", "Alice", "age", 30, "active", true);
    String json = converter.convertToDatabaseColumn(original);
    Map<String, Object> restored = converter.convertToEntityAttribute(json);

    assertEquals("Alice", restored.get("name"));
    assertEquals(0, new BigDecimal("30").compareTo(new BigDecimal(restored.get("age").toString())));
    assertEquals(true, restored.get("active"));
  }

  @Test
  void nullAttribute_returnsNull() {
    assertNull(converter.convertToDatabaseColumn(null));
  }

  @Test
  void nullDbData_returnsNull() {
    assertNull(converter.convertToEntityAttribute(null));
  }

  @Test
  void emptyString_returnsNull() {
    assertNull(converter.convertToEntityAttribute(""));
  }

  @Test
  void malformedJson_throwsException() {
    assertThrows(
        IllegalArgumentException.class, () -> converter.convertToEntityAttribute("not json"));
  }
}
