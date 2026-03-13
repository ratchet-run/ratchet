package run.ratchet.store.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonMapConverterTest {

  private final JsonMapConverter converter = new JsonMapConverter();

  @Test
  void roundtrip_preservesEntries() {
    Map<String, String> original = Map.of("key1", "value1", "key2", "value2");
    String json = converter.convertToDatabaseColumn(original);
    Map<String, String> restored = converter.convertToEntityAttribute(json);

    assertEquals("value1", restored.get("key1"));
    assertEquals("value2", restored.get("key2"));
    assertEquals(2, restored.size());
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
    assertThrows(IllegalArgumentException.class, () -> converter.convertToEntityAttribute("{bad}"));
  }
}
