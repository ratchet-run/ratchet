package run.ratchet.store.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonListConverterTest {

  private final JsonListConverter converter = new JsonListConverter();

  @Test
  void roundtrip_preservesElements() {
    List<Object> original = List.of("alpha", 42, true);
    String json = converter.convertToDatabaseColumn(original);
    List<Object> restored = converter.convertToEntityAttribute(json);

    assertEquals(3, restored.size());
    assertEquals("alpha", restored.get(0));
    assertEquals(0, new BigDecimal("42").compareTo(new BigDecimal(restored.get(1).toString())));
    assertEquals(true, restored.get(2));
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
        IllegalArgumentException.class, () -> converter.convertToEntityAttribute("[broken"));
  }
}
