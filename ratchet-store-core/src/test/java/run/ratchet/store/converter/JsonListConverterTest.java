package run.ratchet.store.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.PayloadSerializer;

class JsonListConverterTest {

  private final JsonListConverter converter = new JsonListConverter();

  @AfterEach
  void reset() {
    PayloadSerializerHolder.set(null);
  }

  @Test
  void roundtrip_preservesElements() {
    List<Object> original = List.of("alpha", 42, true);
    String json = converter.convertToDatabaseColumn(original);
    List<Object> restored = converter.convertToEntityAttribute(json);

    assertEquals(3, restored.size());
    assertEquals("alpha", restored.get(0));
    assertEquals(new BigDecimal("42"), restored.get(1));
    assertEquals(true, restored.get(2));
  }

  @Test
  void roundtrip_preservesNestedGenericStructures() {
    List<Object> original = List.of(Map.of("name", "alpha", "flags", List.of(true, false)));

    List<Object> restored =
        converter.convertToEntityAttribute(converter.convertToDatabaseColumn(original));

    assertEquals(original, restored);
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

  @Test
  void ignoresInstalledPayloadSerializer() {
    PayloadSerializerHolder.set(new ThrowingSerializer());

    String json = converter.convertToDatabaseColumn(List.of("alpha"));
    List<Object> restored = converter.convertToEntityAttribute(json);

    assertEquals(List.of("alpha"), restored);
  }

  static final class ThrowingSerializer implements PayloadSerializer {

    @Override
    public String serialize(Object payload) {
      throw new IllegalArgumentException("holder should not be used");
    }

    @Override
    public <T> T deserialize(String json, Class<T> type) {
      throw new IllegalArgumentException("holder should not be used");
    }
  }
}
