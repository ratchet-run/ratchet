package run.ratchet.store.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobPayload;
import java.util.List;
import org.junit.jupiter.api.Test;

class JobPayloadConverterTest {

  private final JobPayloadConverter converter = new JobPayloadConverter();

  @Test
  void roundtrip_preservesAllFields() {
    JobPayload original = samplePayload();
    String json = converter.convertToDatabaseColumn(original);
    JobPayload restored = converter.convertToEntityAttribute(json);

    assertEquals(original.target(), restored.target());
    assertEquals(original.method(), restored.method());
    assertEquals(original.methodDescriptor(), restored.methodDescriptor());
    assertEquals(original.isStatic(), restored.isStatic());
    assertEquals(original.args(), restored.args());
  }

  @Test
  void nullAttribute_returnsNullJson() {
    assertNull(converter.convertToDatabaseColumn(null));
  }

  @Test
  void nullDbData_returnsNullPayload() {
    assertNull(converter.convertToEntityAttribute(null));
  }

  @Test
  void emptyString_returnsNull() {
    assertNull(converter.convertToEntityAttribute(""));
  }

  @Test
  void producesValidJson() {
    String json = converter.convertToDatabaseColumn(samplePayload());
    assertNotNull(json);
    assertTrue(json.startsWith("{"));
    assertTrue(json.contains("\"target\""));
    assertTrue(json.contains("\"method\""));
  }

  @Test
  void malformedJson_throwsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> converter.convertToEntityAttribute("{not valid json"));
  }

  private JobPayload samplePayload() {
    return new JobPayload(
        "com.example.MyService", "process", "(Ljava/lang/String;)V", true, List.of("hello"));
  }
}
