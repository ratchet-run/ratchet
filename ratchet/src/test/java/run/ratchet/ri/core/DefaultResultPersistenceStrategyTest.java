package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.SerializedJobResult;

class DefaultResultPersistenceStrategyTest {

  @Test
  void resultLimitUsesUtf8ByteLength() {
    RatchetOptions options =
        RatchetOptions.builder().payload(payload -> payload.maxResultBytes(5)).build();
    DefaultResultPersistenceStrategy strategy =
        new DefaultResultPersistenceStrategy(options, new JsonbTestPayloadSerializer());

    SerializedJobResult result = strategy.serialize(new UUID(0L, 42L), "\u00e9\u00e9");

    assertEquals(String.class.getName(), result.type());
    assertTrue(result.json().contains("\"_truncated\":true"));
    assertTrue(result.json().contains("\"_originalSize\":6"));
  }

  @Test
  void resultAtByteLimitIsPersistedWithoutTruncation() {
    RatchetOptions options =
        RatchetOptions.builder().payload(payload -> payload.maxResultBytes(6)).build();
    DefaultResultPersistenceStrategy strategy =
        new DefaultResultPersistenceStrategy(options, new JsonbTestPayloadSerializer());

    SerializedJobResult result = strategy.serialize(new UUID(0L, 41L), "\u00e9\u00e9");

    assertEquals(String.class.getName(), result.type());
    assertEquals("\"\u00e9\u00e9\"", result.json());
  }

  @Test
  void zeroResultLimitPersistsLargeResults() {
    RatchetOptions options =
        RatchetOptions.builder().payload(payload -> payload.maxResultBytes(0)).build();
    DefaultResultPersistenceStrategy strategy =
        new DefaultResultPersistenceStrategy(options, new JsonbTestPayloadSerializer());

    SerializedJobResult result = strategy.serialize(new UUID(0L, 45L), "larger than five bytes");

    assertEquals(String.class.getName(), result.type());
    assertEquals("\"larger than five bytes\"", result.json());
  }

  @Test
  void nullResultPersistsAsEmptyResult() {
    DefaultResultPersistenceStrategy strategy =
        new DefaultResultPersistenceStrategy(defaultOptions(), new JsonbTestPayloadSerializer());

    SerializedJobResult result = strategy.serialize(new UUID(0L, 43L), null);

    assertNull(result.json());
    assertNull(result.type());
  }

  @Test
  void serializationFailureIsPropagated() {
    DefaultResultPersistenceStrategy strategy =
        new DefaultResultPersistenceStrategy(defaultOptions(), new ThrowingPayloadSerializer());

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () -> strategy.serialize(new UUID(0L, 44L), "unserializable"));

    assertTrue(failure.getMessage().contains("Result serialization failed"));
    assertEquals("cannot serialize", failure.getCause().getMessage());
  }

  private static RatchetOptions defaultOptions() {
    return RatchetOptions.builder().build();
  }

  private static final class ThrowingPayloadSerializer implements PayloadSerializer {

    @Override
    public String serialize(Object payload) {
      throw new IllegalArgumentException("cannot serialize");
    }

    @Override
    public <T> T deserialize(String json, Class<T> type) {
      throw new UnsupportedOperationException("not used");
    }
  }
}
