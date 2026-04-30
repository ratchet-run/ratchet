package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.cdi.JsonbPayloadSerializer;
import run.ratchet.spi.SerializedJobResult;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DefaultResultPersistenceStrategyTest {

  @Test
  void resultLimitUsesUtf8ByteLength() {
    RatchetOptions options =
        RatchetOptions.builder().payload(payload -> payload.maxResultBytes(5)).build();
    DefaultResultPersistenceStrategy strategy =
        new DefaultResultPersistenceStrategy(options, new JsonbPayloadSerializer());

    SerializedJobResult result = strategy.serialize(new UUID(0L, 42L), "\u00e9\u00e9");

    assertEquals(String.class.getName(), result.type());
    assertTrue(result.json().contains("\"_truncated\":true"));
    assertTrue(result.json().contains("\"_originalSize\":6"));
  }
}
