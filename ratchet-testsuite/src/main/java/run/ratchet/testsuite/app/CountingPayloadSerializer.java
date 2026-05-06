package run.ratchet.testsuite.app;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.spi.PayloadSerializer;

/**
 * CDI {@link Alternative} {@link PayloadSerializer} that counts every framework invocation and
 * otherwise delegates to JSON-B. Used by {@code CustomSerializationStrategyIT} to prove that the
 * framework routes payload and result JSON through the SPI during real job execution (not just
 * bean-producibility — the counter increments only when the framework itself invokes the SPI).
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class CountingPayloadSerializer implements PayloadSerializer {

  private static final AtomicInteger SERIALIZE_COUNT = new AtomicInteger(0);
  private static final AtomicInteger DESERIALIZE_COUNT = new AtomicInteger(0);

  private final Jsonb jsonb = JsonbBuilder.create();

  public static int getSerializeCount() {
    return SERIALIZE_COUNT.get();
  }

  public static int getDeserializeCount() {
    return DESERIALIZE_COUNT.get();
  }

  public static void resetCounts() {
    SERIALIZE_COUNT.set(0);
    DESERIALIZE_COUNT.set(0);
  }

  @Override
  public String serialize(Object payload) {
    SERIALIZE_COUNT.incrementAndGet();
    if (payload == null) {
      return null;
    }
    return jsonb.toJson(payload);
  }

  @Override
  public <T> T deserialize(String json, Class<T> type) {
    DESERIALIZE_COUNT.incrementAndGet();
    if (json == null || json.isEmpty()) {
      return null;
    }
    return jsonb.fromJson(json, type);
  }
}
