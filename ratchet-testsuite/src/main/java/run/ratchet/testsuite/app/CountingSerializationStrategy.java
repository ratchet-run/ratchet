package run.ratchet.testsuite.app;

import run.ratchet.spi.SerializationStrategy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.atomic.AtomicInteger;

/** Custom {@link SerializationStrategy} for testing SPI overridability. */
@Alternative
@Priority(1)
@ApplicationScoped
public class CountingSerializationStrategy implements SerializationStrategy {

  private static final AtomicInteger SERIALIZE_COUNT = new AtomicInteger(0);
  private static final AtomicInteger DESERIALIZE_COUNT = new AtomicInteger(0);

  @Override
  public byte[] serialize(Object obj) {
    SERIALIZE_COUNT.incrementAndGet();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(obj);
      oos.flush();
      return baos.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize: " + obj, e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T deserialize(byte[] data, Class<T> type) {
    DESERIALIZE_COUNT.incrementAndGet();
    try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      return (T) ois.readObject();
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize to " + type.getName(), e);
    }
  }

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
}
