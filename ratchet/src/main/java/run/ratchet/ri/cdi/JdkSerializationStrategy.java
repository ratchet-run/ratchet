package run.ratchet.ri.cdi;

import run.ratchet.spi.SerializationStrategy;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Default {@link SerializationStrategy} using standard Java serialization.
 *
 * <p>Users can override by providing their own {@code @ApplicationScoped SerializationStrategy}
 * bean (e.g., using Jackson or Kryo).
 */
@ApplicationScoped
public class JdkSerializationStrategy implements SerializationStrategy {

  @Override
  public byte[] serialize(Object obj) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(obj);
      oos.flush();
      return baos.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize object: " + obj, e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T deserialize(byte[] data, Class<T> type) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      return (T) ois.readObject();
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize to " + type.getName(), e);
    }
  }
}
