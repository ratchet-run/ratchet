package run.ratchet.ri.cdi;

import run.ratchet.spi.SerializationStrategy;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * A serialization strategy that uses Java's standard serialization mechanism. This implementation
 * leverages {@link ObjectOutputStream} and {@link ObjectInputStream} to perform the serialization
 * and deserialization of objects.
 *
 * <p>This strategy supports generic serialization and deserialization of objects, ensuring
 * compatibility with Java's built-in serialization framework. It applies a configurable {@link
 * ObjectInputFilter} to maintain security and control during deserialization, preventing loading of
 * undesired or malicious object types.
 *
 * <p>The methods provided by this class implement the contract defined in the {@link
 * SerializationStrategy} interface. Users of this class can serialize an object into a byte array
 * and deserialize a byte array back to an object of the specified type.
 *
 * <p>This implementation should be used when a simple, standard Java serialization approach is
 * needed without external serialization libraries.
 *
 * <p>Implementations of this class are thread-safe.
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
      ois.setObjectInputFilter(
          ObjectInputFilter.Config.createFilter(
              "run.ratchet.**;java.lang.*;java.util.*;java.time.*;java.math.*;!*"));
      return (T) ois.readObject();
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize to " + type.getName(), e);
    }
  }
}
