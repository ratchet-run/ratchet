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
              "run.ratchet.**;"
                  + "[B;[C;[S;[I;[J;[F;[D;[Z;"
                  + "[Ljava.lang.String;[Ljava.lang.Object;"
                  + "[Ljava.util.*;[Ljava.time.*;[Ljava.math.*;"
                  + "[Lrun.ratchet.**;"
                  + "java.lang.Object;java.lang.String;"
                  + "java.lang.Integer;java.lang.Long;"
                  + "java.lang.Double;java.lang.Float;java.lang.Boolean;"
                  + "java.lang.Byte;java.lang.Short;java.lang.Character;"
                  + "java.lang.Number;java.lang.Enum;java.lang.Class;"
                  + "java.lang.StackTraceElement;java.lang.Throwable;"
                  + "java.lang.Exception;java.lang.RuntimeException;"
                  + "java.util.ArrayList;java.util.HashMap;java.util.HashSet;"
                  + "java.util.LinkedList;java.util.LinkedHashMap;java.util.LinkedHashSet;"
                  + "java.util.TreeMap;java.util.TreeSet;"
                  + "java.util.CollSer;"
                  + "java.util.Collections;java.util.Collections$*;"
                  + "java.util.Arrays;java.util.Arrays$*;"
                  + "java.util.ImmutableCollections;java.util.ImmutableCollections$*;"
                  + "java.util.Optional;java.util.UUID;"
                  + "java.util.Date;java.util.EnumMap;java.util.EnumSet;"
                  + "java.time.*;java.math.*;!*"));
      return (T) ois.readObject();
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize to " + type.getName(), e);
    }
  }
}
