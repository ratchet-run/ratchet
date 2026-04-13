package run.ratchet.ri.cdi;

import run.ratchet.spi.SerializationStrategy;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Set;

/** JDK serialization with an allowlist-based deserialization filter. */
@ApplicationScoped
public class JdkSerializationStrategy implements SerializationStrategy {

  /** Types allowed through the deserialization filter. Deny-all ({@code !*}) is appended. */
  private static final Set<String> ALLOWED_TYPES =
      Set.of(
          // Ratchet framework
          "run.ratchet.**",
          // Primitive arrays
          "[B",
          "[C",
          "[S",
          "[I",
          "[J",
          "[F",
          "[D",
          "[Z",
          // Object arrays
          "[Ljava.lang.String",
          "[Ljava.lang.Object",
          "[Ljava.util.*",
          "[Ljava.time.*",
          "[Ljava.math.*",
          "[Lrun.ratchet.**",
          // Boxing types
          "java.lang.Object",
          "java.lang.String",
          "java.lang.Integer",
          "java.lang.Long",
          "java.lang.Double",
          "java.lang.Float",
          "java.lang.Boolean",
          "java.lang.Byte",
          "java.lang.Short",
          "java.lang.Character",
          "java.lang.Number",
          "java.lang.Enum",
          "java.lang.Class",
          // Exception types (specific — no broad Throwable/Exception base classes)
          "java.lang.StackTraceElement",
          "java.lang.RuntimeException",
          "java.lang.IllegalStateException",
          "java.lang.IllegalArgumentException",
          "java.lang.UnsupportedOperationException",
          "java.lang.NullPointerException",
          "java.lang.SecurityException",
          "java.lang.ClassNotFoundException",
          "java.util.concurrent.TimeoutException",
          "java.util.concurrent.CancellationException",
          "java.io.IOException",
          "java.io.UncheckedIOException",
          // Collections
          "java.util.ArrayList",
          "java.util.HashMap",
          "java.util.HashSet",
          "java.util.LinkedList",
          "java.util.LinkedHashMap",
          "java.util.LinkedHashSet",
          "java.util.TreeMap",
          "java.util.TreeSet",
          "java.util.CollSer",
          "java.util.Collections",
          "java.util.Collections$*",
          "java.util.Arrays",
          "java.util.Arrays$*",
          "java.util.ImmutableCollections",
          "java.util.ImmutableCollections$*",
          "java.util.Optional",
          "java.util.UUID",
          "java.util.Date",
          "java.util.EnumMap",
          "java.util.EnumSet",
          // Wildcard packages
          "java.time.*",
          "java.math.*");

  private static final ObjectInputFilter FILTER =
      ObjectInputFilter.Config.createFilter(String.join(";", ALLOWED_TYPES) + ";!*");

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
      ois.setObjectInputFilter(FILTER);
      return (T) ois.readObject();
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize to " + type.getName(), e);
    }
  }
}
