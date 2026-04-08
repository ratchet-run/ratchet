package run.ratchet.ri.util;

import run.ratchet.api.BatchContext;
import run.ratchet.api.JobResult;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.Base64;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * Service for serializing and deserializing lambda expressions used in workflow conditions.
 *
 * <p>This class provides safe serialization and deserialization of {@link SerializablePredicate}
 * instances for persistence in the database. It handles both simple method references and complex
 * lambda expressions with captured variables.
 *
 * <p><strong>Security Considerations:</strong>
 *
 * <p>This class implements a strict allowlist-based deserialization filter to prevent
 * deserialization attacks. Only classes matching {@link #ALLOWED_CLASS_PREFIXES} or {@link
 * #ALLOWED_CLASSES} can be deserialized. All other classes are blocked and logged as security
 * failures.
 *
 * @see SerializablePredicate
 * @see JobResult
 * @see BatchContext
 */
@SuppressWarnings({
  "java:S3740",
  // Raw types in instanceof checks are unavoidable - generics are erased at runtime
  "java:S6201"
  // Pattern matching cannot be used with generics due to type erasure
})
@ApplicationScoped
public class LambdaSerializer {

  private static final Logger log = Logger.getLogger(LambdaSerializer.class);

  /** Allowlist of exact class names permitted during deserialization. */
  private static final Set<String> ALLOWED_CLASSES =
      Set.of(
          "java.io.Serializable",
          "[Z",
          "[B",
          "[C",
          "[S",
          "[I",
          "[J",
          "[F",
          "[D", // Primitive arrays
          "[Ljava.lang.String;",
          "[Ljava.lang.Object;",
          "java.lang.invoke.SerializedLambda" // Required for lambda serialization
          );

  /** Allowlist of class prefixes permitted during deserialization. */
  private static final Set<String> ALLOWED_CLASS_PREFIXES =
      Set.of("run.ratchet.", "java.time.", "java.math.");

  /** Explicit allowlist of safe {@code java.lang} classes permitted during deserialization. */
  private static final Set<String> ALLOWED_JAVA_LANG_CLASSES =
      Set.of(
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
          "java.lang.StackTraceElement",
          "java.lang.Throwable",
          "java.lang.Exception",
          "java.lang.RuntimeException");

  /** Explicit allowlist of safe {@code java.util} classes permitted during deserialization. */
  private static final Set<String> ALLOWED_JAVA_UTIL_CLASSES =
      Set.of(
          "java.util.ArrayList",
          "java.util.HashMap",
          "java.util.HashSet",
          "java.util.LinkedList",
          "java.util.TreeMap",
          "java.util.TreeSet",
          "java.util.LinkedHashMap",
          "java.util.LinkedHashSet",
          "java.util.Collections",
          "java.util.Arrays",
          "java.util.Optional",
          "java.util.UUID",
          "java.util.Date",
          "java.util.EnumMap",
          "java.util.EnumSet");

  /**
   * Deserializes a BatchContext predicate from a Base64-encoded string.
   *
   * @param serialized the Base64-encoded serialized predicate
   * @return the deserialized predicate, or null if deserialization fails
   */
  @SuppressWarnings("unchecked")
  public SerializablePredicate<BatchContext> deserializeBatchContextPredicate(String serialized) {
    if (serialized == null || serialized.trim().isEmpty()) {
      return null;
    }

    try {
      byte[] bytes = Base64.getDecoder().decode(serialized);

      try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
          ObjectInputStream ois = createSecureObjectInputStream(bais)) {

        Object obj = ois.readObject();

        if (obj instanceof SerializablePredicate) {
          return (SerializablePredicate<BatchContext>) obj;
        }

        log.warnf("Deserialized object is not a SerializablePredicate: %s", obj.getClass());
        return null;
      }
    } catch (InvalidClassException e) {
      log.error(
          "Security: Blocked deserialization of unauthorized class in BatchContext predicate", e);
      return null;
    } catch (Exception e) {
      log.error("Failed to deserialize BatchContext predicate", e);
      return null;
    }
  }

  /**
   * Deserializes a JobResult predicate from a Base64-encoded string.
   *
   * @param serialized the Base64-encoded serialized predicate
   * @return the deserialized predicate, or null if deserialization fails
   */
  @SuppressWarnings({"unchecked", "java:S1452"})
  // Wildcard in return type is required - JobResult type is unknown at deserialization time
  public SerializablePredicate<JobResult<?>> deserializeJobResultPredicate(String serialized) {
    if (serialized == null || serialized.trim().isEmpty()) {
      return null;
    }

    try {
      byte[] bytes = Base64.getDecoder().decode(serialized);

      try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
          ObjectInputStream ois = createSecureObjectInputStream(bais)) {

        Object obj = ois.readObject();

        if (obj instanceof SerializablePredicate) {
          return (SerializablePredicate<JobResult<?>>) obj;
        }

        log.warnf("Deserialized object is not a SerializablePredicate: %s", obj.getClass());
        return null;
      }
    } catch (InvalidClassException e) {
      log.error(
          "Security: Blocked deserialization of unauthorized class in JobResult predicate", e);
      return null;
    } catch (Exception e) {
      log.error("Failed to deserialize JobResult predicate", e);
      return null;
    }
  }

  /**
   * Deserializes a result-value function from a Base64-encoded string.
   *
   * @param serialized the Base64-encoded serialized function
   * @return the deserialized function, or null if deserialization fails
   */
  @SuppressWarnings("unchecked")
  public SerializableFunction<Object, Boolean> deserializeResultFunction(String serialized) {
    if (serialized == null || serialized.trim().isEmpty()) {
      return null;
    }

    try {
      byte[] bytes = Base64.getDecoder().decode(serialized);

      try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
          ObjectInputStream ois = createSecureObjectInputStream(bais)) {

        Object obj = ois.readObject();

        if (obj instanceof SerializableFunction<?, ?>) {
          return (SerializableFunction<Object, Boolean>) obj;
        }

        log.warnf("Deserialized object is not a SerializableFunction: %s", obj.getClass());
        return null;
      }
    } catch (InvalidClassException e) {
      log.error("Security: Blocked deserialization of unauthorized class in result function", e);
      return null;
    } catch (Exception e) {
      log.error("Failed to deserialize result function", e);
      return null;
    }
  }

  /**
   * Validates that a serialized predicate can be successfully deserialized.
   *
   * @param serialized the Base64-encoded serialized predicate
   * @return true if the predicate can be deserialized, false otherwise
   */
  public boolean isValidSerializedPredicate(String serialized) {
    if (serialized == null || serialized.trim().isEmpty()) {
      return false;
    }

    try {
      byte[] bytes = Base64.getDecoder().decode(serialized);

      try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
          ObjectInputStream ois = createSecureObjectInputStream(bais)) {

        Object obj = ois.readObject();
        return obj instanceof SerializablePredicate;
      }
    } catch (InvalidClassException e) {
      log.warnf("Validation blocked unauthorized class in predicate: %s", e.getMessage());
      return false;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Serializes a SerializablePredicate to a Base64-encoded string.
   *
   * @param predicate the predicate to serialize
   * @param <T> the type parameter of the predicate
   * @return Base64-encoded serialized predicate, or null if serialization fails
   */
  public <T> String serialize(SerializablePredicate<T> predicate) {
    if (predicate == null) {
      return null;
    }

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {

      oos.writeObject(predicate);
      oos.flush();

      byte[] bytes = baos.toByteArray();
      return Base64.getEncoder().encodeToString(bytes);
    } catch (IOException e) {
      log.error("Failed to serialize predicate", e);
      return null;
    }
  }

  /**
   * Creates a secure ObjectInputStream with class filtering to prevent deserialization attacks.
   *
   * @param bais the byte array input stream to wrap
   * @return a secure ObjectInputStream with filtering enabled
   * @throws IOException if stream creation fails
   */
  private ObjectInputStream createSecureObjectInputStream(ByteArrayInputStream bais)
      throws IOException {
    return new ObjectInputStream(bais) {
      @Override
      protected Class<?> resolveClass(ObjectStreamClass desc)
          throws IOException, ClassNotFoundException {

        String className = desc.getName();

        // Check exact matches first (primitive arrays, SerializedLambda, etc.)
        if (ALLOWED_CLASSES.contains(className)) {
          return super.resolveClass(desc);
        }

        // Check explicit java.lang and java.util class allowlists
        if (ALLOWED_JAVA_LANG_CLASSES.contains(className)
            || ALLOWED_JAVA_UTIL_CLASSES.contains(className)) {
          return super.resolveClass(desc);
        }

        // Check prefix matches (run.ratchet.*, java.time.*, java.math.*)
        for (String prefix : ALLOWED_CLASS_PREFIXES) {
          if (className.startsWith(prefix)) {
            return super.resolveClass(desc);
          }
        }

        // Deny everything else - log security event
        log.errorf("Blocked unauthorized deserialization attempt for class: %s", className);

        throw new InvalidClassException(
            "Unauthorized deserialization attempt for class: "
                + className
                + ". This class is not in the allowlist for workflow predicates.");
      }
    };
  }
}
