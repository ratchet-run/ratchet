package run.ratchet.ri.util;

import run.ratchet.api.BatchContext;
import run.ratchet.api.JobResult;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.LambdaSerializer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.Base64;
import java.util.Objects;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * Default {@link LambdaSerializer} implementation. Serializes and deserializes {@link
 * SerializablePredicate} and {@link SerializableFunction} instances to/from Base64. Uses an
 * allowlist-based {@link ObjectInputStream} filter to block deserialization of unauthorized
 * classes.
 *
 * <p>Vendor-neutral: the allowlist contains only JDK primitives, JDK collections, {@code
 * java.time}, {@code java.math}, and the {@code SerializedLambda} carrier. Vendor and application
 * packages are authorized exclusively through the injected {@link ClassPolicy}.
 *
 * @see SerializablePredicate
 */
@SuppressWarnings({
  "java:S3740",
  // Raw types in instanceof checks are unavoidable - generics are erased at runtime
  "java:S6201"
  // Pattern matching cannot be used with generics due to type erasure
})
@ApplicationScoped
public class DefaultLambdaSerializer implements LambdaSerializer {

  private static final Logger log = Logger.getLogger(DefaultLambdaSerializer.class);
  private static final long MAX_ARRAY_LENGTH = 10_000;
  private static final long MAX_DEPTH = 20;
  private static final long MAX_REFERENCES = 1_000;
  private static final long MAX_STREAM_BYTES = 1_000_000;

  private static final Set<String> ALLOWED_CLASSES =
      Set.of(
          "java.io.Serializable",
          "java.lang.Enum",
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

  /**
   * JDK-only package prefixes permitted without consulting {@link ClassPolicy}. Vendor and
   * application packages (including any {@code run.ratchet.*} classes) MUST flow through
   * {@link ClassPolicy#isAllowed(String)} — this set MUST NOT carry vendor entries.
   */
  private static final Set<String> ALLOWED_CLASS_PREFIXES = Set.of("java.time.", "java.math.");

  /**
   * {@code java.lang} classes accepted during deserialization. Deliberately excludes {@code
   * Throwable}, {@code Exception}, and {@code RuntimeException} — broad allowlisting of base
   * exception types admits gadget-chain entry via any subclass with a custom {@code readObject}.
   * Specific concrete exception types that genuinely need to round-trip should be added explicitly.
   */
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
          "java.lang.StackTraceElement",
          "java.lang.IllegalStateException",
          "java.lang.IllegalArgumentException",
          "java.lang.NullPointerException",
          "java.lang.UnsupportedOperationException");

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
  private final ClassPolicy classPolicy;

  protected DefaultLambdaSerializer() {
    this.classPolicy = className -> false;
  }

  @Inject
  public DefaultLambdaSerializer(ClassPolicy classPolicy) {
    this.classPolicy = Objects.requireNonNull(classPolicy, "classPolicy");
  }

  private static String objectArrayElementType(String className) {
    String element = className;
    while (element.startsWith("[")) {
      if (element.length() == 2) {
        return null;
      }
      element = element.substring(1);
    }
    if (element.startsWith("L") && element.endsWith(";")) {
      return element.substring(1, element.length() - 1);
    }
    if (element.indexOf('.') > 0) {
      return element;
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public SerializablePredicate<BatchContext> deserializeBatchContextPredicate(String serialized) {
    return (SerializablePredicate<BatchContext>)
        deserialize(serialized, SerializablePredicate.class, "BatchContext predicate");
  }

  @Override
  @SuppressWarnings({"unchecked", "java:S1452"})
  // Wildcard in return type is required - JobResult type is unknown at deserialization time
  public SerializablePredicate<JobResult<?>> deserializeJobResultPredicate(String serialized) {
    return (SerializablePredicate<JobResult<?>>)
        deserialize(serialized, SerializablePredicate.class, "JobResult predicate");
  }

  @Override
  @SuppressWarnings("unchecked")
  public SerializableFunction<Object, Boolean> deserializeResultFunction(String serialized) {
    return (SerializableFunction<Object, Boolean>)
        deserialize(serialized, SerializableFunction.class, "result function");
  }

  @Override
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

  @Override
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
      log.error("Predicate serialization error", e);
      return null;
    }
  }

  @SuppressWarnings("java:S3740")
  private <T> T deserialize(String serialized, Class<T> expectedType, String label) {
    if (serialized == null || serialized.trim().isEmpty()) {
      return null;
    }

    try {
      byte[] bytes = Base64.getDecoder().decode(serialized);

      try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
          ObjectInputStream ois = createSecureObjectInputStream(bais)) {

        Object obj = ois.readObject();

        if (expectedType.isInstance(obj)) {
          return expectedType.cast(obj);
        }

        log.warnf(
            "Deserialized object is not a %s: %s", expectedType.getSimpleName(), obj.getClass());
        return null;
      }
    } catch (InvalidClassException e) {
      log.errorf(e, "Security: Blocked deserialization of unauthorized class in %s", label);
      return null;
    } catch (Exception e) {
      log.errorf(e, "Deserialization error for %s", label);
      return null;
    }
  }

  private ObjectInputStream createSecureObjectInputStream(ByteArrayInputStream bais)
      throws IOException {
    ObjectInputStream ois =
        new ObjectInputStream(bais) {
          @Override
          protected Class<?> resolveClass(ObjectStreamClass desc)
              throws IOException, ClassNotFoundException {

            String className = desc.getName();

            if (isAllowedClassName(className)) {
              return super.resolveClass(desc);
            }

            throw unauthorizedClass(className);
          }
        };
    ois.setObjectInputFilter(this::filterClass);
    return ois;
  }

  private ObjectInputFilter.Status filterClass(ObjectInputFilter.FilterInfo info) {
    if (info.arrayLength() > MAX_ARRAY_LENGTH
        || info.depth() > MAX_DEPTH
        || info.references() > MAX_REFERENCES
        || info.streamBytes() > MAX_STREAM_BYTES) {
      return ObjectInputFilter.Status.REJECTED;
    }
    return ObjectInputFilter.Status.UNDECIDED;
  }

  private boolean isAllowedClassName(String className) {
    if (ALLOWED_CLASSES.contains(className)
        || ALLOWED_JAVA_LANG_CLASSES.contains(className)
        || ALLOWED_JAVA_UTIL_CLASSES.contains(className)) {
      return true;
    }

    for (String prefix : ALLOWED_CLASS_PREFIXES) {
      if (className.startsWith(prefix)) {
        return true;
      }
    }

    // Any non-JDK class (including vendor ratchet classes) must be authorized by ClassPolicy.
    // ClassPolicy.isAllowed() is the single source of truth for vendor/application allowlisting.
    if (classPolicy.isAllowed(className)) {
      return true;
    }

    String policyName = objectArrayElementType(className);
    return policyName != null && classPolicy.isAllowed(policyName);
  }

  private InvalidClassException unauthorizedClass(String className) {
    log.errorf("Blocked unauthorized deserialization attempt for class: %s", className);
    return new InvalidClassException(
        "Unauthorized deserialization attempt for class: "
            + className
            + ". This class is not allowed for workflow predicates.");
  }
}
