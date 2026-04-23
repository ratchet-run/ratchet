package run.ratchet.spi;

import run.ratchet.api.BatchContext;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobResult;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;

/**
 * SPI governing JDK-serialization plus {@link java.io.ObjectInputFilter} policy for captured
 * lambdas, method references, and {@link java.io.Serializable} predicates used by workflow
 * conditions.
 *
 * <p><b>Scope:</b> this SPI is <em>only</em> for JDK-binary serialization of {@code Serializable}
 * artifacts captured from user code (batch predicates, workflow result functions, job-result
 * filters). It is <em>not</em> the surface for JSON persistence of payloads or results — that lives
 * in {@link PayloadSerializer}. Implementations MUST enforce an allowlist-driven {@code
 * ObjectInputFilter} and MUST consult {@link ClassPolicy} before admitting any
 * vendor/application-scoped class to deserialization.
 *
 * <p><b>Security contract:</b> implementations MUST reject classes not explicitly allowlisted or
 * admitted by {@link ClassPolicy#isAllowed(String)}. Implementations MUST NOT hardcode vendor
 * package prefixes (including {@code run.ratchet}); vendor-package allowlisting MUST flow
 * through {@link ClassPolicy}. Implementations MUST apply stream-level limits (depth, array length,
 * reference count, stream bytes) sufficient to block resource-exhaustion attacks.
 *
 * <p><b>Default implementation:</b> the reference implementation provides a default bean with an
 * allowlist derived from {@link ClassPolicy} plus a minimal set of JDK types (collections, boxing,
 * {@code java.time}, {@code java.math}, and the {@code SerializedLambda} carrier). Users override
 * this SPI when they need a custom sandbox, classloader isolation, or a stricter {@code
 * ObjectInputFilter} policy, via a CDI {@code @Alternative} bean.
 *
 * <p><b>Thread-safety:</b> implementations MUST be thread-safe.
 *
 * <p><b>Encoding:</b> serialized form is Base64-encoded JDK binary serialization.
 *
 * @see PayloadSerializer
 * @see ClassPolicy
 */
@Incubating
public interface LambdaSerializer {

  /**
   * Serializes the given {@link SerializablePredicate} to a Base64 string.
   *
   * @param predicate the predicate to serialize; MAY be {@code null}
   * @param <T> the predicate input type
   * @return Base64-encoded serialized form, or {@code null} if {@code predicate} is {@code null} or
   *     serialization fails
   */
  <T> String serialize(SerializablePredicate<T> predicate);

  /**
   * Deserializes a Base64-encoded {@link SerializablePredicate} over {@link BatchContext}.
   *
   * @param serialized Base64 string produced by {@link #serialize(SerializablePredicate)}; MAY be
   *     {@code null} or empty
   * @return the deserialized predicate, or {@code null} if input is empty or deserialization is
   *     rejected by the allowlist filter
   */
  SerializablePredicate<BatchContext> deserializeBatchContextPredicate(String serialized);

  /**
   * Deserializes a Base64-encoded {@link SerializablePredicate} over {@link JobResult}.
   *
   * @param serialized Base64 string produced by {@link #serialize(SerializablePredicate)}; MAY be
   *     {@code null} or empty
   * @return the deserialized predicate, or {@code null} if input is empty or deserialization is
   *     rejected by the allowlist filter
   */
  @SuppressWarnings("java:S1452")
  // Wildcard in return type required - JobResult type is unknown at deserialization time.
  SerializablePredicate<JobResult<?>> deserializeJobResultPredicate(String serialized);

  /**
   * Deserializes a Base64-encoded {@link SerializableFunction} that evaluates a parent job's result
   * to a {@code Boolean}.
   *
   * @param serialized Base64 string produced by the framework's workflow plumbing; MAY be {@code
   *     null} or empty
   * @return the deserialized function, or {@code null} if input is empty or deserialization is
   *     rejected by the allowlist filter
   */
  SerializableFunction<Object, Boolean> deserializeResultFunction(String serialized);

  /**
   * Reports whether the supplied Base64 string, when decoded, is a syntactically well-formed and
   * allowlisted {@link SerializablePredicate}.
   *
   * @param serialized Base64 string to inspect; MAY be {@code null} or empty
   * @return {@code true} if the input deserializes to a {@code SerializablePredicate} under the
   *     allowlist policy, {@code false} otherwise
   */
  boolean isValidSerializedPredicate(String serialized);
}
