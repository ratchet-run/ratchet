package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * SPI governing JSON persistence of job payloads and results.
 *
 * <p>This SPI is the single JSON-persistence boundary for the framework's runtime. Every place that
 * converts an arbitrary application object to or from a JSON string for database storage MUST route
 * through a {@code PayloadSerializer}. Modules MUST NOT call {@code JsonbBuilder.create()},
 * Jackson, Gson, or any other JSON library directly when the target string is persisted through
 * Ratchet's stores.
 *
 * <p><b>Default implementation:</b> the reference implementation provides a default bean backed by
 * <a href="https://jakarta.ee/specifications/jsonb/">Jakarta JSON Binding (JSON-B)</a>. Users
 * rarely override this SPI; it is exposed so deployments that require a specific JSON framework,
 * polymorphic type handling, or custom adapter chains can install their own implementation via a
 * CDI {@code @Alternative} bean.
 *
 * <p><b>Scope:</b> this SPI is deliberately scoped to JSON persistence of payloads and results.
 * Implementations MUST NOT enable polymorphic type handling that would allow attacker-controlled
 * JSON to name arbitrary classes for deserialization (the classic Jackson "default typing"
 * gadget-chain vulnerability).
 *
 * <p><b>Security note — no runtime guard:</b> the framework provides no automated detection of a
 * non-compliant implementation. A custom CDI {@code @Alternative} that re-enables provider-specific
 * polymorphic deserialization (e.g. Jackson's {@code DEFAULT_TYPING}, Gson's {@code
 * RuntimeTypeAdapterFactory} with an open type registry) will silently replace the default
 * serializer at startup with no warning logged. Security teams that deploy a custom {@code
 * PayloadSerializer} are responsible for auditing the implementation against this contract and for
 * verifying that no untrusted JSON payload can cause the deserializer to instantiate a class
 * outside the deployment's expected payload types.
 *
 * <p><b>Thread-safety:</b> implementations MUST be thread-safe. The framework holds a single
 * instance per deployment and invokes {@link #serialize(Object)} and {@link #deserialize(String,
 * Class)} concurrently from poller worker threads, JPA attribute converters, and result persistence
 * paths.
 *
 * <p><b>Error handling:</b> implementations MUST throw {@link IllegalArgumentException} (or a
 * subclass) when serialization or deserialization fails. Callers expect this contract and convert
 * it to framework-level error events.
 */
@Incubating
public interface PayloadSerializer {

  /**
   * Serializes the given object to a JSON string.
   *
   * @param payload the object to serialize; MAY be {@code null}
   * @return JSON representation of {@code payload}, or {@code null} if the input was {@code null}
   * @throws IllegalArgumentException if the object cannot be serialized
   */
  String serialize(Object payload);

  /**
   * Deserializes the given JSON string to an instance of the requested type.
   *
   * @param json the JSON string to deserialize; MAY be {@code null} or empty
   * @param type the target class; must not be {@code null}
   * @param <T> the target type
   * @return the deserialized instance, or {@code null} if {@code json} is {@code null} or empty
   * @throws IllegalArgumentException if deserialization fails
   */
  <T> T deserialize(String json, Class<T> type);
}
