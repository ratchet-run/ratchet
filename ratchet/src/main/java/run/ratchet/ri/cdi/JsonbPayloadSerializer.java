package run.ratchet.ri.cdi;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;
import run.ratchet.spi.PayloadSerializer;

/**
 * Default {@link PayloadSerializer} backed by Jakarta JSON Binding (JSON-B).
 *
 * <p>This is the out-of-the-box serializer for payload and result JSON persistence. Holds a single
 * {@link Jsonb} instance per application for thread-safe reuse across the poller, JPA attribute
 * converters, and the result persistence pipeline.
 *
 * <p>Vendor-neutral: does not reference any specific JSON-B provider (Yasson, Johnzon, etc.). The
 * runtime-discovered provider is selected by the host application's classpath/module-path.
 *
 * <p><b>Parse limits:</b> JSON-B 3.0 does not standardize parse-limit configuration. Provider-
 * specific property keys (e.g. Yasson's {@code org.eclipse.yasson.*} properties) are silently
 * ignored on other implementations and cannot be used without sacrificing portability. Applications
 * that require payload size enforcement should validate size at the API boundary before submission
 * rather than inside the serializer. This will be revisited if a future JSON-B release standardizes
 * depth and length limits.
 */
@ApplicationScoped
public class JsonbPayloadSerializer implements PayloadSerializer {

  private volatile Jsonb jsonb;

  @Override
  public String serialize(Object payload) {
    if (payload == null) {
      return null;
    }
    try {
      return jsonb().toJson(payload);
    } catch (JsonbException e) {
      throw new IllegalArgumentException(
          "JSON-B serialization error for " + payload.getClass().getName(), e);
    }
  }

  @Override
  public <T> T deserialize(String json, Class<T> type) {
    if (json == null || json.isEmpty()) {
      return null;
    }
    try {
      return jsonb().fromJson(json, type);
    } catch (JsonbException e) {
      throw new IllegalArgumentException(
          "JSON-B deserialization error for " + (type == null ? "null" : type.getName()), e);
    }
  }

  @PostConstruct
  void init() {
    // Reuse a single configured Jsonb for the lifetime of the bean. JSON-B implementations
    // maintain internal caches; recreating per call would be wasteful.
    this.jsonb = JsonbBuilder.create();
  }

  @PreDestroy
  void close() {
    Jsonb instance = this.jsonb;
    if (instance != null) {
      try {
        instance.close();
      } catch (Exception ignored) {
        // Best-effort cleanup.
      }
    }
  }

  private Jsonb jsonb() {
    Jsonb instance = this.jsonb;
    if (instance == null) {
      // Fallback for direct-construction paths (tests, non-CDI wiring). Container-managed
      // instances go through @PostConstruct above.
      synchronized (this) {
        if (this.jsonb == null) {
          this.jsonb = JsonbBuilder.create();
        }
        instance = this.jsonb;
      }
    }
    return instance;
  }
}
