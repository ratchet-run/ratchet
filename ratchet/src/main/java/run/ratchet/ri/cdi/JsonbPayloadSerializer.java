package run.ratchet.ri.cdi;

import run.ratchet.spi.PayloadSerializer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import jakarta.json.bind.JsonbException;

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
 * <p>TODO (Jakarta spec work): configure parse limits (max depth, array length, document size) on
 * the {@code Jsonb} instance once Ratchet's broader parse-limit policy is settled. Tracked as a
 * separate /dg finding.
 */
@ApplicationScoped
public class JsonbPayloadSerializer implements PayloadSerializer {

  private volatile Jsonb jsonb;

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
}
