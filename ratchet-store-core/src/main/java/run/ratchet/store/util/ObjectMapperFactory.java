package run.ratchet.store.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Provides a shared, properly configured {@link ObjectMapper} singleton for ratchet modules.
 *
 * <p>The singleton instance registers {@link JavaTimeModule} for correct serialization of {@code
 * java.time} types (Instant, Duration, LocalDateTime, etc.) as ISO-8601 strings rather than numeric
 * timestamps.
 */
public final class ObjectMapperFactory {

  private static final ObjectMapper INSTANCE =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

  private ObjectMapperFactory() {}

  /** Returns the shared, thread-safe ObjectMapper singleton. */
  public static ObjectMapper get() {
    return INSTANCE;
  }
}
