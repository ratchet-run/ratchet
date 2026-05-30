package run.ratchet.ri.security;

/**
 * Public entry point for masking sensitive fields (passwords, tokens, PII) in job payload JSON
 * before it is rendered into a log line or other output. Which fields are sensitive is decided by
 * the active {@link run.ratchet.spi.PayloadMaskingPolicy}; deployers can override the default field
 * set by producing their own policy. The original payload in the database is never modified.
 */
public final class PayloadMasker {

  private PayloadMasker() {}

  /** Masks sensitive fields in a job payload JSON string; returns null if input is null. */
  public static String maskPayload(String payloadJson) {
    return run.ratchet.store.util.PayloadMasker.maskPayload(payloadJson);
  }

  /**
   * Serializes {@code payload} to JSON then masks sensitive fields; returns null if input is null.
   */
  public static String maskPayload(Object payload) {
    return run.ratchet.store.util.PayloadMasker.maskPayload(payload);
  }
}
