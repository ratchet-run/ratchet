package run.ratchet.ri.security;

/**
 * Masks sensitive fields (passwords, tokens, PII) in job payload JSON before API/log exposure. The
 * original payload in the database is never modified.
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
