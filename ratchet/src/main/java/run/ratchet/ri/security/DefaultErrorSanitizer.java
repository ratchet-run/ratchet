package run.ratchet.ri.security;

import run.ratchet.spi.ErrorSanitizer;
import java.util.IdentityHashMap;
import java.util.regex.Pattern;

/** Strips PII (JDBC URLs, credentials, optionally emails) from exception messages. */
public class DefaultErrorSanitizer implements ErrorSanitizer {

  /**
   * System property controlling whether email-like patterns are redacted from exception messages.
   * Enabled by default so that user emails leaking into job error messages never reach {@code
   * last_error} columns. Set to {@code false} to opt out when business-record IDs happen to match
   * the email pattern (e.g. {@code order-2026@dev}) and the false positives are worse than the PII
   * risk.
   */
  static final String REDACT_EMAILS_PROPERTY = "ratchet.error-sanitizer.redact-emails";

  private static final int MAX_LENGTH = 2000;
  private static final int MAX_CAUSE_DEPTH = 10;
  private static final String REDACTED = "***REDACTED***";

  /** Matches JDBC URLs: jdbc:mysql://user:pass@host/db or jdbc:postgresql://... */
  private static final Pattern JDBC_URL =
      Pattern.compile("jdbc:[a-z]+://[^\\s,;)]+", Pattern.CASE_INSENSITIVE);

  /** Matches URLs with userinfo: {@code https://user:pass@host} */
  private static final Pattern URL_WITH_CREDENTIALS =
      Pattern.compile("https?://[^@/\\s]+:[^@/\\s]+@[^\\s,;)]+", Pattern.CASE_INSENSITIVE);

  /** Matches email-like patterns. */
  private static final Pattern EMAIL =
      Pattern.compile("[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}");

  /** Matches common credential key=value patterns in log/exception output. */
  private static final Pattern CREDENTIAL_KV =
      Pattern.compile(
          "(password|passwd|pwd|secret|token|apikey|api_key|access_key|private_key|credential)"
              + "\\s*[=:]\\s*\\S+",
          Pattern.CASE_INSENSITIVE);

  /**
   * Matches URL-encoded credential parameters in connection strings / query params (e.g. {@code
   * &password=hunter2&}). Hibernate and JDBC exception messages commonly echo back the original
   * connection URL with query-string credentials intact.
   */
  private static final Pattern URL_PARAM_CREDENTIAL =
      Pattern.compile(
          "([?&;](?:password|passwd|pwd|user|username|secret|token|apikey|api_key|access_key|credential))"
              + "=[^&\\s;,)]+",
          Pattern.CASE_INSENSITIVE);

  @Override
  public String sanitize(Throwable ex) {
    if (ex == null) {
      return "null";
    }

    String className = ex.getClass().getName();
    String message = ex.getMessage();
    if (message == null) {
      return className;
    }

    StringBuilder result = new StringBuilder(className).append(": ").append(redact(message));

    // Walk the full cause chain (bounded by MAX_CAUSE_DEPTH) with identity-based cycle detection.
    // Deeply-wrapped exceptions from Jakarta EE layers (e.g. EJBException -> PersistenceException
    // -> SQLException) commonly carry JDBC credentials in the deepest cause — a single-level walk
    // would let them leak into last_error.
    IdentityHashMap<Throwable, Boolean> seen = new IdentityHashMap<>();
    seen.put(ex, Boolean.TRUE);
    Throwable cause = ex.getCause();
    int depth = 0;
    while (cause != null && depth < MAX_CAUSE_DEPTH && !seen.containsKey(cause)) {
      seen.put(cause, Boolean.TRUE);
      String causeMessage = cause.getMessage();
      if (causeMessage != null) {
        result
            .append(" -> caused by ")
            .append(cause.getClass().getName())
            .append(": ")
            .append(redact(causeMessage));
      } else {
        result.append(" -> caused by ").append(cause.getClass().getName());
      }
      cause = cause.getCause();
      depth++;
    }

    if (result.length() > MAX_LENGTH) {
      return result.substring(0, MAX_LENGTH - 3) + "...";
    }
    return result.toString();
  }

  private static String redact(String text) {
    String sanitized = text;
    sanitized = JDBC_URL.matcher(sanitized).replaceAll(REDACTED);
    sanitized = URL_WITH_CREDENTIALS.matcher(sanitized).replaceAll(REDACTED);
    if (Boolean.parseBoolean(System.getProperty(REDACT_EMAILS_PROPERTY, "true"))) {
      sanitized = EMAIL.matcher(sanitized).replaceAll(REDACTED);
    }
    sanitized = URL_PARAM_CREDENTIAL.matcher(sanitized).replaceAll("$1=" + REDACTED);
    sanitized = CREDENTIAL_KV.matcher(sanitized).replaceAll("$1=" + REDACTED);
    return sanitized;
  }
}
