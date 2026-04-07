package run.ratchet.ri.security;

import run.ratchet.spi.ErrorSanitizer;
import java.util.regex.Pattern;

/**
 * Default {@link ErrorSanitizer} implementation that strips common PII patterns from exception
 * messages and truncates to a maximum length.
 *
 * <p>Patterns redacted include:
 *
 * <ul>
 *   <li>JDBC connection URLs with embedded credentials
 *   <li>URLs containing userinfo (user:password@host)
 *   <li>Email-like patterns
 *   <li>Common credential key-value patterns (password=..., token=..., etc.)
 * </ul>
 */
public class DefaultErrorSanitizer implements ErrorSanitizer {

  /**
   * System property controlling whether email-like patterns are redacted from exception messages.
   * Disabled by default — most ratchet failure messages do not contain user emails, but
   * business-record IDs that match the email pattern (e.g. {@code order-2026@dev}) are common false
   * positives that lose debug information. Set to {@code true} to opt in.
   */
  static final String REDACT_EMAILS_PROPERTY = "ratchet.error-sanitizer.redact-emails";

  private static final int MAX_LENGTH = 2000;
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

    String sanitized = message;
    sanitized = JDBC_URL.matcher(sanitized).replaceAll(REDACTED);
    sanitized = URL_WITH_CREDENTIALS.matcher(sanitized).replaceAll(REDACTED);
    if (Boolean.parseBoolean(System.getProperty(REDACT_EMAILS_PROPERTY, "false"))) {
      sanitized = EMAIL.matcher(sanitized).replaceAll(REDACTED);
    }
    sanitized = CREDENTIAL_KV.matcher(sanitized).replaceAll("$1=" + REDACTED);

    StringBuilder result = new StringBuilder(className).append(": ").append(sanitized);
    // Append the (sanitized) cause chain so operators don't lose root-cause context.
    Throwable cause = ex.getCause();
    if (cause != null && cause != ex) {
      String causeMessage = cause.getMessage();
      if (causeMessage != null) {
        String sanitizedCause = causeMessage;
        sanitizedCause = JDBC_URL.matcher(sanitizedCause).replaceAll(REDACTED);
        sanitizedCause = URL_WITH_CREDENTIALS.matcher(sanitizedCause).replaceAll(REDACTED);
        if (Boolean.parseBoolean(System.getProperty(REDACT_EMAILS_PROPERTY, "false"))) {
          sanitizedCause = EMAIL.matcher(sanitizedCause).replaceAll(REDACTED);
        }
        sanitizedCause = CREDENTIAL_KV.matcher(sanitizedCause).replaceAll("$1=" + REDACTED);
        result
            .append(" -> caused by ")
            .append(cause.getClass().getName())
            .append(": ")
            .append(sanitizedCause);
      }
    }

    if (result.length() > MAX_LENGTH) {
      return result.substring(0, MAX_LENGTH - 3) + "...";
    }
    return result.toString();
  }
}
