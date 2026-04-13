package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * SPI for sanitizing exception information before it is persisted to the job store or published in
 * events.
 *
 * <p>Exception messages may contain sensitive information such as database connection strings, API
 * keys, authentication tokens, or personally identifiable information (PII). Implementations of
 * this interface sanitize error details to prevent information disclosure while preserving enough
 * context for debugging.
 *
 * <p>The default implementation truncates to a maximum length and strips common PII patterns such
 * as JDBC URLs with credentials and email addresses. Users can override by providing their own
 * {@code @Alternative @Priority(APPLICATION) ErrorSanitizer} bean.
 */
@Incubating
public interface ErrorSanitizer {

  /**
   * Sanitizes the given exception for safe persistence and logging.
   *
   * <p>Implementations should preserve the exception class name, truncate long messages, and redact
   * patterns that commonly contain credentials or PII.
   *
   * @param ex the exception to sanitize, never null
   * @return a sanitized, non-null string suitable for storage
   */
  String sanitize(Throwable ex);
}
