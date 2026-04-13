package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * SPI for sanitizing exception information before it is persisted to the job store or published in
 * events.
 *
 * <p>Exceptions thrown during job execution may contain JDBC URLs, auth tokens, or PII. Sanitize
 * before storing to the job record or broadcasting in events.
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
