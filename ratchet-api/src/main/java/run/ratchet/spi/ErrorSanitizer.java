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
   * Converts a throwable into text safe for persistence and event publication.
   *
   * <p>Implementations must inspect the throwable cause chain, must tolerate cyclic cause graphs,
   * must return bounded text, and should not throw. If sanitization itself fails, returning the
   * throwable class name or another conservative fallback is preferable to surfacing the sanitizer
   * failure to the job execution path.
   *
   * @param ex throwable to sanitize; {@code null} input returns a bounded representation of {@code
   *     "null"}
   * @return sanitized, bounded error text; never {@code null}
   */
  String sanitize(Throwable ex);
}
