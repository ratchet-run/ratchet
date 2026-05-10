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
   * @param ex throwable to sanitize; never {@code null}
   * @return sanitized, bounded error text; never {@code null}
   */
  String sanitize(Throwable ex);
}
