/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
