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
package run.ratchet.ri.security;

import java.util.IdentityHashMap;
import java.util.regex.Pattern;
import run.ratchet.spi.ErrorSanitizer;

/** Strips PII (JDBC URLs, credentials, optionally emails) from exception messages. */
public class DefaultErrorSanitizer implements ErrorSanitizer {

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
              + "\\s*[=:]\\s*[^\\s,;)\\]>]+",
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

  private final boolean redactEmails;

  public DefaultErrorSanitizer() {
    this(true);
  }

  public DefaultErrorSanitizer(boolean redactEmails) {
    this.redactEmails = redactEmails;
  }

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

  private String redact(String text) {
    String sanitized = text;
    sanitized = JDBC_URL.matcher(sanitized).replaceAll(REDACTED);
    sanitized = URL_WITH_CREDENTIALS.matcher(sanitized).replaceAll(REDACTED);
    if (redactEmails) {
      sanitized = EMAIL.matcher(sanitized).replaceAll(REDACTED);
    }
    sanitized = URL_PARAM_CREDENTIAL.matcher(sanitized).replaceAll("$1=" + REDACTED);
    sanitized = CREDENTIAL_KV.matcher(sanitized).replaceAll("$1=" + REDACTED);
    return sanitized;
  }
}
