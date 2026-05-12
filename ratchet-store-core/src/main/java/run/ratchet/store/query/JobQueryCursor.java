package run.ratchet.store.query;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;
import run.ratchet.api.JobPage;
import run.ratchet.api.JobQuerySortField;

/**
 * Opaque keyset-pagination cursor encoding {@code (sortField, sortValue, jobId)}.
 *
 * <p>Encoded as URL-safe base64 with {@code |} delimiters so it can be passed in query parameters
 * without further escaping. Use {@link #encode()} to produce the opaque string for {@link
 * JobPage#nextCursor()} and {@link #decode(String)} to parse it back before building the store seek
 * predicate.
 *
 * <p>Sort value encoding by field type:
 *
 * <ul>
 *   <li>{@code CREATED_AT, SCHEDULED_TIME, UPDATED_AT}: ISO-8601 {@link Instant} string
 *   <li>{@code PRIORITY}: decimal integer ordinal
 *   <li>{@code STATUS}: enum name string
 * </ul>
 */
public record JobQueryCursor(JobQuerySortField sortField, String sortValue, UUID jobId) {

  public static JobQueryCursor decode(String cursor) {
    byte[] bytes;
    try {
      bytes = Base64.getUrlDecoder().decode(cursor);
    } catch (IllegalArgumentException e) {
      throw malformedCursor(e);
    }
    String raw = new String(bytes, StandardCharsets.UTF_8);
    int first = raw.indexOf('|');
    int second = raw.indexOf('|', first + 1);
    if (first < 0 || second < 0) {
      throw malformedCursor(null);
    }
    try {
      JobQuerySortField field = JobQuerySortField.valueOf(raw.substring(0, first));
      String sortValue = raw.substring(first + 1, second);
      UUID jobId = UUID.fromString(raw.substring(second + 1));
      return new JobQueryCursor(field, sortValue, jobId);
    } catch (IllegalArgumentException e) {
      throw malformedCursor(e);
    }
  }

  public String encode() {
    // The supported sort encodings reserve '|' as the field delimiter.
    String raw = sortField.name() + "|" + sortValue + "|" + jobId;
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
  }

  private static IllegalArgumentException malformedCursor(Throwable cause) {
    return cause == null
        ? new IllegalArgumentException("Malformed pagination cursor")
        : new IllegalArgumentException("Malformed pagination cursor", cause);
  }
}
