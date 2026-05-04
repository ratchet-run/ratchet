package run.ratchet.store.query;

import run.ratchet.api.JobQuerySortField;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

/**
 * Opaque keyset-pagination cursor encoding {@code (sortField, sortValue, jobId)}.
 *
 * <p>Encoded as URL-safe base64 with {@code |} delimiters so it can be passed in query parameters
 * without further escaping. Use {@link #encode()} to produce the opaque string for {@link
 * run.ratchet.api.JobPage#nextCursor()} and {@link #decode(String)} to parse it back
 * before building the store seek predicate.
 *
 * <p>Sort value encoding by field type:
 *
 * <ul>
 *   <li>{@code CREATED_AT, SCHEDULED_TIME, UPDATED_AT}: ISO-8601 {@link java.time.Instant} string
 *   <li>{@code PRIORITY}: decimal integer ordinal
 *   <li>{@code STATUS}: enum name string
 * </ul>
 */
public final class JobQueryCursor {

  public final JobQuerySortField sortField;
  public final String sortValue;
  public final UUID jobId;

  public JobQueryCursor(JobQuerySortField sortField, String sortValue, UUID jobId) {
    this.sortField = sortField;
    this.sortValue = sortValue;
    this.jobId = jobId;
  }

  public String encode() {
    String raw = sortField.name() + "|" + sortValue + "|" + jobId;
    return Base64.getUrlEncoder().withoutPadding()
        .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
  }

  public static JobQueryCursor decode(String cursor) {
    byte[] bytes = Base64.getUrlDecoder().decode(cursor);
    String raw = new String(bytes, StandardCharsets.UTF_8);
    int first = raw.indexOf('|');
    int second = raw.indexOf('|', first + 1);
    if (first < 0 || second < 0) {
      throw new IllegalArgumentException("malformed cursor: " + cursor);
    }
    JobQuerySortField field = JobQuerySortField.valueOf(raw.substring(0, first));
    String sortValue = raw.substring(first + 1, second);
    UUID jobId = UUID.fromString(raw.substring(second + 1));
    return new JobQueryCursor(field, sortValue, jobId);
  }
}
