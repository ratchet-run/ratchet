package run.ratchet.api;

import java.util.List;

/**
 * A page of query results from {@link JobQueryService}.
 *
 * @param <T> the item type (typically {@link JobSummary})
 * @param items the items on this page
 * @param totalCount total number of matching items across all pages; may be {@code -1} if counting
 *     was skipped for performance
 * @param limit the page size requested
 * @param offset the zero-based offset of this page
 * @param hasMore true if there are additional items beyond this page
 * @param nextCursor opaque cursor for the next page, or null if no cursor is available
 */
@Incubating
public record JobPage<T>(
    List<T> items, long totalCount, int limit, int offset, boolean hasMore, String nextCursor) {}
