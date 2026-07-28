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
package run.ratchet.api;

import java.util.List;

/**
 * A page of query results from Ratchet read-only query APIs such as {@link JobQueryService} and
 * {@link ClusterQueryService}.
 *
 * @param <T> the item type (typically {@link JobSummary} or {@link NodeStatus})
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
    List<T> items, long totalCount, int limit, int offset, boolean hasMore, String nextCursor) {
  public JobPage {
    items = List.copyOf(items);
  }
}
