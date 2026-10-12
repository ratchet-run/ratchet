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
package run.ratchet.store.query;

import run.ratchet.api.JobFilter;

/** Shared archive-inclusion rules for job search and count queries. */
public final class ArchiveSearch {

  private ArchiveSearch() {}

  /**
   * Returns whether the query can include archived rows.
   *
   * <p>Archived rows do not keep callerPrincipal, tags, propertyFilters, idempotencyKey, pickedBy,
   * resourceName or traceCorrelationId. A filter that constrains any of these fields skips the
   * archive instead of returning archived rows to which the constraint was never applied.
   * Authorization policies may scope with any of these fields, so this fails closed.
   */
  public static boolean includesArchive(JobFilter filter) {
    return filter != null
        && filter.includeArchived()
        && (filter.callerPrincipal() == null || filter.callerPrincipal().isEmpty())
        && (filter.tags() == null || filter.tags().isEmpty())
        && (filter.propertyFilters() == null || filter.propertyFilters().isEmpty())
        && (filter.idempotencyKey() == null || filter.idempotencyKey().isEmpty())
        && (filter.pickedBy() == null || filter.pickedBy().isEmpty())
        && (filter.resourceName() == null || filter.resourceName().isEmpty())
        && (filter.traceCorrelationId() == null || filter.traceCorrelationId().isEmpty());
  }
}
