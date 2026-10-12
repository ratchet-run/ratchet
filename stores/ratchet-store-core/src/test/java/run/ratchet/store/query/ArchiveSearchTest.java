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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobQuerySortField;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;

class ArchiveSearchTest {

  @Test
  void nullFilterExcludesArchive() {
    assertFalse(ArchiveSearch.includesArchive(null));
  }

  @Test
  void includeArchivedFalseExcludesArchive() {
    assertFalse(ArchiveSearch.includesArchive(JobFilter.builder().includeArchived(false).build()));
  }

  @Test
  void callerPrincipalExcludesArchive() {
    assertFalse(
        ArchiveSearch.includesArchive(
            JobFilter.builder().includeArchived(true).callerPrincipal("value").build()));
  }

  @Test
  void tagsExcludesArchive() {
    assertFalse(
        ArchiveSearch.includesArchive(
            JobFilter.builder().includeArchived(true).tags("value").build()));
  }

  @Test
  void propertyFiltersExcludesArchive() {
    assertFalse(
        ArchiveSearch.includesArchive(
            JobFilter.builder().includeArchived(true).propertyEquals("tenant", "A").build()));
  }

  @Test
  void idempotencyKeyExcludesArchive() {
    assertFalse(
        ArchiveSearch.includesArchive(
            JobFilter.builder().includeArchived(true).idempotencyKey("value").build()));
  }

  @Test
  void pickedByExcludesArchive() {
    assertFalse(
        ArchiveSearch.includesArchive(
            JobFilter.builder().includeArchived(true).pickedBy("value").build()));
  }

  @Test
  void resourceNameExcludesArchive() {
    assertFalse(
        ArchiveSearch.includesArchive(
            JobFilter.builder().includeArchived(true).resourceName("value").build()));
  }

  @Test
  void traceCorrelationIdExcludesArchive() {
    assertFalse(
        ArchiveSearch.includesArchive(
            JobFilter.builder().includeArchived(true).traceCorrelationId("value").build()));
  }

  @Test
  void allArchiveSafeFieldsIncludeArchive() {
    JobFilter filter =
        JobFilter.builder()
            .includeArchived(true)
            .statuses(JobStatus.SUCCEEDED)
            .types(JobType.SINGLE)
            .priorities(JobPriority.HIGH)
            .businessKey("business")
            .targetClass("Target")
            .parentJobId(UUID.randomUUID())
            .createdAfter(Instant.EPOCH)
            .createdBefore(Instant.EPOCH.plusSeconds(10))
            .scheduledAfter(Instant.EPOCH)
            .scheduledBefore(Instant.EPOCH.plusSeconds(10))
            .updatedAfter(Instant.EPOCH)
            .cursor("cursor")
            .sortField(JobQuerySortField.SCHEDULED_TIME)
            .sortAscending(true)
            .skipCount(true)
            .build();

    assertTrue(ArchiveSearch.includesArchive(filter));
  }

  @Test
  void emptyPropertyFiltersIncludeArchive() {
    JobFilter filter =
        new JobFilter(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            JobQuerySortField.CREATED_AT,
            false,
            false,
            true,
            null,
            Map.of());

    assertTrue(ArchiveSearch.includesArchive(filter));
  }

  @Test
  void emptyUnsupportedFieldsIncludeArchive() {
    JobFilter filter =
        JobFilter.builder()
            .includeArchived(true)
            .callerPrincipal("")
            .tags(Set.of())
            .idempotencyKey("")
            .pickedBy("")
            .resourceName("")
            .traceCorrelationId("")
            .build();

    assertTrue(ArchiveSearch.includesArchive(filter));
    assertTrue(ArchiveSearch.includesArchive(JobFilter.builder().includeArchived(true).build()));
  }
}
