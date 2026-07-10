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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JobFilterTest {

  @Test
  void emptyEnumVarargsAreNoOps() {
    JobFilter filter =
        JobFilter.builder()
            .statuses(JobStatus.PENDING)
            .statuses()
            .types(JobType.SINGLE)
            .types()
            .priorities(JobPriority.HIGH)
            .priorities()
            .build();

    assertEquals(Set.of(JobStatus.PENDING), filter.statuses());
    assertEquals(Set.of(JobType.SINGLE), filter.types());
    assertEquals(Set.of(JobPriority.HIGH), filter.priorities());
  }

  @Test
  void emptyTagVarargsAreNoOps() {
    JobFilter filter = JobFilter.builder().tags("billing").tags().build();

    assertEquals(Set.of("billing"), filter.tags());
  }

  @Test
  void setValuesOverwriteVarargsValues() {
    JobFilter filter =
        JobFilter.builder()
            .statuses(JobStatus.PENDING)
            .statuses(Set.of(JobStatus.FAILED))
            .types(JobType.SINGLE)
            .types(Set.of(JobType.RECURRING))
            .priorities(JobPriority.HIGH)
            .priorities(Set.of(JobPriority.LOW))
            .tags("billing")
            .tags(Set.of("reports"))
            .build();

    assertEquals(Set.of(JobStatus.FAILED), filter.statuses());
    assertEquals(Set.of(JobType.RECURRING), filter.types());
    assertEquals(Set.of(JobPriority.LOW), filter.priorities());
    assertEquals(Set.of("reports"), filter.tags());
  }

  @Test
  void varargsValuesOverwriteSetValues() {
    JobFilter filter =
        JobFilter.builder()
            .statuses(Set.of(JobStatus.FAILED))
            .statuses(JobStatus.PENDING)
            .types(Set.of(JobType.RECURRING))
            .types(JobType.SINGLE)
            .priorities(Set.of(JobPriority.LOW))
            .priorities(JobPriority.HIGH)
            .tags(Set.of("reports"))
            .tags("billing")
            .build();

    assertEquals(Set.of(JobStatus.PENDING), filter.statuses());
    assertEquals(Set.of(JobType.SINGLE), filter.types());
    assertEquals(Set.of(JobPriority.HIGH), filter.priorities());
    assertEquals(Set.of("billing"), filter.tags());
  }

  @Test
  void nullSetValuesClearConstraints() {
    JobFilter filter =
        JobFilter.builder()
            .statuses(JobStatus.PENDING)
            .statuses((Set<JobStatus>) null)
            .types(JobType.SINGLE)
            .types((Set<JobType>) null)
            .priorities(JobPriority.HIGH)
            .priorities((Set<JobPriority>) null)
            .tags("billing")
            .tags((Set<String>) null)
            .build();

    assertNull(filter.statuses());
    assertNull(filter.types());
    assertNull(filter.priorities());
    assertNull(filter.tags());
  }

  @Test
  void canonicalConstructorDefensivelyCopiesSetValues() {
    Set<JobStatus> statuses = new HashSet<>(Set.of(JobStatus.PENDING));
    Set<JobType> types = new HashSet<>(Set.of(JobType.SINGLE));
    Set<JobPriority> priorities = new HashSet<>(Set.of(JobPriority.HIGH));
    Set<String> tags = new HashSet<>(Set.of("billing"));

    JobFilter filter =
        new JobFilter(
            statuses,
            types,
            priorities,
            null,
            null,
            tags,
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
            false,
            null,
            null);

    statuses.add(JobStatus.FAILED);
    types.add(JobType.RECURRING);
    priorities.add(JobPriority.LOW);
    tags.add("reports");

    assertEquals(Set.of(JobStatus.PENDING), filter.statuses());
    assertEquals(Set.of(JobType.SINGLE), filter.types());
    assertEquals(Set.of(JobPriority.HIGH), filter.priorities());
    assertEquals(Set.of("billing"), filter.tags());
    assertThrows(
        UnsupportedOperationException.class, () -> filter.statuses().add(JobStatus.RUNNING));
    assertThrows(UnsupportedOperationException.class, () -> filter.types().add(JobType.BATCH));
    assertThrows(
        UnsupportedOperationException.class, () -> filter.priorities().add(JobPriority.NORMAL));
    assertThrows(UnsupportedOperationException.class, () -> filter.tags().add("audit"));
  }
}
