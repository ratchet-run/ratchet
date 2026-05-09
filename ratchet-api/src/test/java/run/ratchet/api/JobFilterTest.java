package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
