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
}
