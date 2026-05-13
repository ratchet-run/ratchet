package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.LambdaDescriptor;

class ApiValueImmutabilityTest {

  @Test
  void jobDetailDefensivelyCopiesCollections() {
    Map<String, String> params = new HashMap<>();
    params.put("tenant", "a");
    Map<String, String> trace = new HashMap<>();
    trace.put("traceparent", "one");
    List<ExecutionHistorySummary> history = new ArrayList<>();
    List<UUID> dependants = new ArrayList<>();
    UUID dependant = UUID.randomUUID();
    dependants.add(dependant);

    JobDetail detail =
        new JobDetail(
            summary(List.of("blue")),
            params,
            trace,
            null,
            null,
            null,
            null,
            null,
            null,
            history,
            dependants);

    params.put("tenant", "b");
    trace.put("traceparent", "two");
    dependants.add(UUID.randomUUID());

    assertEquals("a", detail.params().get("tenant"));
    assertEquals("one", detail.traceContext().get("traceparent"));
    assertEquals(List.of(dependant), detail.dependantJobIds());
    assertThrows(UnsupportedOperationException.class, () -> detail.params().put("x", "y"));
    assertThrows(
        UnsupportedOperationException.class, () -> detail.dependantJobIds().add(UUID.randomUUID()));
  }

  @Test
  void jobDetailDefaultsNullCollectionsToEmptyLists() {
    JobDetail detail =
        new JobDetail(
            summary(List.of("blue")), null, null, null, null, null, null, null, null, null, null);

    assertEquals(List.of(), detail.executionHistory());
    assertEquals(List.of(), detail.dependantJobIds());
  }

  @Test
  void jobSummaryDefensivelyCopiesTags() {
    List<String> tags = new ArrayList<>(List.of("alpha"));

    JobSummary summary = summary(tags);
    tags.add("beta");

    assertEquals(List.of("alpha"), summary.tags());
    assertThrows(UnsupportedOperationException.class, () -> summary.tags().add("gamma"));
  }

  @Test
  void jobSummaryDefaultsNullTagsToEmptyList() {
    assertEquals(List.of(), summary(null).tags());
  }

  @Test
  void queueHealthSnapshotDefensivelyCopiesBreakdowns() {
    Map<JobType, Long> byType = new EnumMap<>(JobType.class);
    byType.put(JobType.SINGLE, 1L);
    Map<JobPriority, Long> byPriority = new EnumMap<>(JobPriority.class);
    byPriority.put(JobPriority.NORMAL, 2L);

    QueueHealthSnapshot snapshot =
        new QueueHealthSnapshot(1, 0, 0, 0, 0, 0, 0, 0, 1, 0.0, 0.0, 0, null, byType, byPriority);
    byType.put(JobType.RECURRING, 9L);
    byPriority.put(JobPriority.HIGH, 9L);

    assertEquals(Map.of(JobType.SINGLE, 1L), snapshot.pendingByType());
    assertEquals(Map.of(JobPriority.NORMAL, 2L), snapshot.pendingByPriority());
    assertThrows(
        UnsupportedOperationException.class, () -> snapshot.pendingByType().put(JobType.BATCH, 3L));
  }

  @Test
  void queueHealthSnapshotDefaultsNullBreakdownsToEmptyMaps() {
    QueueHealthSnapshot snapshot =
        new QueueHealthSnapshot(1, 0, 0, 0, 0, 0, 0, 0, 1, 0.0, 0.0, 0, null, null, null);

    assertEquals(Map.of(), snapshot.pendingByType());
    assertEquals(Map.of(), snapshot.pendingByPriority());
  }

  @Test
  void lambdaDescriptorDefensivelyCopiesCapturedArgs() {
    Object[] captured = {"first"};

    LambdaDescriptor descriptor = new LambdaDescriptor("Target", "method", "()V", false, captured);
    captured[0] = "changed";
    Object[] returned = descriptor.capturedArgs();
    returned[0] = "also changed";

    assertEquals("first", descriptor.capturedArgs()[0]);
  }

  private static JobSummary summary(List<String> tags) {
    return new JobSummary(
        UUID.randomUUID(),
        JobStatus.PENDING,
        JobType.SINGLE,
        JobPriority.NORMAL,
        null,
        null,
        "Target",
        "method",
        tags,
        null,
        null,
        Instant.EPOCH,
        Instant.EPOCH,
        Instant.EPOCH,
        null,
        null,
        0,
        3,
        null);
  }
}
