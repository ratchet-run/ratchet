package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;

/** Base contract tests for {@code TagStore}. */
public abstract class AbstractTagStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupTagFixture() {
    cleanupStore();
  }

  @Test
  void insertTags_andFindByTag_returnsJobId() {
    var saved = persist(newPendingJob());
    store().insertTags(saved.getId(), List.of("tag1", "tag2"));

    List<UUID> ids = store().findJobIdsByTag("tag1", 10, 0);

    assertTrue(ids.contains(saved.getId()), "findJobIdsByTag should return the tagged job");
    assertTrue(
        store().findJobIdsByTag("tag2", 10, 0).contains(saved.getId()),
        "findJobIdsByTag should return the job for every inserted tag");
  }

  @Test
  void deleteTagsByJobId_removesAllTags() {
    var saved = persist(newPendingJob());
    store().insertTags(saved.getId(), List.of("tag1", "tag2"));

    int deleted = store().deleteTagsByJobId(saved.getId());

    assertTrue(deleted > 0, "deleteTagsByJobId should report removed tags");
    assertTrue(
        store().findJobIdsByTag("tag1", 10, 0).isEmpty(),
        "findJobIdsByTag should return empty after deletion");
    assertTrue(
        store().findJobIdsByTag("tag2", 10, 0).isEmpty(),
        "findJobIdsByTag should return empty after deletion");
  }

  @Test
  void findJobIdsByTag_respectsPagination() {
    var first = persist(newPendingJob());
    var second = persist(newPendingJob());
    var third = persist(newPendingJob());

    store().insertTags(first.getId(), List.of("shared"));
    store().insertTags(second.getId(), List.of("shared"));
    store().insertTags(third.getId(), List.of("shared"));

    List<UUID> page1 = store().findJobIdsByTag("shared", 2, 0);
    List<UUID> page2 = store().findJobIdsByTag("shared", 2, 2);

    assertEquals(2, page1.size(), "First page should contain 2 results");
    assertEquals(1, page2.size(), "Second page should contain 1 result");
    assertTrue(
        page1.stream().noneMatch(page2::contains),
        "Second page should contain ids not returned on the first page");
    assertEquals(
        List.of(first.getId(), second.getId(), third.getId()).stream()
            .filter(page2::contains)
            .count(),
        page2.size(),
        "Second page should contain one of the remaining tagged jobs");
  }

  @Test
  void insertTags_duplicateTag_isIdempotent() {
    var saved = persist(newPendingJob());

    assertDoesNotThrow(
        () -> {
          store().insertTags(saved.getId(), List.of("dup-tag"));
          store().insertTags(saved.getId(), List.of("dup-tag"));
        },
        "Inserting the same tag twice should not throw");
  }

  @Test
  void findJobIdsByTag_unknownTag_returnsEmpty() {
    List<UUID> ids = store().findJobIdsByTag("nonexistent-tag", 10, 0);

    assertTrue(ids.isEmpty(), "findJobIdsByTag with unknown tag should return empty");
  }

  @Test
  void findJobIdsByTag_paginationOffset_skipsRows() {
    for (int i = 0; i < 5; i++) {
      var job = persist(newPendingJob());
      store().insertTags(job.getId(), List.of("offset-tag"));
    }

    List<UUID> page = store().findJobIdsByTag("offset-tag", 10, 3);

    assertEquals(2, page.size(), "Offset 3 with 5 total should return 2 results");
  }

  @Test
  void countJobsByParamForTag_supportsLiteralParamKeysWithDots() {
    var first = newPendingJob();
    first.setParams(Map.of("loadtest.enqueue.node", "node-a"));
    first = persist(first);
    store().insertTags(first.getId(), List.of("run-tag"));

    var second = newPendingJob();
    second.setParams(Map.of("loadtest.enqueue.node", "node-a"));
    second = persist(second);
    store().insertTags(second.getId(), List.of("run-tag"));

    var third = newPendingJob();
    third.setParams(Map.of("loadtest.enqueue.node", "node-b"));
    third = persist(third);
    store().insertTags(third.getId(), List.of("run-tag"));

    Map<String, Long> counts = store().countJobsByParamForTag("run-tag", "loadtest.enqueue.node");

    assertEquals(2L, counts.get("node-a"));
    assertEquals(1L, counts.get("node-b"));
  }

  @Test
  void countJobsByStatusForTag_groupsOnlyTaggedJobs() {
    var pending = newPendingJob();
    pending.setStatus(JobStatus.PENDING);
    pending = persist(pending);
    store().insertTags(pending.getId(), List.of("run-tag"));

    var running = newPendingJob();
    running.setStatus(JobStatus.RUNNING);
    running = persist(running);
    store().insertTags(running.getId(), List.of("run-tag"));

    var secondRunning = newPendingJob();
    secondRunning.setStatus(JobStatus.RUNNING);
    secondRunning = persist(secondRunning);
    store().insertTags(secondRunning.getId(), List.of("run-tag"));

    var otherTag = newPendingJob();
    otherTag.setStatus(JobStatus.FAILED);
    otherTag = persist(otherTag);
    store().insertTags(otherTag.getId(), List.of("other-tag"));

    Map<JobStatus, Long> counts = store().countJobsByStatusForTag("run-tag");

    assertEquals(Map.of(JobStatus.PENDING, 1L, JobStatus.RUNNING, 2L), counts);
  }

  @Test
  void countJobsByExecutionNodeForTag_groupsOnlyTaggedJobsWithNodes() {
    var first = newPendingJob();
    first.setPickedBy("node-a");
    first = persist(first);
    store().insertTags(first.getId(), List.of("run-tag"));

    var second = newPendingJob();
    second.setPickedBy("node-a");
    second = persist(second);
    store().insertTags(second.getId(), List.of("run-tag"));

    var third = newPendingJob();
    third.setPickedBy("node-b");
    third = persist(third);
    store().insertTags(third.getId(), List.of("run-tag"));

    var unassigned = newPendingJob();
    unassigned = persist(unassigned);
    store().insertTags(unassigned.getId(), List.of("run-tag"));

    var otherTag = newPendingJob();
    otherTag.setPickedBy("node-c");
    otherTag = persist(otherTag);
    store().insertTags(otherTag.getId(), List.of("other-tag"));

    Map<String, Long> counts = store().countJobsByExecutionNodeForTag("run-tag");

    assertEquals(Map.of("node-a", 2L, "node-b", 1L), counts);
  }

  @Test
  void insertTags_emptyList_isNoOp() {
    var saved = persist(newPendingJob());

    assertDoesNotThrow(
        () -> store().insertTags(saved.getId(), List.of()),
        "Inserting empty tag list should not throw");
  }

  @Test
  void deleteTagsByJobId_unknownJob_returnsZero() {
    int deleted = store().deleteTagsByJobId(new UUID(0L, Long.MAX_VALUE));

    assertEquals(0, deleted, "deleteTagsByJobId for unknown job should return 0");
  }
}
