package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code TagStore}. */
public abstract class AbstractTagStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupTagFixture() {
    cleanupStore();
  }

  @Test
  void insertTags_andFindByTag_returnsJobId() {
    var saved = persist(newPendingJob());
    store().insertTags(saved.getId(), List.of("tag1", "tag2"));

    List<Long> ids = store().findJobIdsByTag("tag1", 10, 0);

    assertTrue(ids.contains(saved.getId()), "findJobIdsByTag should return the tagged job");
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

    List<Long> page1 = store().findJobIdsByTag("shared", 2, 0);
    List<Long> page2 = store().findJobIdsByTag("shared", 2, 2);

    assertEquals(2, page1.size(), "First page should contain 2 results");
    assertEquals(1, page2.size(), "Second page should contain 1 result");
  }
}
