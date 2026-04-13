package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobEntity;
import java.util.List;
import org.junit.jupiter.api.Test;

class TagStoreIT extends BaseDocumentStoreIT {

  @Test
  void insertAndQueryTags() {
    JobEntity job = store().save(newPendingJob());
    store().insertTags(job.getId(), List.of("priority", "batch-run-42"));

    List<Long> found = store().findJobIdsByTag("priority", 100, 0);
    assertEquals(1, found.size());
    assertEquals(job.getId(), found.get(0));

    found = store().findJobIdsByTag("batch-run-42", 100, 0);
    assertEquals(1, found.size());
  }

  @Test
  void insertTags_deduplicates() {
    JobEntity job = store().save(newPendingJob());
    store().insertTags(job.getId(), List.of("tag-a", "tag-b"));
    store().insertTags(job.getId(), List.of("tag-b", "tag-c")); // tag-b already exists

    JobEntity reloaded = store().findById(job.getId()).orElseThrow();
    assertEquals(3, reloaded.getTags().size());
    assertTrue(reloaded.getTags().containsAll(List.of("tag-a", "tag-b", "tag-c")));
  }

  @Test
  void deleteTagsByJobId_removesAll() {
    JobEntity job = store().save(newPendingJob());
    store().insertTags(job.getId(), List.of("x", "y", "z"));

    int removed = store().deleteTagsByJobId(job.getId());
    assertEquals(3, removed);

    JobEntity reloaded = store().findById(job.getId()).orElseThrow();
    assertTrue(reloaded.getTags().isEmpty());
  }

  @Test
  void findJobIdsByTag_pagination() {
    for (int i = 0; i < 5; i++) {
      JobEntity job = store().save(newPendingJob());
      store().insertTags(job.getId(), List.of("shared-tag"));
    }

    List<Long> page1 = store().findJobIdsByTag("shared-tag", 2, 0);
    assertEquals(2, page1.size());

    List<Long> page2 = store().findJobIdsByTag("shared-tag", 2, 2);
    assertEquals(2, page2.size());

    page1.retainAll(page2);
    assertTrue(page1.isEmpty());
  }
}
