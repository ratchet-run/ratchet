package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;

class MongoJobQueryOperationsTest {

  @Test
  void archiveSearchRejectsDeepOffsetWithoutCursor() {
    MongoJobQueryOperations operations = new MongoJobQueryOperations(null);
    JobFilter filter = JobFilter.builder().includeArchived(true).build();

    assertThrows(IllegalArgumentException.class, () -> operations.searchJobs(filter, 1000, 1001));
  }
}
