package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import run.ratchet.api.JobType;

class ArchivedJobEntityTest {

  @Test
  void getPublicJobTypeConvertsStoredJobType() {
    ArchivedJobEntity archive = new ArchivedJobEntity();
    archive.setJobType(JobExecutionType.BATCH_CHILD);

    assertEquals(JobType.BATCH, archive.getPublicJobType());
  }

  @Test
  void getPublicJobTypeRejectsMissingJobType() {
    ArchivedJobEntity archive = new ArchivedJobEntity();

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, archive::getPublicJobType);

    assertEquals("Archived job type is not set", thrown.getMessage());
  }
}
