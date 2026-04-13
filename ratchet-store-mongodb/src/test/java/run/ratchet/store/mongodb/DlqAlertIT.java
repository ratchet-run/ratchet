package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobEntity;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class DlqAlertIT extends BaseDocumentStoreIT {

  @Test
  void saveDlqAlert_and_checkRecent() {
    JobEntity job = store().save(newPendingJob());

    DlqAlertEntity alert = new DlqAlertEntity();
    alert.setJobId(job.getId());
    alert.setErrorHash("abc123");
    alert.setAlertSentAt(Instant.now());
    alert.setAlertChannel("test-channel");

    DlqAlertEntity saved = store().saveDlqAlert(alert);
    assertNotNull(saved.getId());

    boolean exists =
        store().existsRecentDlqAlert(job.getId(), "abc123", Instant.now().minusSeconds(60));
    assertTrue(exists);

    boolean notExists =
        store().existsRecentDlqAlert(job.getId(), "different-hash", Instant.now().minusSeconds(60));
    assertFalse(notExists);
  }

  @Test
  void existsRecentDlqAlert_respectsCutoff() {
    JobEntity job = store().save(newPendingJob());

    DlqAlertEntity alert = new DlqAlertEntity();
    alert.setJobId(job.getId());
    alert.setErrorHash("old-hash");
    alert.setAlertSentAt(Instant.now().minusSeconds(120));
    alert.setAlertChannel("test-channel");
    store().saveDlqAlert(alert);

    // Cutoff is 60 seconds ago — the 120-second-old alert should not match
    boolean exists =
        store().existsRecentDlqAlert(job.getId(), "old-hash", Instant.now().minusSeconds(60));
    assertFalse(exists);
  }
}
