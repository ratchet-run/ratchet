package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.DlqAlertEntity;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code DlqAlertStore}. */
public abstract class AbstractDlqAlertStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupDlqAlertFixture() {
    cleanupStore();
  }

  @Test
  void saveDlqAlert_persistsAlert() {
    var job = persist(newPendingJob());

    DlqAlertEntity alert = new DlqAlertEntity();
    alert.setJobId(job.getId());
    alert.setErrorHash("abc123hash");
    alert.setAlertSentAt(Instant.now());
    alert.setAlertChannel("test-channel");

    store().saveDlqAlert(alert);
    // No exception means the alert was persisted successfully
  }

  @Test
  void existsRecentDlqAlert_returnsTrueForRecentAlert() {
    var job = persist(newPendingJob());

    DlqAlertEntity alert = new DlqAlertEntity();
    alert.setJobId(job.getId());
    alert.setErrorHash("abc123hash");
    alert.setAlertSentAt(Instant.now());
    alert.setAlertChannel("test-channel");

    store().saveDlqAlert(alert);

    boolean exists =
        store().existsRecentDlqAlert(job.getId(), "abc123hash", Instant.now().minusSeconds(60));

    assertTrue(exists, "existsRecentDlqAlert should return true for a recently saved alert");
  }

  @Test
  void existsRecentDlqAlert_returnsFalseWhenNoneExists() {
    boolean exists =
        store().existsRecentDlqAlert(999_999L, "nonexistent", Instant.now().minusSeconds(60));

    assertFalse(exists, "existsRecentDlqAlert should return false when no matching alert exists");
  }
}
