package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.DlqAlertEntity;

/** Base contract tests for {@code DlqAlertStore}. */
public abstract class AbstractDlqAlertStoreContract implements JobStoreContractFixture {

  @BeforeEach
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
        store()
            .existsRecentDlqAlert(
                new UUID(0L, 999_999L), "nonexistent", Instant.now().minusSeconds(60));

    assertFalse(exists, "existsRecentDlqAlert should return false when no matching alert exists");
  }

  @Test
  void existsRecentDlqAlert_returnsFalseForExpiredAlert() {
    var job = persist(newPendingJob());

    DlqAlertEntity alert = new DlqAlertEntity();
    alert.setJobId(job.getId());
    alert.setErrorHash("expired-hash");
    alert.setAlertSentAt(Instant.now().minusSeconds(7200));
    alert.setAlertChannel("test-channel");
    store().saveDlqAlert(alert);

    // Cutoff is 1 hour ago — the alert is 2 hours old, so it should not match
    boolean exists =
        store().existsRecentDlqAlert(job.getId(), "expired-hash", Instant.now().minusSeconds(3600));

    assertFalse(exists, "existsRecentDlqAlert should return false for an alert older than cutoff");
  }

  @Test
  void existsRecentDlqAlert_differentErrorHash_returnsFalse() {
    var job = persist(newPendingJob());

    DlqAlertEntity alert = new DlqAlertEntity();
    alert.setJobId(job.getId());
    alert.setErrorHash("hash-A");
    alert.setAlertSentAt(Instant.now());
    alert.setAlertChannel("test-channel");
    store().saveDlqAlert(alert);

    boolean exists =
        store().existsRecentDlqAlert(job.getId(), "hash-B", Instant.now().minusSeconds(60));

    assertFalse(exists, "existsRecentDlqAlert should return false when error hash does not match");
  }

  @Test
  void saveDlqAlert_andRetrieve_roundTripsFields() {
    var job = persist(newPendingJob());
    Instant sentAt = Instant.now();

    DlqAlertEntity alert = new DlqAlertEntity();
    alert.setJobId(job.getId());
    alert.setErrorHash("roundtrip-hash");
    alert.setAlertSentAt(sentAt);
    alert.setAlertChannel("email");
    store().saveDlqAlert(alert);

    boolean exists =
        store().existsRecentDlqAlert(job.getId(), "roundtrip-hash", sentAt.minusSeconds(1));

    assertTrue(exists, "Alert should be retrievable after save");
  }

  @Test
  void existsRecentDlqAlert_exactCutoffBoundary() {
    var job = persist(newPendingJob());
    Instant alertTime = Instant.now().minusSeconds(60);

    DlqAlertEntity alert = new DlqAlertEntity();
    alert.setJobId(job.getId());
    alert.setErrorHash("boundary-hash");
    alert.setAlertSentAt(alertTime);
    alert.setAlertChannel("test-channel");
    store().saveDlqAlert(alert);

    // Cutoff exactly at alert time — alert should be included (at or after cutoff)
    boolean exists = store().existsRecentDlqAlert(job.getId(), "boundary-hash", alertTime);

    assertTrue(exists, "existsRecentDlqAlert with cutoff at exact alert time should return true");
  }
}
