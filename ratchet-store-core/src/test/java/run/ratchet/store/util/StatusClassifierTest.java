package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;

class StatusClassifierTest {

  @Test
  void recStatusForLiveStatusEncodesOnlyRecurringColdStatuses() {
    assertEquals("P", StatusClassifier.recStatusForLiveStatus(JobStatus.PENDING));
    assertEquals("A", StatusClassifier.recStatusForLiveStatus(JobStatus.PAUSED));
    assertNull(StatusClassifier.recStatusForLiveStatus(JobStatus.RUNNING));
    assertNull(StatusClassifier.recStatusForLiveStatus(JobStatus.SUCCEEDED));
  }

  @Test
  void recStatusDecodeRecognizesRecurringColdStatuses() {
    assertEquals(JobStatus.PENDING, StatusClassifier.recStatusDecode("P"));
    assertEquals(JobStatus.PAUSED, StatusClassifier.recStatusDecode("A"));
    assertNull(StatusClassifier.recStatusDecode(null));
    assertNull(StatusClassifier.recStatusDecode("R"));
  }
}
