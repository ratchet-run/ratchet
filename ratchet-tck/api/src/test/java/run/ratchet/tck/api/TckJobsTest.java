package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TckJobsTest {

  @AfterEach
  void resetTckJobs() {
    TckJobs.resetAll();
  }

  @Test
  void blockUntilReleasedRequiresBeginBlocking() {
    assertThrows(IllegalStateException.class, TckJobs::blockUntilReleased);
  }
}
