package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class JobEntityCallerPrincipalTest {

  @Test
  void firstNonNullSet_isAccepted() {
    JobEntity job = new JobEntity();
    job.setCallerPrincipal("alice");
    assertEquals("alice", job.getCallerPrincipal());
  }

  @Test
  void idempotentResetToSameValue_isAllowed() {
    JobEntity job = new JobEntity();
    job.setCallerPrincipal("alice");
    job.setCallerPrincipal("alice");
    assertEquals("alice", job.getCallerPrincipal());
  }

  @Test
  void overwriteWithDifferentValue_isSilentNoOp() {
    JobEntity job = new JobEntity();
    job.setCallerPrincipal("alice");
    job.setCallerPrincipal("bob");
    assertEquals(
        "alice",
        job.getCallerPrincipal(),
        "Caller principal must be write-once: 'bob' must not overwrite 'alice'");
  }

  @Test
  void clearWithNullAfterSet_isSilentNoOp() {
    JobEntity job = new JobEntity();
    job.setCallerPrincipal("alice");
    job.setCallerPrincipal(null);
    assertEquals(
        "alice",
        job.getCallerPrincipal(),
        "Caller principal must be write-once: null must not clear 'alice'");
  }

  @Test
  void nullSetBeforeAnyValue_leavesFieldNullAndAllowsLaterSet() {
    JobEntity job = new JobEntity();
    job.setCallerPrincipal(null);
    assertNull(job.getCallerPrincipal());
    job.setCallerPrincipal("alice");
    assertEquals("alice", job.getCallerPrincipal());
  }
}
