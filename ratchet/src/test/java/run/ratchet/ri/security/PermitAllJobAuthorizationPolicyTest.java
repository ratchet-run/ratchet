package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobStatus;

class PermitAllJobAuthorizationPolicyTest {

  private final PermitAllJobAuthorizationPolicy policy = new PermitAllJobAuthorizationPolicy();

  @Test
  void checkCreate_permitsAlways() {
    assertDoesNotThrow(() -> policy.checkCreate(UUID.randomUUID(), "alice"));
  }

  @Test
  void checkCreate_nullPrincipal_permitsAlways() {
    assertDoesNotThrow(() -> policy.checkCreate(UUID.randomUUID(), null));
  }

  @Test
  void checkExecute_permitsAlways() {
    assertDoesNotThrow(() -> policy.checkExecute(UUID.randomUUID(), "owner"));
  }

  @Test
  void checkExecute_nullOwnerPrincipal_permitsAlways() {
    assertDoesNotThrow(() -> policy.checkExecute(UUID.randomUUID(), null));
  }

  @Test
  void checkCancel_permitsAlways() {
    assertDoesNotThrow(() -> policy.checkCancel(UUID.randomUUID(), "owner", "actor"));
  }

  @Test
  void checkPause_permitsAlways() {
    assertDoesNotThrow(() -> policy.checkPause(UUID.randomUUID(), "owner", "actor"));
  }

  @Test
  void checkResume_permitsAlways() {
    assertDoesNotThrow(() -> policy.checkResume(UUID.randomUUID(), "owner", "actor"));
  }

  @Test
  void checkRetry_permitsAlways() {
    assertDoesNotThrow(() -> policy.checkRetry(UUID.randomUUID(), "owner", "actor"));
  }

  @Test
  void checkRead_permitsAlways() {
    assertDoesNotThrow(() -> policy.checkRead(UUID.randomUUID(), "actor"));
  }

  @Test
  void checkRead_nullCallerPrincipal_permitsAlways() {
    assertDoesNotThrow(() -> policy.checkRead(UUID.randomUUID(), null));
  }

  @Test
  void filterForPrincipal_returnsOriginalFilterUnchanged() {
    JobFilter original =
        JobFilter.builder()
            .statuses(JobStatus.PENDING)
            .callerPrincipal("requested-principal")
            .businessKey("billing-42")
            .build();

    JobFilter scoped = policy.filterForPrincipal(original, "current-principal");

    assertSame(original, scoped);
  }

  @Test
  void filterForPrincipal_nullCallerPrincipal_returnsOriginalFilterUnchanged() {
    JobFilter original = JobFilter.builder().businessKey("system-job").build();

    JobFilter scoped = policy.filterForPrincipal(original, null);

    assertSame(original, scoped);
  }
}
