package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.UUID;
import org.junit.jupiter.api.Test;

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
}
