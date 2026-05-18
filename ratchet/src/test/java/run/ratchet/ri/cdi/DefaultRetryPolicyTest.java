package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class DefaultRetryPolicyTest {

  @Test
  void defaultPolicyIsOnlyAPassthrough() {
    DefaultRetryPolicy policy = new DefaultRetryPolicy();

    assertTrue(policy.shouldRetry(1, new IllegalStateException("failed")));
    assertTrue(policy.shouldRetry(Integer.MAX_VALUE, new RuntimeException("still failed")));
    assertEquals(Duration.ZERO, policy.getDelay(Integer.MAX_VALUE));
  }
}
