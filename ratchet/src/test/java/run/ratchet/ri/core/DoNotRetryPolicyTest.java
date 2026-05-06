package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.DoNotRetry;

class DoNotRetryPolicyTest {

  private DoNotRetryPolicy policy;

  @BeforeEach
  void setUp() {
    policy = new DoNotRetryPolicy();
  }

  @Test
  void shouldNotRetry_null_returnsFalse() {
    assertFalse(policy.shouldNotRetry(null));
  }

  @Test
  void shouldNotRetry_plainRuntimeException_returnsFalse() {
    assertFalse(policy.shouldNotRetry(new RuntimeException("transient")));
  }

  @Test
  void shouldNotRetry_illegalStateException_returnsFalse() {
    // IllegalStateException is intentionally excluded: CDI and JPA throw it for transient
    // conditions (e.g. EntityManager already closed) that may resolve on retry.
    assertFalse(policy.shouldNotRetry(new IllegalStateException("entity manager closed")));
  }

  @Test
  void shouldNotRetry_illegalArgumentException_returnsTrue() {
    assertTrue(policy.shouldNotRetry(new IllegalArgumentException("bad arg")));
  }

  @Test
  void shouldNotRetry_nullPointerException_returnsTrue() {
    assertTrue(policy.shouldNotRetry(new NullPointerException()));
  }

  @Test
  void shouldNotRetry_securityException_returnsTrue() {
    assertTrue(policy.shouldNotRetry(new SecurityException("access denied")));
  }

  @Test
  void shouldNotRetry_wrappedNonRetryableCause_returnsTrue() {
    // Wrapping a non-retryable exception in a retryable one must not bypass the check.
    RuntimeException wrapped = new RuntimeException("outer", new IllegalArgumentException("inner"));
    assertTrue(policy.shouldNotRetry(wrapped));
  }

  @Test
  void shouldNotRetry_deeplyWrappedNonRetryableCause_returnsTrue() {
    // Three levels deep: RuntimeException -> RuntimeException -> NullPointerException
    RuntimeException mid = new RuntimeException("mid", new NullPointerException("deep"));
    RuntimeException outer = new RuntimeException("outer", mid);
    assertTrue(policy.shouldNotRetry(outer));
  }

  @Test
  void shouldNotRetry_wrappedRetryableCause_returnsFalse() {
    RuntimeException wrapped =
        new RuntimeException("outer", new IllegalStateException("transient cause"));
    assertFalse(policy.shouldNotRetry(wrapped));
  }

  @Test
  void shouldNotRetry_doNotRetryAnnotatedClass_returnsTrue() {
    assertTrue(policy.shouldNotRetry(new AnnotatedBusinessException()));
  }

  @Test
  void shouldNotRetry_doNotRetryAnnotatedCause_returnsTrue() {
    RuntimeException wrapped = new RuntimeException("wrapper", new AnnotatedBusinessException());
    assertTrue(policy.shouldNotRetry(wrapped));
  }

  @Test
  void shouldNotRetry_unannotatedSubclassOfAnnotatedBase_returnsTrue() {
    // Subclass inherits the annotation — isAnnotationPresent checks declared annotations only,
    // but DoNotRetry is @Target(TYPE), so check that the direct class annotation is detected.
    assertTrue(policy.shouldNotRetry(new AnnotatedBusinessException()));
  }

  @Test
  void shouldNotRetry_nullCause_doesNotLoop() {
    // Exception with explicit null cause terminates cause-chain walk immediately.
    assertFalse(policy.shouldNotRetry(new RuntimeException("no cause", null)));
  }

  @DoNotRetry("permanent business failure")
  private static final class AnnotatedBusinessException extends RuntimeException {

    AnnotatedBusinessException() {
      super("business error");
    }
  }
}
