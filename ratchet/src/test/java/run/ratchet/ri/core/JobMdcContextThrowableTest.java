package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.jboss.logging.MDC;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// Regression: MDC keys must be cleared in finally even when a job throws Error.
class JobMdcContextThrowableTest {

  @BeforeEach
  void clearMdcBefore() {
    MDC.clear();
  }

  @AfterEach
  void clearMdcAfter() {
    MDC.clear();
  }

  @Test
  void clearRemovesMdcKeysAfterAssertionError() {
    // Bind via the 4-arg overload that JobTask.call() uses.
    JobMdcContext.bindJobContext(42L, Map.of(), "node-A", "alice");

    assertEquals("42", MDC.get(JobMdcContext.MDC_JOB_ID));
    assertEquals("node-A", MDC.get(JobMdcContext.MDC_NODE));
    assertEquals("alice", MDC.get(JobMdcContext.MDC_JOB_CREATOR));

    // Simulate the JobTask.call() try/finally pattern: Error propagates, finally still runs.
    AssertionError thrown =
        assertThrows(
            AssertionError.class,
            () -> {
              try {
                throw new AssertionError("simulated worker failure");
              } finally {
                JobMdcContext.clear();
              }
            });

    assertNotNull(thrown);
    assertNull(MDC.get(JobMdcContext.MDC_JOB_ID));
    assertNull(MDC.get(JobMdcContext.MDC_NODE));
    assertNull(MDC.get(JobMdcContext.MDC_JOB_CREATOR));
  }

  @Test
  void clearPreservesEnclosingApplicationMdcKeys() {
    // Enclosing application sets its own MDC key — e.g. a request-correlation ID set by a
    // Servlet filter or JAX-RS interceptor before the job was submitted.
    MDC.put("requestId", "req-xyz");

    JobMdcContext.bindJobContext(7L, Map.of(), "node-B", "bob");
    assertEquals("7", MDC.get(JobMdcContext.MDC_JOB_ID));
    assertEquals("req-xyz", MDC.get("requestId"));

    JobMdcContext.clear();

    // Ratchet keys are gone; enclosing-app key survives.
    assertNull(MDC.get(JobMdcContext.MDC_JOB_ID));
    assertNull(MDC.get(JobMdcContext.MDC_NODE));
    assertNull(MDC.get(JobMdcContext.MDC_JOB_CREATOR));
    assertEquals("req-xyz", MDC.get("requestId"));
  }

  @Test
  void clearIsIdempotent() {
    JobMdcContext.bindJobContext(1L, Map.of(), "node-C", "carol");
    JobMdcContext.clear();
    JobMdcContext.clear(); // second call should not throw
    assertNull(MDC.get(JobMdcContext.MDC_JOB_ID));
  }

  @Test
  void clearIsSafeOnUnboundContext() {
    // Defensive: bind() never called, but clear() must still be a no-op.
    JobMdcContext.clear();
    assertNull(MDC.get(JobMdcContext.MDC_JOB_ID));
  }
}
