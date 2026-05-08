package run.ratchet.testsuite.app;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.JobFilter;
import run.ratchet.api.exception.JobAuthorizationException;
import run.ratchet.spi.JobAuthorizationPolicy;

/**
 * {@link JobAuthorizationPolicy} {@code @Alternative} that permits every operation and tracks
 * per-method invocation counts. Used in IT tests to verify the framework invokes the policy at the
 * right lifecycle points without blocking execution.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class StubJobAuthorizationPolicy implements JobAuthorizationPolicy {

  private static final AtomicInteger CREATE_COUNT = new AtomicInteger(0);
  private static final AtomicInteger EXECUTE_COUNT = new AtomicInteger(0);
  private static final AtomicInteger CANCEL_COUNT = new AtomicInteger(0);
  private static final AtomicInteger PAUSE_COUNT = new AtomicInteger(0);
  private static final AtomicInteger RESUME_COUNT = new AtomicInteger(0);
  private static final AtomicInteger RETRY_COUNT = new AtomicInteger(0);
  private static final AtomicInteger READ_COUNT = new AtomicInteger(0);
  private static final AtomicInteger FILTER_COUNT = new AtomicInteger(0);

  public static int getCreateCount() {
    return CREATE_COUNT.get();
  }

  public static int getExecuteCount() {
    return EXECUTE_COUNT.get();
  }

  public static int getCancelCount() {
    return CANCEL_COUNT.get();
  }

  public static int getPauseCount() {
    return PAUSE_COUNT.get();
  }

  public static int getResumeCount() {
    return RESUME_COUNT.get();
  }

  public static int getRetryCount() {
    return RETRY_COUNT.get();
  }

  public static int getReadCount() {
    return READ_COUNT.get();
  }

  public static int getFilterCount() {
    return FILTER_COUNT.get();
  }

  public static void resetAll() {
    CREATE_COUNT.set(0);
    EXECUTE_COUNT.set(0);
    CANCEL_COUNT.set(0);
    PAUSE_COUNT.set(0);
    RESUME_COUNT.set(0);
    RETRY_COUNT.set(0);
    READ_COUNT.set(0);
    FILTER_COUNT.set(0);
  }

  @Override
  public void checkCreate(UUID jobId, String callerPrincipal) throws JobAuthorizationException {
    CREATE_COUNT.incrementAndGet();
  }

  @Override
  public void checkExecute(UUID jobId, String ownerPrincipal) throws JobAuthorizationException {
    EXECUTE_COUNT.incrementAndGet();
  }

  @Override
  public void checkCancel(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {
    CANCEL_COUNT.incrementAndGet();
  }

  @Override
  public void checkPause(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {
    PAUSE_COUNT.incrementAndGet();
  }

  @Override
  public void checkResume(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {
    RESUME_COUNT.incrementAndGet();
  }

  @Override
  public void checkRetry(UUID jobId, String ownerPrincipal, String currentPrincipal)
      throws JobAuthorizationException {
    RETRY_COUNT.incrementAndGet();
  }

  @Override
  public void checkRead(UUID jobId, String callerPrincipal) throws JobAuthorizationException {
    READ_COUNT.incrementAndGet();
  }

  @Override
  public JobFilter filterForPrincipal(JobFilter filter, String callerPrincipal) {
    FILTER_COUNT.incrementAndGet();
    return filter;
  }
}
