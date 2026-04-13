package run.ratchet.ri.core;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/** Result of a job submission attempt: either accepted (with a {@link Future}) or rejected. */
public record ExecutionResult(
    boolean rejected, Future<Void> future, RejectedExecutionException exception) {

  /** Creates a rejected result. */
  public static ExecutionResult rejected(RejectedExecutionException exception) {
    return new ExecutionResult(true, null, exception);
  }

  /** Creates a successful result. */
  public static ExecutionResult success(Future<Void> future) {
    return new ExecutionResult(false, future, null);
  }

  public boolean isRejected() {
    return rejected;
  }
}
