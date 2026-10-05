/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.api.exception;

import java.io.Serial;
import run.ratchet.api.JobContext;
import run.ratchet.api.Nullable;
import run.ratchet.api.internal.AttemptTokens;

/**
 * Thrown by job code, usually via {@link JobContext#throwIfCancellationRequested()}, when it stops
 * at a safe point because Ratchet requested cancellation.
 *
 * <p>The exception records, when created, whether cancellation had been requested and which attempt
 * it was created in. Ratchet treats it as a timeout, with the same retry/DLQ outcome as a hard
 * timeout and no interrupt, only if it was created after the request in the attempt that fails with
 * it. One created before the request, with no {@link JobContext} bound, or in an earlier attempt
 * and rethrown by a retry is an ordinary failure, even if a request arrives while it unwinds.
 *
 * <p>The constructors read the context bound to the current thread. To create the exception on
 * another thread, use {@link #forContext(String, JobContext)} and pass the job's context.
 */
public class CancellationRequestedException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  static {
    AttemptTokens.installStopMatcher(
        (exception, token) ->
            exception.cancellationRequested && token != null && exception.attemptToken == token);
  }

  private final boolean cancellationRequested;
  private final transient @Nullable Object attemptToken;

  /**
   * Records the request state and attempt of the {@link JobContext} bound to the current thread, if
   * any.
   */
  public CancellationRequestedException(String message) {
    this(message, null, JobContext.currentOrNull());
  }

  /**
   * Records the request state and attempt of the {@link JobContext} bound to the current thread, if
   * any.
   */
  public CancellationRequestedException(String message, @Nullable Throwable cause) {
    this(message, cause, JobContext.currentOrNull());
  }

  private CancellationRequestedException(
      String message, @Nullable Throwable cause, @Nullable JobContext context) {
    super(message, cause);
    this.cancellationRequested = requested(context);
    this.attemptToken = AttemptTokens.tokenOf(context);
  }

  /**
   * Creates an exception that records the request state and attempt of {@code context}. Use this
   * off the job thread, where no context is bound; a {@code null} context records no request.
   */
  public static CancellationRequestedException forContext(
      String message, @Nullable JobContext context) {
    return new CancellationRequestedException(message, null, context);
  }

  /**
   * Creates an exception with a cause that records the request state and attempt of {@code
   * context}. Use this off the job thread, where no context is bound; a {@code null} context
   * records no request.
   */
  public static CancellationRequestedException forContext(
      String message, @Nullable Throwable cause, @Nullable JobContext context) {
    return new CancellationRequestedException(message, cause, context);
  }

  private static boolean requested(@Nullable JobContext context) {
    return context != null && context.isCancellationRequested();
  }

  /**
   * Returns whether cancellation had been requested for the attempt when this exception was
   * created. A timeout also requires that the exception belongs to the failing attempt.
   */
  public boolean isCancellationRequested() {
    return cancellationRequested;
  }
}
