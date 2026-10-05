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

/**
 * Thrown by job code, usually via {@link JobContext#throwIfCancellationRequested()}, when it stops
 * at a safe point because Ratchet requested cancellation.
 *
 * <p>The exception records, when it is created, whether cancellation had already been requested for
 * the attempt. Ratchet classifies the failure from that recorded state, not from the flag at the
 * time the failure is handled. An exception created after the request gives the attempt the same
 * retry/DLQ timeout outcome as a hard timeout, without an interrupt. An exception created before
 * the request, or with no {@link JobContext} bound, is an ordinary failure, even if the request
 * arrives while the exception unwinds through {@code finally} blocks or wrappers.
 *
 * <p>The constructors read the context bound to the current thread. To create the exception on
 * another thread, use {@link #forContext(String, JobContext)} and pass the job's context.
 */
public class CancellationRequestedException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  private final boolean cancellationRequested;

  /** Records the request state of the {@link JobContext} bound to the current thread, if any. */
  public CancellationRequestedException(String message) {
    this(message, null, requested(JobContext.currentOrNull()));
  }

  /** Records the request state of the {@link JobContext} bound to the current thread, if any. */
  public CancellationRequestedException(String message, @Nullable Throwable cause) {
    this(message, cause, requested(JobContext.currentOrNull()));
  }

  private CancellationRequestedException(
      String message, @Nullable Throwable cause, boolean cancellationRequested) {
    super(message, cause);
    this.cancellationRequested = cancellationRequested;
  }

  /**
   * Creates an exception that records the request state of {@code context}. Use this off the job
   * thread, where no context is bound; a {@code null} context records no request.
   */
  public static CancellationRequestedException forContext(
      String message, @Nullable JobContext context) {
    return new CancellationRequestedException(message, null, requested(context));
  }

  /**
   * Creates an exception with a cause that records the request state of {@code context}. Use this
   * off the job thread, where no context is bound; a {@code null} context records no request.
   */
  public static CancellationRequestedException forContext(
      String message, @Nullable Throwable cause, @Nullable JobContext context) {
    return new CancellationRequestedException(message, cause, requested(context));
  }

  private static boolean requested(@Nullable JobContext context) {
    return context != null && context.isCancellationRequested();
  }

  /**
   * Returns whether cancellation had been requested for the attempt when this exception was
   * created. Ratchet treats the failure as a timeout only when this is true.
   */
  public boolean isCancellationRequested() {
    return cancellationRequested;
  }
}
