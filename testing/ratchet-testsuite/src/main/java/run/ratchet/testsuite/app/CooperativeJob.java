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
package run.ratchet.testsuite.app;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import run.ratchet.api.JobContext;

/** A job that stops at a safe point when its watchdog requests cancellation. */
public class CooperativeJob {
  public static final AtomicBoolean STARTED = new AtomicBoolean();
  public static final AtomicBoolean SAW_REQUEST = new AtomicBoolean();
  public static final AtomicBoolean INTERRUPTED = new AtomicBoolean();
  public static final AtomicBoolean STOPPED_COOPERATIVELY = new AtomicBoolean();
  public static final AtomicBoolean COMPLETED = new AtomicBoolean();
  public static final AtomicReference<Instant> DEADLINE = new AtomicReference<>();
  public static final AtomicInteger FAILURE_CALLBACKS = new AtomicInteger();

  public static void execute() {
    STARTED.set(true);
    DEADLINE.set(JobContext.current().deadline().orElse(null));
    try {
      for (int i = 0; i < 120; i++) {
        if (JobContext.current().isCancellationRequested()) {
          SAW_REQUEST.set(true);
          STOPPED_COOPERATIVELY.set(true);
          JobContext.current().throwIfCancellationRequested();
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          INTERRUPTED.set(true);
          Thread.currentThread().interrupt();
          throw new RuntimeException(ex);
        }
      }
      COMPLETED.set(true);
    } finally {
      if (Thread.currentThread().isInterrupted()) {
        INTERRUPTED.set(true);
      }
    }
  }

  public static void recordFailureCallback() {
    FAILURE_CALLBACKS.incrementAndGet();
  }

  public static void reset() {
    STARTED.set(false);
    SAW_REQUEST.set(false);
    INTERRUPTED.set(false);
    STOPPED_COOPERATIVELY.set(false);
    COMPLETED.set(false);
    DEADLINE.set(null);
    FAILURE_CALLBACKS.set(0);
  }
}
