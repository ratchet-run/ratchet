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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.JobContext;

/** A cancellable body whose exit can be held while the test checks its business key. */
public class CancellableJob {
  public static final AtomicBoolean STARTED = new AtomicBoolean();
  public static final AtomicBoolean SAW_REQUEST = new AtomicBoolean();
  public static final AtomicBoolean INTERRUPTED = new AtomicBoolean();
  public static final AtomicBoolean COMPLETED = new AtomicBoolean();
  public static final AtomicBoolean ENDED = new AtomicBoolean();
  public static final AtomicInteger FAILURE_CALLBACKS = new AtomicInteger();
  public static volatile boolean HOLD_AFTER_REQUEST;
  private static volatile CountDownLatch release = new CountDownLatch(1);

  public static void execute() {
    STARTED.set(true);
    try {
      for (int i = 0; i < 600; i++) {
        if (JobContext.current().isCancellationRequested()) {
          SAW_REQUEST.set(true);
          if (HOLD_AFTER_REQUEST && !release.await(60, TimeUnit.SECONDS)) {
            throw new IllegalStateException("The test did not release the job within 60 s");
          }
          JobContext.current().throwIfCancellationRequested();
        }
        Thread.sleep(100);
      }
      COMPLETED.set(true);
    } catch (InterruptedException ex) {
      INTERRUPTED.set(true);
      Thread.currentThread().interrupt();
      throw new RuntimeException(ex);
    } finally {
      ENDED.set(true);
    }
  }

  public static void recordFailureCallback() {
    FAILURE_CALLBACKS.incrementAndGet();
  }

  public static void release() {
    release.countDown();
  }

  public static void reset() {
    STARTED.set(false);
    SAW_REQUEST.set(false);
    INTERRUPTED.set(false);
    COMPLETED.set(false);
    ENDED.set(false);
    FAILURE_CALLBACKS.set(0);
    HOLD_AFTER_REQUEST = false;
    release = new CountDownLatch(1);
  }
}
