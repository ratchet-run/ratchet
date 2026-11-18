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
package run.ratchet.testsuite.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class StoreCallGateTest {
  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  @AfterEach
  void releaseGatesAndStopExecutor() throws InterruptedException {
    try {
      StoreCallGate.reset(Duration.ofSeconds(5));
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void wrongCallerMethodPassesThrough() throws Exception {
    UUID jobId = UUID.randomUUID();
    StoreCallGate.Pause pause = StoreCallGate.pauseNext("getJobStatus", Callers.class, "right");

    executor.submit(() -> Callers.wrong(jobId)).get(5, TimeUnit.SECONDS);

    assertNull(pause.jobId());
  }

  @Test
  void callerClassAndMethodMustMatchOnTheSameFrame() throws Exception {
    UUID jobId = UUID.randomUUID();
    StoreCallGate.Pause pause = StoreCallGate.pauseNext("getJobStatus", Callers.class, "right");

    executor.submit(() -> Outer.right(jobId)).get(5, TimeUnit.SECONDS);

    assertNull(pause.jobId());
  }

  @Test
  void matchingCallerParksOnlyTheFirstCall() throws Exception {
    UUID jobId = UUID.randomUUID();
    Thread executorThread = executor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
    StoreCallGate.Pause pause = StoreCallGate.pauseNext("getJobStatus", Callers.class, "right");

    Future<?> first = executor.submit(() -> Callers.right(jobId));
    pause.awaitParked(Duration.ofSeconds(5));
    assertEquals(jobId, pause.jobId());
    assertSame(executorThread, pause.thread());
    assertFalse(first.isDone());
    pause.release();
    first.get(5, TimeUnit.SECONDS);
    pause.awaitReturned(Duration.ofSeconds(5));

    executor.submit(() -> Callers.right(jobId)).get(5, TimeUnit.SECONDS);
  }

  @Test
  void completedNormallyRequiresANormalReturnOnTheSameThread() throws Exception {
    UUID jobId = UUID.randomUUID();
    Thread executorThread = executor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
    executor.submit(() -> Callers.right(jobId)).get(5, TimeUnit.SECONDS);

    assertTrue(StoreCallGate.completedNormally(jobId, "getJobStatus", executorThread));
    assertFalse(StoreCallGate.completedNormally(jobId, "getJobStatus", Thread.currentThread()));

    UUID throwingJobId = UUID.randomUUID();
    Future<?> throwing = executor.submit(() -> Callers.throwing(throwingJobId));
    ExecutionException failure =
        assertThrows(ExecutionException.class, () -> throwing.get(5, TimeUnit.SECONDS));
    assertInstanceOf(IllegalStateException.class, failure.getCause());
    assertFalse(StoreCallGate.completedNormally(throwingJobId, "getJobStatus", executorThread));
  }

  private static final class Callers {
    static void right(UUID jobId) {
      StoreCallGate.Pause fired = StoreCallGate.enter("getJobStatus", jobId);
      boolean completed = false;
      try {
        completed = true;
      } finally {
        StoreCallGate.exit("getJobStatus", jobId, fired, completed);
      }
    }

    static void wrong(UUID jobId) {
      StoreCallGate.Pause fired = StoreCallGate.enter("getJobStatus", jobId);
      boolean completed = false;
      try {
        completed = true;
      } finally {
        StoreCallGate.exit("getJobStatus", jobId, fired, completed);
      }
    }

    static void throwing(UUID jobId) {
      StoreCallGate.Pause fired = StoreCallGate.enter("getJobStatus", jobId);
      boolean completed = false;
      try {
        throw new IllegalStateException("Store call failed");
      } finally {
        StoreCallGate.exit("getJobStatus", jobId, fired, completed);
      }
    }
  }

  private static final class Outer {
    static void right(UUID jobId) {
      Callers.wrong(jobId);
    }
  }
}
