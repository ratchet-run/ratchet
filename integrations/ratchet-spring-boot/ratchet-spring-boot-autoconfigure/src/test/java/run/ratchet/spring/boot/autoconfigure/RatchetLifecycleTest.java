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
package run.ratchet.spring.boot.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import run.ratchet.ri.runtime.RatchetRuntime;

class RatchetLifecycleTest {

  @Test
  void startsAndStopsRuntimeWithConfiguredDrainTimeoutIdempotently() {
    RecordingRuntime runtime = new RecordingRuntime();
    Duration drainTimeout = Duration.ofSeconds(23);
    RatchetLifecycle lifecycle = lifecycle(runtime, drainTimeout);
    AtomicInteger callbacks = new AtomicInteger();

    lifecycle.start();
    lifecycle.start();

    assertTrue(lifecycle.isRunning());
    assertTrue(lifecycle.isAutoStartup());
    assertEquals(RatchetLifecycle.PHASE, lifecycle.getPhase());
    assertEquals(Integer.MAX_VALUE - 4096, lifecycle.getPhase());
    assertEquals(1, runtime.startCalls);

    lifecycle.stop(callbacks::incrementAndGet);
    lifecycle.stop(callbacks::incrementAndGet);

    assertFalse(lifecycle.isRunning());
    assertEquals(1, runtime.boundedStopCalls);
    assertEquals(drainTimeout, runtime.drainTimeout);
    assertEquals(2, callbacks.get());
  }

  @Test
  void deferredAutoStartStillAllowsManualStart() {
    RecordingRuntime runtime = new RecordingRuntime();
    RatchetLifecycle lifecycle = lifecycle(runtime, Duration.ofSeconds(30), true);

    assertFalse(lifecycle.isAutoStartup());
    assertFalse(lifecycle.isRunning());
    assertEquals(0, runtime.startCalls);

    lifecycle.start();

    assertTrue(lifecycle.isRunning());
    assertEquals(1, runtime.startCalls);
  }

  @Test
  void missingRuntimeLeavesLifecycleStopped() {
    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    RatchetLifecycle lifecycle =
        new RatchetLifecycle(
            beanFactory.getBeanProvider(RatchetRuntime.class), Duration.ofSeconds(30), false);

    lifecycle.start();

    assertFalse(lifecycle.isRunning());
  }

  private static RatchetLifecycle lifecycle(RecordingRuntime runtime, Duration drainTimeout) {
    return lifecycle(runtime, drainTimeout, false);
  }

  private static RatchetLifecycle lifecycle(
      RecordingRuntime runtime, Duration drainTimeout, boolean deferAutoStart) {
    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.registerSingleton("ratchetRuntime", runtime);
    return new RatchetLifecycle(
        beanFactory.getBeanProvider(RatchetRuntime.class), drainTimeout, deferAutoStart);
  }

  private static final class RecordingRuntime implements RatchetRuntime {

    private int startCalls;
    private int boundedStopCalls;
    private Duration drainTimeout;

    @Override
    public void start() {
      startCalls++;
    }

    @Override
    public void stop() {}

    @Override
    public void stop(Duration timeout) {
      boundedStopCalls++;
      drainTimeout = timeout;
    }
  }
}
