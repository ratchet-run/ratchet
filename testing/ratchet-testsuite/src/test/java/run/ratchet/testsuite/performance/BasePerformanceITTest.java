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
package run.ratchet.testsuite.performance;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import run.ratchet.ri.core.internal.DefaultPollerScheduler;

class BasePerformanceITTest {

  @Test
  void pollerSchedulerStoppedReturnsFalseDuringPollCycle() throws Exception {
    DefaultPollerScheduler scheduler = new DefaultPollerScheduler(null, null);
    setField(scheduler, "cycleRunning", true);

    assertFalse(BasePerformanceIT.pollerSchedulerStopped(scheduler));
  }

  @Test
  void pollerSchedulerStoppedReturnsFalseForScheduledHandle() throws Exception {
    DefaultPollerScheduler scheduler = new DefaultPollerScheduler(null, null);
    setField(scheduler, "handle", new CompletableFuture<>());

    assertFalse(BasePerformanceIT.pollerSchedulerStopped(scheduler));
  }

  @Test
  void pollerSchedulerStoppedReturnsTrueWhenIdle() {
    DefaultPollerScheduler scheduler = new DefaultPollerScheduler(null, null);

    assertTrue(BasePerformanceIT.pollerSchedulerStopped(scheduler));
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
