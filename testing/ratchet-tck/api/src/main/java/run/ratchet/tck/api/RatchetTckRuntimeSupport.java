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
package run.ratchet.tck.api;

import java.time.Duration;

/** Shared reset sequence for TCK runtimes backed by the Ratchet executor. */
public final class RatchetTckRuntimeSupport {

  private static final Duration CLEAR_DRAIN_TIMEOUT = Duration.ofSeconds(30);

  private RatchetTckRuntimeSupport() {}

  public static void clearRuntime(
      String runtimeName,
      DrainControl drainControl,
      IdleAwaiter idleAwaiter,
      Runnable resetStore,
      Runnable resetProbe) {
    drainControl.setDraining(true);
    try {
      boolean idle = idleAwaiter.awaitIdle(CLEAR_DRAIN_TIMEOUT);
      if (!idle) {
        throw new IllegalStateException(
            runtimeName
                + ".clear(): executor did not become idle within "
                + CLEAR_DRAIN_TIMEOUT
                + " — implementation drain is buggy or a worker is stuck");
      }
      resetStore.run();
      resetProbe.run();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("clear() interrupted", e);
    } finally {
      drainControl.setDraining(false);
    }
  }

  @FunctionalInterface
  public interface DrainControl {
    void setDraining(boolean draining);
  }

  @FunctionalInterface
  public interface IdleAwaiter {
    boolean awaitIdle(Duration timeout) throws InterruptedException;
  }
}
