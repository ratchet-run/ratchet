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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;
import run.ratchet.ri.runtime.RatchetRuntime;

/** Starts and drains the portable Ratchet runtime with the Spring application context. */
final class RatchetLifecycle implements SmartLifecycle {

  /*
   * Boot 3.5.16's servlet WebServerStartStopLifecycle uses Integer.MAX_VALUE - 2048. Ratchet starts
   * before the web server accepts traffic and therefore stops after it stops accepting traffic.
   */
  static final int PHASE = Integer.MAX_VALUE - 4096;

  private final ObjectProvider<RatchetRuntime> runtimeProvider;
  private final Duration drainTimeout;
  private final boolean deferAutoStart;
  private final AtomicBoolean running = new AtomicBoolean();
  private volatile RatchetRuntime runtime;

  RatchetLifecycle(
      ObjectProvider<RatchetRuntime> runtimeProvider,
      Duration drainTimeout,
      boolean deferAutoStart) {
    this.runtimeProvider = Objects.requireNonNull(runtimeProvider, "runtimeProvider");
    this.drainTimeout = Objects.requireNonNull(drainTimeout, "drainTimeout");
    this.deferAutoStart = deferAutoStart;
  }

  @Override
  public synchronized void start() {
    if (running.get()) {
      return;
    }
    RatchetRuntime resolved = runtimeProvider.getIfAvailable();
    if (resolved == null) {
      return;
    }
    resolved.start();
    runtime = resolved;
    running.set(true);
  }

  @Override
  public void stop() {
    RatchetRuntime stopping = takeRunningRuntime();
    if (stopping != null) {
      stopping.stop(drainTimeout);
    }
  }

  @Override
  public void stop(Runnable callback) {
    Objects.requireNonNull(callback, "callback");
    try {
      stop();
    } finally {
      callback.run();
    }
  }

  private synchronized RatchetRuntime takeRunningRuntime() {
    if (!running.compareAndSet(true, false)) {
      return null;
    }
    RatchetRuntime stopping = runtime;
    runtime = null;
    return stopping;
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  @Override
  public boolean isAutoStartup() {
    return !deferAutoStart;
  }

  @Override
  public int getPhase() {
    return PHASE;
  }
}
