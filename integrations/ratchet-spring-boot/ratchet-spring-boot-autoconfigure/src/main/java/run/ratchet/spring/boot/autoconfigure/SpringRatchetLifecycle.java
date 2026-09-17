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

import org.springframework.context.SmartLifecycle;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.store.converter.RuntimeContextInstallation;

/**
 * Starts after singleton initialization and stops while application persistence is still usable.
 */
public final class SpringRatchetLifecycle implements SmartLifecycle {
  private final RatchetRuntime runtime;
  private final RuntimeContextInstallation installation;
  private volatile boolean running;

  public SpringRatchetLifecycle(RatchetRuntime runtime, RuntimeContextInstallation installation) {
    this.runtime = runtime;
    this.installation = installation;
  }

  @Override
  public synchronized void start() {
    if (running) return;
    try {
      runtime.start();
      running = true;
    } catch (RuntimeException | Error failure) {
      runtime.close();
      installation.close();
      throw failure;
    }
  }

  @Override
  public synchronized void stop() {
    try {
      runtime.close();
    } finally {
      running = false;
    }
  }

  @Override
  public void stop(Runnable callback) {
    try {
      stop();
    } finally {
      callback.run();
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    return Integer.MAX_VALUE - 1024;
  }
}
