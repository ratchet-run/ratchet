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
package run.ratchet.micronaut;

import io.micronaut.context.event.ApplicationEvent;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.ShutdownEvent;
import io.micronaut.context.event.StartupEvent;
import jakarta.annotation.PreDestroy;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.store.converter.RuntimeContextInstallation;

/**
 * Starts after context initialization. ShutdownEvent is synchronous and precedes singleton
 * destruction, so drain and node release finish while datasources are still usable.
 */
public final class MicronautRatchetLifecycle implements ApplicationEventListener<ApplicationEvent> {
  private final RatchetRuntime runtime;
  private final RuntimeContextInstallation installation;
  private boolean running;
  private boolean stopped;

  public MicronautRatchetLifecycle(
      RatchetRuntime runtime, RuntimeContextInstallation installation) {
    this.runtime = runtime;
    this.installation = installation;
  }

  @Override
  public void onApplicationEvent(ApplicationEvent event) {
    if (event instanceof StartupEvent) start();
    else if (event instanceof ShutdownEvent) stop();
  }

  @Override
  public boolean supports(ApplicationEvent event) {
    return event instanceof StartupEvent || event instanceof ShutdownEvent;
  }

  public synchronized void start() {
    if (running || stopped) return;
    try {
      runtime.start();
      running = true;
    } catch (RuntimeException | Error failure) {
      try {
        stop();
      } catch (RuntimeException | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      try {
        installation.close();
      } catch (RuntimeException | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  // ShutdownEvent is published synchronously, before datasources are destroyed; the drain relies
  // on that ordering.
  @PreDestroy
  public synchronized void stop() {
    if (stopped) return;
    stopped = true;
    try {
      runtime.close();
    } finally {
      running = false;
    }
  }

  public synchronized boolean isRunning() {
    return running;
  }
}
