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
package run.ratchet.ri.cdi;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import run.ratchet.ri.core.JobExecutorService;
import run.ratchet.store.converter.RuntimeContextInstallation;

/** Shares one converter owner across the CDI installers without changing their startup phases. */
@ApplicationScoped
public class CdiRuntimeContextInstallation {
  private RuntimeContextInstallation installation;
  private Runnable stopRuntime;
  private boolean closed;

  public synchronized RuntimeContextInstallation installation() {
    if (closed) throw new IllegalStateException("CDI Ratchet runtime has already stopped");
    if (installation == null) installation = RuntimeContextInstallation.claim();
    return installation;
  }

  public synchronized void bindRuntime(Runnable stop, JobExecutorService executor) {
    RuntimeContextInstallation owner = installation();
    owner.releaseWhen(executor.onIdle(owner::releaseIfRequested));
    stopRuntime = stop;
  }

  void onShutdown(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
    close();
  }

  public synchronized void close() {
    if (closed) return;
    closed = true;
    try {
      if (stopRuntime != null) stopRuntime.run();
    } finally {
      if (installation != null) installation.close();
    }
  }
}
