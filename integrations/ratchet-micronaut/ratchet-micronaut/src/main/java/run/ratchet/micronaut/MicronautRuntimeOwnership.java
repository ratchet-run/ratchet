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

import io.micronaut.context.BeanContext;
import java.lang.ref.WeakReference;
import java.util.function.Supplier;
import run.ratchet.store.converter.RuntimeContextInstallation;

final class MicronautRuntimeOwnership {
  private static Owner owner;

  private MicronautRuntimeOwnership() {}

  static synchronized RuntimeContextInstallation install(
      BeanContext context, Supplier<RuntimeContextInstallation> factory) {
    // Micronaut never destroys beans of a context whose start failed.
    if (owner != null && owner.isAbandoned()) owner.installation().close();
    RuntimeContextInstallation installation = factory.get();
    owner =
        new Owner(
            installation,
            new WeakReference<>(context),
            new WeakReference<>(Thread.currentThread()));
    return installation;
  }

  private record Owner(
      RuntimeContextInstallation installation,
      WeakReference<BeanContext> context,
      WeakReference<Thread> creatingThread) {
    boolean isAbandoned() {
      BeanContext owningContext = context.get();
      Thread thread = creatingThread.get();
      return (owningContext == null || !owningContext.isRunning())
          && (thread == Thread.currentThread() || thread == null || !thread.isAlive());
    }
  }
}
