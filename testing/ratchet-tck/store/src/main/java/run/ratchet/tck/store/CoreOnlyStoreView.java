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
package run.ratchet.tck.store;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Optional;
import run.ratchet.store.spi.JobStore;

/**
 * Wraps a fully capable {@link JobStore} so it advertises only the mandatory core contract.
 *
 * <p>The returned view implements {@code JobStore} (and therefore its core sub-interfaces) and
 * nothing else, so {@link JobStore#capability} reports every optional capability as absent. Core
 * lifecycle calls — CRUD, claiming, terminal transitions, node heartbeat, and crash-recovery resets
 * — delegate to the backing store and behave normally.
 *
 * <p>It exists to prove that an engine and its conformance suite cope with a store that implements
 * core lifecycle but no optional capability: capability contracts skip, and crash recovery (which
 * rides on the mandatory {@code NodeStore} surface) keeps working.
 */
public final class CoreOnlyStoreView {

  private CoreOnlyStoreView() {}

  /** Returns a core-only view of {@code full} that advertises no optional capability. */
  public static JobStore of(JobStore full) {
    return (JobStore)
        Proxy.newProxyInstance(
            JobStore.class.getClassLoader(),
            new Class<?>[] {JobStore.class},
            (proxy, method, args) -> {
              if ("capability".equals(method.getName()) && args != null && args.length == 1) {
                Class<?> type = (Class<?>) args[0];
                // The proxy implements JobStore alone. Core sub-interfaces of JobStore are part of
                // its type closure and resolve; optional capability interfaces are not, so they
                // report absent even though the backing store implements them.
                return type.isInstance(proxy) ? Optional.of(type.cast(proxy)) : Optional.empty();
              }
              try {
                return method.invoke(full, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }
}
