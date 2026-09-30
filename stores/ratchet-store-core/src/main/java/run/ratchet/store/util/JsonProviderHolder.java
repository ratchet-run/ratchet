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
package run.ratchet.store.util;

import jakarta.json.spi.JsonProvider;

/**
 * Process-wide JSON-P provider for store row mapping.
 *
 * <p>The static {@code jakarta.json.Json} factory methods call {@link JsonProvider#provider()},
 * which runs a fresh {@link java.util.ServiceLoader} scan on every call. Row hydration parses every
 * job payload, so that scan ran once per job load and read the classpath each time. In a Spring
 * Boot fat jar the scan reads through the nested-jar loader's locks; on a job virtual thread it
 * could unmount while holding one of them, and virtual threads pinned by class loading then filled
 * every carrier, deadlocking the scheduler. Resolving the provider once removes that per-row scan.
 *
 * <p>The provider is resolved lazily on first use through a holder class, so nothing is looked up
 * at build time (native image) or when the store module is merely on the classpath.
 */
public final class JsonProviderHolder {

  private JsonProviderHolder() {}

  /** Returns the JSON-P provider, resolving it on the first call only. */
  public static JsonProvider provider() {
    return Holder.PROVIDER;
  }

  private static final class Holder {
    static final JsonProvider PROVIDER = JsonProvider.provider();
  }
}
