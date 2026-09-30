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
package run.ratchet.coordinator.common.internal;

import jakarta.json.JsonException;
import jakarta.json.spi.JsonProvider;

/**
 * JSON-P provider probe and cached provider shared by coordinator transports.
 *
 * @apiNote Framework-internal. This class is consumed only by Ratchet's bundled coordinator
 *     transports; it is not part of the public coordinator SPI and may change without notice.
 */
public final class JsonProviders {

  private JsonProviders() {}

  private static volatile JsonProvider cached;

  /**
   * Returns the JSON-P provider, resolved once per class loader that loads this class. The static
   * {@code jakarta.json.Json} factories run a {@link java.util.ServiceLoader} scan on every call;
   * the codec encodes and decodes a wakeup envelope per notification, so it uses this cached
   * instance instead. A failed lookup is not cached, so a later call retries it.
   *
   * @throws JsonException if no JSON-P provider is available
   */
  public static JsonProvider provider() {
    JsonProvider provider = cached;
    if (provider == null) {
      synchronized (JsonProviders.class) {
        provider = cached;
        if (provider == null) {
          provider = JsonProvider.provider();
          cached = provider;
        }
      }
    }
    return provider;
  }

  public static void requireJsonProvider() {
    try {
      provider();
    } catch (JsonException ex) {
      throw new IllegalStateException(
          "No JSON-P provider (jakarta.json.spi.JsonProvider) found on the classpath. Add"
              + " org.eclipse.parsson:parsson (or another JSON-P 2.x implementation) at runtime"
              + " scope, or deploy into a Jakarta EE container that supplies one.",
          ex);
    }
  }
}
