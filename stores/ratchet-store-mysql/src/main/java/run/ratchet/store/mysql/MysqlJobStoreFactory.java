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
package run.ratchet.store.mysql;

import java.util.Objects;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.spi.RatchetEntityManagerProvider;

/** Creates ready-to-use MySQL job stores without requiring CDI. */
public final class MysqlJobStoreFactory {

  private MysqlJobStoreFactory() {}

  /**
   * Creates and fully initializes a MySQL job store.
   *
   * @param entityManagerProvider provider for the store's entity manager
   * @param metricsCollector collector for store metrics
   * @param options Ratchet runtime options
   * @return an initialized MySQL job store
   */
  public static MysqlJobStore create(
      RatchetEntityManagerProvider entityManagerProvider,
      MetricsCollector metricsCollector,
      RatchetOptions options) {
    Objects.requireNonNull(entityManagerProvider, "entityManagerProvider");
    Objects.requireNonNull(metricsCollector, "metricsCollector");
    Objects.requireNonNull(options, "options");

    MysqlJobStoreImpl store =
        new MysqlJobStoreImpl(entityManagerProvider, metricsCollector, options);
    store.initialize();
    return store;
  }
}
