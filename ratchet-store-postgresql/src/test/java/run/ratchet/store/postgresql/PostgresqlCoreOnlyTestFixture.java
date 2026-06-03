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
package run.ratchet.store.postgresql;

import jakarta.persistence.EntityManager;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.CoreOnlyStoreView;

/**
 * PostgreSQL fixture whose store advertises only the mandatory core contract. The real PostgreSQL
 * store implements every capability; this fixture hides them behind {@link CoreOnlyStoreView} so
 * the conformance suite sees a store that supports core lifecycle and crash recovery but no
 * optional capability.
 */
public class PostgresqlCoreOnlyTestFixture extends PostgresqlTestFixture {

  @Override
  protected JobStore createStore(EntityManager em, MetricsCollector metrics) {
    return CoreOnlyStoreView.of(super.createStore(em, metrics));
  }
}
