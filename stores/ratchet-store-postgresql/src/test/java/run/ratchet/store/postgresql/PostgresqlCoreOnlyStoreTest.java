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

import run.ratchet.tck.store.AbstractCoreOnlyStoreContract;
import run.ratchet.tck.store.CoreOnlyStoreView;
import run.ratchet.tck.store.JobStoreContractFixture;

/**
 * Proves a store that advertises only the mandatory core contract still satisfies the engine's
 * required lifecycle. The fixture wraps the full PostgreSQL implementation in {@link
 * CoreOnlyStoreView}, so core lifecycle calls run for real while every optional capability reports
 * absent.
 */
class PostgresqlCoreOnlyStoreTest extends AbstractCoreOnlyStoreContract {

  private final PostgresqlCoreOnlyTestFixture fixture = new PostgresqlCoreOnlyTestFixture();

  @Override
  protected JobStoreContractFixture fixture() {
    return fixture;
  }
}
