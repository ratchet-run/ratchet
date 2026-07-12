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
package run.ratchet.store.sqlserver;

import run.ratchet.tck.store.AbstractCoreOnlyStoreContract;
import run.ratchet.tck.store.JobStoreContractFixture;

/**
 * Proves a store that advertises only the mandatory core contract still satisfies the engine's
 * correctness floor. The backing store is a real SQL Server implementation viewed through {@link
 * run.ratchet.tck.store.CoreOnlyStoreView}, so core lifecycle calls run for real while every
 * optional capability reports absent.
 */
class SqlserverCoreOnlyStoreTest extends AbstractCoreOnlyStoreContract {

  private final SqlserverCoreOnlyTestFixture fixture = new SqlserverCoreOnlyTestFixture();

  @Override
  protected JobStoreContractFixture fixture() {
    return fixture;
  }
}
