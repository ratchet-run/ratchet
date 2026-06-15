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
package run.ratchet.coordinator.infinispan.tck;

import run.ratchet.coordinator.infinispan.InfinispanCoordinatorTestHarness;
import run.ratchet.tck.coordinator.AbstractClusterCoordinatorOptionalContract;
import run.ratchet.tck.coordinator.CoordinatorTestHarness;

/**
 * Runs the {@link AbstractClusterCoordinatorOptionalContract} (pre-registration buffer) against two
 * embedded Infinispan cache managers.
 */
class InfinispanCoordinatorOptionalContractIT extends AbstractClusterCoordinatorOptionalContract {

  @Override
  protected CoordinatorTestHarness harness() {
    try {
      return new InfinispanCoordinatorTestHarness();
    } catch (Exception e) {
      throw new RuntimeException("could not bootstrap Infinispan harness", e);
    }
  }
}
