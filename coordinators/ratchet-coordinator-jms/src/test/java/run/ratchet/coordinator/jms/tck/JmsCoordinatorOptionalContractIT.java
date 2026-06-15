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
package run.ratchet.coordinator.jms.tck;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import run.ratchet.coordinator.jms.EmbeddedArtemisBroker;
import run.ratchet.coordinator.jms.JmsCoordinatorTestHarness;
import run.ratchet.tck.coordinator.AbstractClusterCoordinatorOptionalContract;
import run.ratchet.tck.coordinator.CoordinatorTestHarness;

/**
 * Runs the {@link AbstractClusterCoordinatorOptionalContract} (pre-registration buffer) against an
 * embedded Artemis broker.
 */
class JmsCoordinatorOptionalContractIT extends AbstractClusterCoordinatorOptionalContract {

  private static EmbeddedArtemisBroker broker;

  @BeforeAll
  static void start() throws Exception {
    broker = new EmbeddedArtemisBroker();
    broker.start();
  }

  @AfterAll
  static void stop() throws Exception {
    if (broker != null) {
      broker.stop();
    }
  }

  @Override
  protected CoordinatorTestHarness harness() {
    return new JmsCoordinatorTestHarness(broker);
  }
}
