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
package run.ratchet.store.oracle;

import jakarta.persistence.EntityManager;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.ConstraintDetector;
import run.ratchet.store.context.AbstractSqlStoreContext;

final class OracleStoreContext extends AbstractSqlStoreContext {

  private final OracleConstraintDetector constraintDetector = new OracleConstraintDetector();

  OracleStoreContext(EntityManager em, MetricsCollector metricsCollector) {
    this(em, metricsCollector, 15);
  }

  OracleStoreContext(
      EntityManager em, MetricsCollector metricsCollector, int priorityBoostIntervalMinutes) {
    super(em, metricsCollector, priorityBoostIntervalMinutes);
  }

  @Override
  protected String dialectMetric() {
    return "oracle";
  }

  @Override
  protected String dialectLabel() {
    return "Oracle";
  }

  @Override
  public ConstraintDetector constraintDetector() {
    return constraintDetector;
  }
}
