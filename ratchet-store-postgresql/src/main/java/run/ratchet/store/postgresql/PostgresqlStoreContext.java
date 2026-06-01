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
import run.ratchet.api.JobStatus;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.ConstraintDetector;
import run.ratchet.store.context.AbstractSqlStoreContext;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.util.StatusClassifier;

final class PostgresqlStoreContext extends AbstractSqlStoreContext {

  private final PostgresqlConstraintDetector constraintDetector =
      new PostgresqlConstraintDetector();

  PostgresqlStoreContext(EntityManager em) {
    this(em, noopMetricsCollector(), 15);
  }

  PostgresqlStoreContext(EntityManager em, int priorityBoostIntervalMinutes) {
    this(em, noopMetricsCollector(), priorityBoostIntervalMinutes);
  }

  PostgresqlStoreContext(
      EntityManager em, MetricsCollector metricsCollector, int priorityBoostIntervalMinutes) {
    super(em, metricsCollector, priorityBoostIntervalMinutes);
  }

  static boolean isPollerExecutable(JobExecutionType jobType) {
    return StatusClassifier.isPollerExecutable(jobType);
  }

  static boolean isLiveStatus(JobStatus status) {
    return StatusClassifier.isLiveStatus(status);
  }

  static boolean isTerminalStatus(JobStatus status) {
    return StatusClassifier.isTerminalStatus(status);
  }

  static JobStatus effectiveStatus(JobStatus status) {
    return StatusClassifier.effectiveStatus(status);
  }

  @Override
  protected String dialectMetric() {
    return "postgresql";
  }

  @Override
  protected String dialectLabel() {
    return "PostgreSQL";
  }

  @Override
  public ConstraintDetector constraintDetector() {
    return constraintDetector;
  }
}
