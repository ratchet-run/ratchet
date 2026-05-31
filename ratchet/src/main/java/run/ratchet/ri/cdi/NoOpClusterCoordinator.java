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
package run.ratchet.ri.cdi;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.function.Consumer;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.JobWakeupHint;

/** Default no-op {@link ClusterCoordinator} for deployments that do not need cross-node wakeups. */
@ApplicationScoped
public class NoOpClusterCoordinator implements ClusterCoordinator {

  private static final Logger log = Logger.getLogger(NoOpClusterCoordinator.class);

  /**
   * One INFO line at startup so an operator can confirm cluster coordination is opt-out — without
   * this, a misconfigured deployment that should have had a coordinator silently uses NoOp and
   * loses cross-node wakeups with no visible signal.
   */
  @PostConstruct
  void announce() {
    log.info(
        "Ratchet cluster coordination: NoOp (no cross-node wakeups). Add a ratchet-coordinator-*"
            + " module to enable push-based wakeups.");
  }

  @Override
  public void notifyNewWork(JobPriority priority, NodeIdentity source, String executionTarget) {}

  @Override
  public void registerWakeupListener(Consumer<JobWakeupHint> listener) {}

  @Override
  public void close() {}
}
