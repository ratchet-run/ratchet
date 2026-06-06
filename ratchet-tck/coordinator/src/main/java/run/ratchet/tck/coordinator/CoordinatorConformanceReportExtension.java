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
package run.ratchet.tck.coordinator;

import java.nio.file.Path;
import java.util.List;
import run.ratchet.tck.util.AbstractConformanceReportExtension;

/**
 * Coordinator-tier conformance report listener. Writes {@code
 * target/tck-coordinator-conformance-report.md} after any test run that exercises at least one
 * {@code ratchet-tck-coordinator} contract class.
 *
 * <p>Coordinator modules inject identity via Surefire's {@code systemPropertyVariables}: {@code
 * ratchet.tck.coordinator.name=${project.artifactId}}.
 */
public class CoordinatorConformanceReportExtension extends AbstractConformanceReportExtension {

  private static final List<ContractGroup> GROUPS =
      List.of(
          new ContractGroup(
              "Cluster Coordinator",
              "Cross-implementation ClusterCoordinator SPI contracts: envelope round-trip,"
                  + " self-suppression, listener isolation, transport-failure tolerance, and the"
                  + " metrics surface. The pre-registration buffer is an implementation choice, so"
                  + " its contract is optional.",
              List.of("AbstractClusterCoordinatorContract"),
              List.of("AbstractClusterCoordinatorOptionalContract")));

  @Override
  protected String tierTitle() {
    return "Coordinator";
  }

  @Override
  protected String runtimeProperty() {
    return "ratchet.tck.coordinator.name";
  }

  @Override
  protected Path reportPath() {
    return Path.of("target", "tck-coordinator-conformance-report.md");
  }

  @Override
  protected List<ContractGroup> contractGroups() {
    return GROUPS;
  }
}
