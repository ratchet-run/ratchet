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
package run.ratchet.tck.api;

import java.nio.file.Path;
import java.util.List;
import run.ratchet.tck.util.AbstractConformanceReportExtension;

/**
 * API-tier conformance report listener. Writes {@code target/tck-api-conformance-report.md} after
 * any test run that exercises at least one {@code ratchet-tck-api} contract class.
 *
 * <p>Runtimes inject identity via Failsafe's {@code systemPropertyVariables}: {@code
 * ratchet.tck.runtime.name=${testsuite.profile}/${ratchet.test.db.type}}.
 *
 * @apiNote <b>Internal.</b> This is a JUnit Platform {@code TestExecutionListener} registered with
 *     the TCK harness via {@code META-INF/services} and the module-info {@code provides} clause; it
 *     must remain a public top-level class so the platform can instantiate it, but it is NOT a
 *     supported extension point. Implementors reference it only by name in their runtime profile
 *     and MUST NOT subclass it — the {@code GROUPS} catalog and report layout are TCK
 *     implementation details that may change between releases.
 */
public class ApiConformanceReportExtension extends AbstractConformanceReportExtension {

  private static final List<ContractGroup> GROUPS =
      List.of(
          new ContractGroup(
              "API Behavioral",
              "Pure-JVM behavioral contracts for the JobSchedulerService public API.",
              List.of(
                  "AbstractJobLifecycleContract",
                  "AbstractJobRetryContract",
                  "AbstractBatchRetryContract",
                  "AbstractJobCancelContract",
                  "AbstractDelayedSchedulingContract",
                  "AbstractBusinessKeyContract",
                  "AbstractIdempotencyContract",
                  "AbstractSimpleWorkflowContract",
                  "AbstractExclusiveWorkflowContract",
                  "AbstractResilienceStrategyContract",
                  "AbstractJobAuthorizationContract",
                  "AbstractSignalDecisionContract",
                  "AbstractSignalPayloadContract",
                  "AbstractPayloadSizeContract",
                  "AbstractBroadcastSignalContract",
                  "AbstractBulkCancelEventContract",
                  "AbstractJobControlReturnContract",
                  "AbstractRetryPolicyContract",
                  "AbstractJobQueryContract",
                  "AbstractJobQueryDenialContract")),
          new ContractGroup(
              "Encryption SPI",
              "Conformance contract every PayloadEncryption engine must satisfy: AEAD round-trip,"
                  + " tamper/mismatched-AAD/wrong-key rejection, and nonce uniqueness.",
              List.of("AbstractPayloadEncryptionEngineContract")));

  @Override
  protected String tierTitle() {
    return "API";
  }

  @Override
  protected String runtimeProperty() {
    return "ratchet.tck.runtime.name";
  }

  @Override
  protected Path reportPath() {
    return Path.of("target", "tck-api-conformance-report.md");
  }

  @Override
  protected List<ContractGroup> contractGroups() {
    return GROUPS;
  }
}
