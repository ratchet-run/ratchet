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
package run.ratchet.tck.jakarta;

import java.nio.file.Path;
import java.util.List;
import run.ratchet.tck.util.AbstractConformanceReportExtension;

/**
 * Jakarta-Runtime-tier conformance report listener. Writes {@code
 * target/tck-jakarta-conformance-report.md} after any test run that exercises at least one {@code
 * ratchet-tck-jakarta} contract class.
 *
 * <p>Runtimes inject identity via Failsafe's {@code systemPropertyVariables}: {@code
 * ratchet.tck.runtime.name=${testsuite.profile}/${ratchet.test.db.type}}.
 */
public class JakartaConformanceReportExtension extends AbstractConformanceReportExtension {

  private static final List<ContractGroup> GROUPS =
      List.of(
          new ContractGroup(
              "Jakarta Runtime",
              "CDI injection, CDI event, and JTA transaction contracts verified in a live"
                  + " Jakarta EE container.",
              List.of(
                  "AbstractCdiEventContract",
                  "AbstractCdiInjectionContract",
                  "AbstractTxDlqOrderContract",
                  "AbstractTxEnqueueContract",
                  "AbstractTxNotSupportedContract",
                  "AbstractTxRequiredContract",
                  "AbstractTxSupportsContract"),
              List.of("AbstractTxRequiresNewContract")));

  @Override
  protected String tierTitle() {
    return "Jakarta Runtime";
  }

  @Override
  protected String runtimeProperty() {
    return "ratchet.tck.runtime.name";
  }

  @Override
  protected Path reportPath() {
    return Path.of("target", "tck-jakarta-conformance-report.md");
  }

  @Override
  protected List<ContractGroup> contractGroups() {
    return GROUPS;
  }
}
