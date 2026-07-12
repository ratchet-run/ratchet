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
package run.ratchet.tck.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Groups store-TCK contracts into three categories and serves as the static inventory that {@link
 * ConformanceReportExtension} checks against — enabling detection of missing contracts, not just
 * failing ones.
 *
 * <p>Each level separates <em>required</em> contracts, which every conforming store must run and
 * pass, from <em>optional</em> contracts that apply only to stores supporting the capability. A
 * document store has no schema-migration step and is not JTA-managed, so the schema-migrator,
 * JPA-recurring-claim-concurrency, schema-conformance, and transaction-boundary contracts are
 * optional: a store that runs one is held to it, and a store that does not is reported {@code N/A}
 * rather than {@code MISSING}.
 */
public enum ConformanceLevel {
  CORE(
      "Core",
      "Fundamental persistence primitives required by all conforming store implementations. The"
          + " core-only projection contract is optional validation that a full store can expose"
          + " only those mandatory capabilities.",
      List.of(
          "AbstractJobCrudStoreContract",
          "AbstractJobClaimStoreContract",
          "AbstractNodeStoreContract",
          "AbstractTagStoreContract",
          "AbstractPayloadEncryptionStoreContract"),
      List.of("AbstractCoreOnlyStoreContract")),

  BEHAVIORAL(
      "Behavioral",
      "Job lifecycle operations. The required contracts — terminal transitions, retry, pause/resume,"
          + " and non-terminal status CAS — are part of the mandatory core. The optional contracts"
          + " apply only to stores advertising the matching capability: audit, recurring scheduling,"
          + " batch orchestration, workflow conditions, query, analytics, and signal delivery.",
      List.of(
          "AbstractJobRetryStoreContract",
          "AbstractJobPauseStoreContract",
          "AbstractJobTerminalStoreContract",
          "AbstractJobBatchStatusStoreContract"),
      List.of(
          "AbstractJobAuditStoreContract",
          "AbstractRecurringJobStoreContract",
          "AbstractBatchStoreContract",
          "AbstractWorkflowConditionStoreContract",
          "AbstractJobQueryStoreContract",
          "AbstractJobAnalyticsStoreContract",
          "AbstractSignalStoreContract")),

  ADVANCED(
      "Advanced",
      "Bulk operations, business-key uniqueness, and the dual-write storage invariant are core. The"
          + " optional contracts apply only to stores that advertise the capability: archival,"
          + " distributed locks, resource permits, and — for SQL stores —"
          + " schema conformance, schema migration, JPA recurring-claim concurrency, and JTA"
          + " transaction boundaries.",
      List.of(
          "AbstractJobBulkStoreContract",
          "AbstractActiveBusinessKeyContract",
          "AbstractDualWriteInvariantContract"),
      List.of(
          "AbstractArchiveStoreContract",
          "AbstractJobExtensionStoreContract",
          "AbstractLockStoreContract",
          "AbstractResourcePermitStoreContract",
          "AbstractSchemaConformanceContract",
          "AbstractSchemaMigratorContract",
          "AbstractJpaRecurringClaimConcurrencyContract",
          "AbstractJobStoreTransactionBoundaryContract"));

  private static final Map<String, ConformanceLevel> BY_CONTRACT = buildIndex();

  private final String label;
  private final String description;
  private final List<String> requiredContracts;
  private final List<String> optionalContracts;

  ConformanceLevel(
      String label,
      String description,
      List<String> requiredContracts,
      List<String> optionalContracts) {
    this.label = label;
    this.description = description;
    this.requiredContracts = requiredContracts;
    this.optionalContracts = optionalContracts;
  }

  /**
   * Returns the level that owns {@code simpleClassName} (required or optional), or {@code null} if
   * unrecognized.
   */
  public static ConformanceLevel forContract(String simpleClassName) {
    return BY_CONTRACT.get(simpleClassName);
  }

  private static Map<String, ConformanceLevel> buildIndex() {
    Map<String, ConformanceLevel> index = new HashMap<>();
    for (ConformanceLevel level : values()) {
      for (String name : level.requiredContracts) {
        index.put(name, level);
      }
      for (String name : level.optionalContracts) {
        index.put(name, level);
      }
    }
    return index;
  }

  public String getLabel() {
    return label;
  }

  public String getDescription() {
    return description;
  }

  public List<String> getRequiredContracts() {
    return requiredContracts;
  }

  public List<String> getOptionalContracts() {
    return optionalContracts;
  }
}
