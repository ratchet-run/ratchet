package run.ratchet.tck.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Groups store-TCK contracts into three categories and serves as the static inventory that {@link
 * ConformanceReportExtension} checks against — enabling detection of missing contracts, not just
 * failing ones.
 */
public enum ConformanceLevel {
  CORE(
      "Core",
      "Fundamental persistence and locking primitives required by all conforming store"
          + " implementations.",
      List.of(
          "AbstractJobCrudStoreContract",
          "AbstractJobClaimStoreContract",
          "AbstractLockStoreContract",
          "AbstractNodeStoreContract",
          "AbstractTagStoreContract")),

  BEHAVIORAL(
      "Behavioral",
      "Job lifecycle operations: execution tracking, retry, pause/resume, and batch"
          + " orchestration.",
      List.of(
          "AbstractExecutionStoreContract",
          "AbstractJobRetryStoreContract",
          "AbstractJobPauseStoreContract",
          "AbstractJobTerminalStoreContract",
          "AbstractBatchStoreContract",
          "AbstractWorkflowConditionStoreContract",
          "AbstractJobBatchStatusStoreContract",
          "AbstractBatchMetricsStoreContract",
          "AbstractJobQueryStoreContract")),

  ADVANCED(
      "Advanced",
      "Optional capabilities: archival, dead-letter queues, bulk operations, schema"
          + " conformance, and business-key uniqueness.",
      List.of(
          "AbstractArchiveStoreContract",
          "AbstractDlqAlertStoreContract",
          "AbstractJobBulkStoreContract",
          "AbstractActiveBusinessKeyContract",
          "AbstractSchemaConformanceContract",
          "AbstractDualWriteInvariantContract",
          "AbstractResourcePermitStoreContract"));

  private static final Map<String, ConformanceLevel> BY_CONTRACT = buildIndex();

  private final String label;
  private final String description;
  private final List<String> requiredContracts;

  ConformanceLevel(String label, String description, List<String> requiredContracts) {
    this.label = label;
    this.description = description;
    this.requiredContracts = requiredContracts;
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

  /** Returns the level that owns {@code simpleClassName}, or {@code null} if unrecognized. */
  public static ConformanceLevel forContract(String simpleClassName) {
    return BY_CONTRACT.get(simpleClassName);
  }

  private static Map<String, ConformanceLevel> buildIndex() {
    Map<String, ConformanceLevel> index = new HashMap<>();
    for (ConformanceLevel level : values()) {
      for (String name : level.requiredContracts) {
        index.put(name, level);
      }
    }
    return index;
  }
}
