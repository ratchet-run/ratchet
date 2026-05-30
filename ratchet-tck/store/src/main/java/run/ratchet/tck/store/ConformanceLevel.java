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
      "Fundamental persistence and locking primitives required by all conforming store"
          + " implementations.",
      List.of(
          "AbstractJobCrudStoreContract",
          "AbstractJobClaimStoreContract",
          "AbstractLockStoreContract",
          "AbstractNodeStoreContract",
          "AbstractTagStoreContract"),
      List.of()),

  BEHAVIORAL(
      "Behavioral",
      "Job lifecycle operations: execution tracking, retry, pause/resume, recurring scheduling, and"
          + " batch orchestration.",
      List.of(
          "AbstractExecutionStoreContract",
          "AbstractJobLogStoreContract",
          "AbstractJobRetryStoreContract",
          "AbstractJobPauseStoreContract",
          "AbstractJobTerminalStoreContract",
          "AbstractRecurringJobStoreContract",
          "AbstractBatchStoreContract",
          "AbstractWorkflowConditionStoreContract",
          "AbstractJobBatchStatusStoreContract",
          "AbstractBatchMetricsStoreContract",
          "AbstractJobQueryStoreContract"),
      List.of()),

  ADVANCED(
      "Advanced",
      "Optional capabilities: archival, dead-letter queues, bulk operations, business-key"
          + " uniqueness, and — for SQL stores — schema conformance, schema migration, and JTA"
          + " transaction boundaries.",
      List.of(
          "AbstractArchiveStoreContract",
          "AbstractDlqAlertStoreContract",
          "AbstractJobBulkStoreContract",
          "AbstractActiveBusinessKeyContract",
          "AbstractDualWriteInvariantContract",
          "AbstractResourcePermitStoreContract"),
      List.of(
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
