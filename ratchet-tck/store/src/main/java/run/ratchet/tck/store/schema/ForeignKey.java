package run.ratchet.tck.store.schema;

/**
 * Foreign-key constraint with action semantics. Action is part of the contract because cascade
 * behavior is correctness-critical (e.g. hot-row deletion on parent-job delete depends on {@link
 * OnDeleteAction#CASCADE}).
 */
public record ForeignKey(
    String name, String column, String refTable, String refColumn, OnDeleteAction onDelete) {}
