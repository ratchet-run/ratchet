package run.ratchet.tck.store.schema;

import java.util.List;

/**
 * Canonical Ratchet schema, expressed as logical declarations. Tables list required tables (extras
 * are tolerated — the contract verifies presence and conformance, not exclusivity). Deprecated
 * artifacts list what must be absent for the bidirectional check.
 */
public record SchemaSpec(int version, List<Table> tables, List<DeprecatedArtifact> deprecated) {

  public SchemaSpec {
    tables = List.copyOf(tables);
    deprecated = List.copyOf(deprecated);
  }
}
