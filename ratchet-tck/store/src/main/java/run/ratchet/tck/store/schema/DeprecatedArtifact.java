package run.ratchet.tck.store.schema;

/**
 * Schema artifacts that a migration explicitly removed. The conformance contract verifies these are
 * absent from any conforming store, catching upgrade paths that left obsolete columns or indexes
 * behind. {@code sinceVersion} is the schema version that performed the removal.
 */
public sealed interface DeprecatedArtifact {

  int sinceVersion();

  record DroppedColumn(String table, String column, int sinceVersion)
      implements DeprecatedArtifact {}

  record DroppedIndex(String table, String index, int sinceVersion) implements DeprecatedArtifact {}
}
