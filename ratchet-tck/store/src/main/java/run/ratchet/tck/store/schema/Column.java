package run.ratchet.tck.store.schema;

/** Canonical column declaration. {@code partOfPrimaryKey} is set by the table builder, not here. */
public record Column(String name, LogicalType type, boolean nullable) {
  public static Column required(String name, LogicalType type) {
    return new Column(name, type, false);
  }

  public static Column nullable(String name, LogicalType type) {
    return new Column(name, type, true);
  }
}
