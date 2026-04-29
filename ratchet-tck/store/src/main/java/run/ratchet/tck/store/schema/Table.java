package run.ratchet.tck.store.schema;

import java.util.List;

/**
 * Canonical table declaration. Lists are unmodifiable snapshots; use {@link #builder(String)} for
 * fluent construction in the catalog.
 */
public record Table(
    String name,
    List<Column> columns,
    List<String> primaryKey,
    List<ForeignKey> foreignKeys,
    List<Index> indexes) {

  public Table {
    columns = List.copyOf(columns);
    primaryKey = List.copyOf(primaryKey);
    foreignKeys = List.copyOf(foreignKeys);
    indexes = List.copyOf(indexes);
  }

  public static Builder builder(String name) {
    return new Builder(name);
  }

  public static final class Builder {
    private final String name;
    private final java.util.List<Column> columns = new java.util.ArrayList<>();
    private final java.util.List<String> primaryKey = new java.util.ArrayList<>();
    private final java.util.List<ForeignKey> foreignKeys = new java.util.ArrayList<>();
    private final java.util.List<Index> indexes = new java.util.ArrayList<>();

    private Builder(String name) {
      this.name = name;
    }

    public Builder column(Column column) {
      columns.add(column);
      return this;
    }

    public Builder primaryKey(String... columns) {
      primaryKey.clear();
      for (String c : columns) {
        primaryKey.add(c);
      }
      return this;
    }

    public Builder foreignKey(ForeignKey fk) {
      foreignKeys.add(fk);
      return this;
    }

    public Builder index(Index index) {
      indexes.add(index);
      return this;
    }

    public Table build() {
      return new Table(name, columns, primaryKey, foreignKeys, indexes);
    }
  }
}
