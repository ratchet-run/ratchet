package run.ratchet.tck.store.schema;

/** Foreign-key cascade behavior on parent-row delete. */
public enum OnDeleteAction {
  NO_ACTION,
  RESTRICT,
  CASCADE,
  SET_NULL,
  SET_DEFAULT
}
