module run.ratchet.tck.store {
  requires run.ratchet.tck.util;
  requires run.ratchet.api;
  requires run.ratchet.store.core;
  requires jakarta.persistence;
  requires org.junit.jupiter.api;

  exports run.ratchet.tck.store;
}
