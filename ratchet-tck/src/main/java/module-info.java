module run.ratchet.tck {
  requires run.ratchet.api;
  requires run.ratchet.store.core;
  requires jakarta.persistence;
  requires org.junit.jupiter.api;

  exports run.ratchet.tck.store;
  exports run.ratchet.tck.util;
}
