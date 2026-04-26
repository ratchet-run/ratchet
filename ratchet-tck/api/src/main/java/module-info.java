module run.ratchet.tck.api {
  requires transitive run.ratchet.api;
  requires run.ratchet.tck.util;
  requires org.junit.jupiter.api;

  exports run.ratchet.tck.api;
}
