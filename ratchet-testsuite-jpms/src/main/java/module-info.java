/**
 * JPMS consumer test for Ratchet module descriptors.
 *
 * <p>This module exists solely to assert — at compile time — that {@code ratchet-api}'s {@code
 * module-info.java} correctly exports the public API/SPI packages for module-path consumers.
 *
 * <p>The single source file {@code test.jpms.consumer.JpmsConsumerProbe} imports types from the
 * exported {@code run.ratchet.api}, {@code run.ratchet.api.event}, {@code
 * run.ratchet.api.exception}, and {@code run.ratchet.spi} packages. Compile success of this module
 * IS the verification. The module also requires {@code run.ratchet.ri} to prove the RI resolves as
 * a named module without exporting its implementation packages.
 */
module ratchet.testsuite.jpms {
  requires run.ratchet.api;
  requires run.ratchet.ri;
}
