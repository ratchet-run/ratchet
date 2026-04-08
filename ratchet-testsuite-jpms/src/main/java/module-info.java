/**
 * JPMS consumer test for {@code ratchet-api}.
 *
 * <p>This module exists solely to assert — at compile time — that {@code ratchet-api}'s {@code
 * module-info.java} correctly exports the public API/SPI packages and that internal implementation
 * packages remain inaccessible from module-path consumers.
 *
 * <p>The single source file {@code test.jpms.consumer.JpmsConsumerProbe} imports types from the
 * exported {@code run.ratchet.api}, {@code run.ratchet.api.event}, {@code
 * run.ratchet.api.exception}, and {@code run.ratchet.spi} packages. Compile
 * success of this module IS the verification.
 *
 * <p>For the negative test (proving internal types are NOT importable), see the commented-out
 * import in {@code JpmsConsumerProbe.java}. Uncommenting it should produce a compile error like
 * "package run.ratchet.ri.core is not visible".
 */
module ratchet.testsuite.jpms {
  requires run.ratchet.api;
}
