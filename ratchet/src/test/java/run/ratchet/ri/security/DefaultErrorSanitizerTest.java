package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DefaultErrorSanitizerTest {

  private final DefaultErrorSanitizer sanitizer = new DefaultErrorSanitizer();

  @Test
  void redactsJdbcUrlAtTopLevel() {
    String out =
        sanitizer.sanitize(
            new RuntimeException(
                "failed to connect to jdbc:mysql://admin:hunter2@db.example.com/prod"));
    assertFalse(out.contains("hunter2"));
    assertFalse(out.contains("admin"));
    assertTrue(out.contains("***REDACTED***"));
  }

  @Test
  void redactsJdbcUrlBuriedFourLevelsDeep() {
    // 4-level cause chain
    Throwable deep = new RuntimeException("driver: jdbc:postgresql://root:s3cret@10.0.0.1/app");
    Throwable l3 = new RuntimeException("SQL error", deep);
    Throwable l2 = new RuntimeException("persistence error", l3);
    Throwable l1 = new RuntimeException("EJB invocation failed", l2);

    String out = sanitizer.sanitize(l1);

    assertFalse(out.contains("s3cret"), "password must not leak from depth 4");
    assertFalse(out.contains("root"), "username must not leak from depth 4");
    assertTrue(out.contains("***REDACTED***"));
    assertTrue(out.contains("EJB invocation failed"));
    assertTrue(out.contains("persistence error"));
  }

  @Test
  void redactsCredentialKvInDeeperCause() {
    Throwable deep = new RuntimeException("authFailed password=hunter2 token=abc123");
    Throwable l1 = new RuntimeException("wrapper", deep);

    String out = sanitizer.sanitize(l1);

    assertFalse(out.contains("hunter2"));
    assertFalse(out.contains("abc123"));
    assertTrue(out.contains("password=***REDACTED***"));
    assertTrue(out.contains("token=***REDACTED***"));
  }

  @Test
  void redactsAdjacentPlainCredentialKeyValuesSeparately() {
    String out =
        sanitizer.sanitize(
            new RuntimeException("auth failed password=hunter2 token=abc123,host=db"));

    assertFalse(out.contains("hunter2"));
    assertFalse(out.contains("abc123"));
    assertTrue(out.contains("password=***REDACTED***"));
    assertTrue(out.contains("token=***REDACTED***"));
    assertTrue(out.contains(",host=db"));
  }

  @Test
  void handlesSelfCyclingCauseWithoutInfiniteLoop() {
    // A Throwable whose cause is itself. Real cycles occur rarely but are possible via custom
    // exception subclasses. The walker must terminate.
    class SelfCycling extends RuntimeException {
      SelfCycling(String msg) {
        super(msg);
      }
    }
    SelfCycling ex = new SelfCycling("cycle: jdbc:mysql://u:p@h/d");
    // initCause rejects self-cycles on standard Throwable, so we construct via two instances.
    SelfCycling a = new SelfCycling("a-level password=topsecret");
    SelfCycling b = new SelfCycling("b-level");
    // a -> b -> a (cycle between two distinct instances)
    a.initCause(b);
    try {
      b.initCause(a);
    } catch (IllegalStateException ignored) {
      // Throwable.initCause throws if cause already set; safe to ignore.
    }

    String out = sanitizer.sanitize(a);
    // Must terminate and still redact sensitive content from the top message.
    assertFalse(out.contains("topsecret"));
    assertTrue(out.contains("password=***REDACTED***"));
  }

  @Test
  void truncatesOutputAboveMaxLength() {
    StringBuilder big = new StringBuilder();
    for (int i = 0; i < 3000; i++) {
      big.append("x");
    }
    String out = sanitizer.sanitize(new RuntimeException(big.toString()));
    assertTrue(out.length() <= 2000);
    assertTrue(out.endsWith("..."));
  }

  @Test
  void redactsEmailsByDefault() {
    String out = sanitizer.sanitize(new RuntimeException("notify alice@example.com failed"));
    assertFalse(out.contains("alice@example.com"), "email must be redacted by default");
    assertTrue(out.contains("***REDACTED***"));
  }

  @Test
  void canDisableEmailRedactionThroughOptions() {
    DefaultErrorSanitizer sanitizer = new DefaultErrorSanitizer(false);
    String out = sanitizer.sanitize(new RuntimeException("notify alice@example.com failed"));
    assertTrue(out.contains("alice@example.com"));
  }

  @Test
  void redactsUrlParamCredentials() {
    // Hibernate / JDBC exceptions commonly echo the connection URL query string with credentials.
    String out =
        sanitizer.sanitize(
            new RuntimeException(
                "connection refused: host=db.internal?user=admin&password=hunter2&ssl=true"));
    assertFalse(out.contains("hunter2"));
    assertFalse(out.contains("admin"));
    assertTrue(out.contains("password=***REDACTED***"));
    assertTrue(out.contains("user=***REDACTED***"));
  }
}
