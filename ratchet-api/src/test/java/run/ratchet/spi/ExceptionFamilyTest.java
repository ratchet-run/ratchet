package run.ratchet.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import run.ratchet.api.exception.RatchetTransientStoreException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class ExceptionFamilyTest {

  @Test
  void classifyTimeoutFromCauseChain() {
    ExceptionFamily family =
        ExceptionFamily.classify(new RuntimeException("wrapper", new TimeoutException("slow")));

    assertEquals(ExceptionFamily.TIMEOUT, family);
  }

  @Test
  void classifyTransientFromCauseChain() {
    ExceptionFamily family =
        ExceptionFamily.classify(
            new RuntimeException("wrapper", new RatchetTransientStoreException("deadlock")));

    assertEquals(ExceptionFamily.TRANSIENT, family);
  }

  @Test
  void classifyValidationForIllegalArgumentException() {
    assertEquals(
        ExceptionFamily.VALIDATION,
        ExceptionFamily.classify(new IllegalArgumentException("bad input")));
  }

  @Test
  void classifyBusinessForCustomRuntimeException() {
    assertEquals(
        ExceptionFamily.BUSINESS,
        ExceptionFamily.classify(new OrderRejectedException("customer rule")));
  }

  @Test
  void classifyUnknownForJdkRuntimeException() {
    assertEquals(ExceptionFamily.UNKNOWN, ExceptionFamily.classify(new NullPointerException()));
  }

  private static final class OrderRejectedException extends RuntimeException {

    private OrderRejectedException(String message) {
      super(message);
    }
  }
}
