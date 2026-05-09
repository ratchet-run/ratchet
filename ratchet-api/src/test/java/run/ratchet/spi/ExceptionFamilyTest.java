package run.ratchet.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.SocketTimeoutException;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.RatchetTransientStoreException;

class ExceptionFamilyTest {

  @Test
  void classifyTimeoutFromCauseChain() {
    ExceptionFamily family =
        ExceptionFamily.classify(new RuntimeException("wrapper", new TimeoutException("slow")));

    assertEquals(ExceptionFamily.TIMEOUT, family);
  }

  @Test
  void classifyNullAsUnknown() {
    assertEquals(ExceptionFamily.UNKNOWN, ExceptionFamily.classify(null));
  }

  @Test
  void classifyAdditionalTimeoutTypes() throws Exception {
    assertEquals(ExceptionFamily.TIMEOUT, ExceptionFamily.classify(new SocketTimeoutException()));
    assertEquals(
        ExceptionFamily.TIMEOUT, ExceptionFamily.classify(new InterruptedByTimeoutException()));
    assertEquals(
        ExceptionFamily.TIMEOUT, ExceptionFamily.classify(newHttpTimeoutException("slow")));
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

  private static Throwable newHttpTimeoutException(String message) throws Exception {
    Class<?> type = Class.forName("java.net.http.HttpTimeoutException");
    return (Throwable) type.getConstructor(String.class).newInstance(message);
  }
}
