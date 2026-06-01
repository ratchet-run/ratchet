/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
  void classifyPreservesFamilyPriorityAcrossCauseChain() {
    ExceptionFamily family =
        ExceptionFamily.classify(
            new IllegalArgumentException("bad input", new RatchetTransientStoreException("retry")));

    assertEquals(ExceptionFamily.TRANSIENT, family);
  }

  @Test
  void classifyValidationForIllegalArgumentException() {
    assertEquals(
        ExceptionFamily.VALIDATION,
        ExceptionFamily.classify(new IllegalArgumentException("bad input")));
  }

  @Test
  void classifyValidationForJakartaValidationExceptions() {
    assertEquals(
        ExceptionFamily.VALIDATION,
        ExceptionFamily.classify(new jakarta.validation.ValidationException("bad input")));
    assertEquals(
        ExceptionFamily.VALIDATION,
        ExceptionFamily.classify(new jakarta.validation.ConstraintViolationException("bad input")));
  }

  @Test
  void classifyBusinessForCustomRuntimeException() {
    assertEquals(
        ExceptionFamily.BUSINESS,
        ExceptionFamily.classify(new OrderRejectedException("customer rule")));
  }

  @Test
  void classifyBusinessForCheckedException() {
    assertEquals(
        ExceptionFamily.BUSINESS, ExceptionFamily.classify(new CheckedBusinessException()));
  }

  @Test
  void classifyUnknownForJdkRuntimeException() {
    assertEquals(ExceptionFamily.UNKNOWN, ExceptionFamily.classify(new NullPointerException()));
  }

  @Test
  void classifyTerminatesOnCyclicCauseChain() {
    assertEquals(ExceptionFamily.UNKNOWN, ExceptionFamily.classify(new SelfCausedError()));
  }

  private static final class OrderRejectedException extends RuntimeException {

    private OrderRejectedException(String message) {
      super(message);
    }
  }

  private static final class CheckedBusinessException extends Exception {}

  private static final class SelfCausedError extends Error {

    @Override
    public synchronized Throwable getCause() {
      return this;
    }
  }

  private static Throwable newHttpTimeoutException(String message) throws Exception {
    Class<?> type = Class.forName("java.net.http.HttpTimeoutException");
    return (Throwable) type.getConstructor(String.class).newInstance(message);
  }
}
