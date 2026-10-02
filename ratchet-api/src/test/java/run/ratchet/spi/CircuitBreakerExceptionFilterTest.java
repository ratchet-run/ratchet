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

import static org.junit.jupiter.api.Assertions.*;
import static run.ratchet.spi.CircuitBreakerExceptionFilter.Outcome.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

class CircuitBreakerExceptionFilterTest {

  @Test
  void emptyAndNullListsRecordAll() {
    assertEquals(RECORDED, CircuitBreakerExceptionFilter.RECORD_ALL.classify(new Exception()));
    CircuitBreakerExceptionFilter filter = new CircuitBreakerExceptionFilter(null, null, null);
    assertEquals(CircuitBreakerExceptionFilter.RECORD_ALL, filter);
    assertTrue(filter.isRecordAll());
  }

  @Test
  void recordClassesMatchSubclassesAndOtherExceptionsCountAsSuccesses() {
    CircuitBreakerExceptionFilter filter =
        new CircuitBreakerExceptionFilter(List.of(IllegalArgumentException.class), List.of(), null);
    assertEquals(RECORDED, filter.classify(new IllegalArgumentException()));
    assertEquals(RECORDED, filter.classify(new NumberFormatException()));
    assertEquals(NOT_RECORDED, filter.classify(new IllegalStateException()));
  }

  @Test
  void ignoreWinsOverTheSameRecordClassAndRecordedSuperclass() {
    for (Class<? extends Throwable> recorded :
        List.of(IllegalArgumentException.class, RuntimeException.class)) {
      CircuitBreakerExceptionFilter filter =
          new CircuitBreakerExceptionFilter(
              List.of(recorded), List.of(IllegalArgumentException.class), t -> true);
      assertEquals(IGNORED, filter.classify(new IllegalArgumentException()));
      assertEquals(IGNORED, filter.classify(new NumberFormatException()));
    }
    CircuitBreakerExceptionFilter filter =
        new CircuitBreakerExceptionFilter(List.of(), List.of(IllegalArgumentException.class), null);
    assertEquals(IGNORED, filter.classify(new IllegalArgumentException()));
    assertEquals(RECORDED, filter.classify(new IllegalStateException()));
  }

  @Test
  void causeChainMatchesTwoLevelsDeepAndIgnoreWinsAfterOuterRecordMatch() {
    Throwable wrapped =
        new RuntimeException(new CompletionException(new IllegalArgumentException()));
    assertEquals(
        RECORDED,
        new CircuitBreakerExceptionFilter(List.of(IllegalArgumentException.class), List.of(), null)
            .classify(wrapped));
    assertEquals(
        IGNORED,
        new CircuitBreakerExceptionFilter(
                List.of(RuntimeException.class), List.of(IllegalArgumentException.class), null)
            .classify(wrapped));
  }

  @Test
  void causeCycleTerminates() {
    Exception first = new Exception();
    Exception second = new Exception();
    first.initCause(second);
    second.initCause(first);
    assertEquals(RECORDED, CircuitBreakerExceptionFilter.RECORD_ALL.classify(first));
    assertEquals(
        NOT_RECORDED,
        new CircuitBreakerExceptionFilter(List.of(IllegalArgumentException.class), List.of(), null)
            .classify(first));
  }

  @Test
  void throwingPredicateRecordsAndAttachesItsExceptionAsSuppressed() {
    IllegalStateException predicateFailure = new IllegalStateException("predicate bug");
    CircuitBreakerExceptionFilter filter =
        new CircuitBreakerExceptionFilter(
            List.of(),
            List.of(),
            t -> {
              throw predicateFailure;
            });
    Exception original = new Exception("task failure");
    assertEquals(RECORDED, filter.classify(original));
    assertArrayEquals(new Throwable[] {predicateFailure}, original.getSuppressed());
    assertNull(original.getCause());
  }

  @Test
  void predicateRethrowingTheOriginalRecordsWithoutSelfSuppression() {
    CircuitBreakerExceptionFilter filter =
        new CircuitBreakerExceptionFilter(
            List.of(),
            List.of(),
            t -> {
              throw (RuntimeException) t;
            });
    RuntimeException original = new RuntimeException("task failure");
    assertEquals(RECORDED, filter.classify(original));
    assertEquals(0, original.getSuppressed().length);
  }

  @Test
  void predicateReceivesOriginalThrowableAndIsOrEdWithRecordClasses() {
    Throwable original = new RuntimeException(new IllegalStateException());
    Predicate<Throwable> predicate = t -> t == original;
    CircuitBreakerExceptionFilter filter =
        new CircuitBreakerExceptionFilter(
            List.of(IllegalArgumentException.class), List.of(), predicate);
    assertEquals(RECORDED, filter.classify(original));
    assertEquals(RECORDED, filter.classify(new IllegalArgumentException()));
    assertEquals(NOT_RECORDED, filter.classify(new IllegalStateException()));
    assertEquals(
        RECORDED,
        new CircuitBreakerExceptionFilter(List.of(), List.of(), t -> true)
            .classify(new Exception()));
    assertEquals(
        NOT_RECORDED,
        new CircuitBreakerExceptionFilter(List.of(), List.of(), t -> false)
            .classify(new Exception()));
  }

  @Test
  void mergeUnionsClassesAndOrsPredicates() {
    CircuitBreakerExceptionFilter first =
        new CircuitBreakerExceptionFilter(
            List.of(IllegalArgumentException.class),
            List.of(NumberFormatException.class),
            t -> "first".equals(t.getMessage()));
    CircuitBreakerExceptionFilter second =
        new CircuitBreakerExceptionFilter(
            List.of(IllegalStateException.class, IllegalArgumentException.class),
            List.of(UnsupportedOperationException.class),
            t -> "second".equals(t.getMessage()));
    CircuitBreakerExceptionFilter merged = first.merge(second);
    assertEquals(
        List.of(IllegalArgumentException.class, IllegalStateException.class),
        merged.recordExceptions());
    assertEquals(
        List.of(NumberFormatException.class, UnsupportedOperationException.class),
        merged.ignoreExceptions());
    assertEquals(RECORDED, merged.classify(new Exception("first")));
    assertEquals(RECORDED, merged.classify(new Exception("second")));
    assertEquals(NOT_RECORDED, merged.classify(new Exception("other")));
    assertEquals(RECORDED, merged.classify(new IllegalArgumentException()));
    assertEquals(RECORDED, merged.classify(new IllegalStateException()));
    assertEquals(IGNORED, merged.classify(new NumberFormatException()));
    assertEquals(IGNORED, merged.classify(new UnsupportedOperationException()));
    assertSame(first, first.merge(null));
    assertEquals(
        RECORDED,
        CircuitBreakerExceptionFilter.RECORD_ALL.merge(first).classify(new Exception("first")));
    assertEquals(
        NOT_RECORDED,
        first.merge(CircuitBreakerExceptionFilter.RECORD_ALL).classify(new Exception("other")));
    assertEquals(
        RECORDED,
        new CircuitBreakerExceptionFilter(List.of(), List.of(), null)
            .merge(
                new CircuitBreakerExceptionFilter(
                    List.of(IllegalStateException.class), List.of(), null))
            .classify(new IllegalStateException()));
  }

  @Test
  void listsAndAnnotationArraysAreCopiedAndNullElementsRejected() {
    List<Class<? extends Throwable>> classes =
        new ArrayList<>(List.of(IllegalArgumentException.class));
    CircuitBreakerExceptionFilter filter =
        new CircuitBreakerExceptionFilter(classes, classes, null);
    classes.clear();
    assertEquals(List.of(IllegalArgumentException.class), filter.recordExceptions());
    assertEquals(List.of(IllegalArgumentException.class), filter.ignoreExceptions());
    assertThrows(UnsupportedOperationException.class, () -> filter.recordExceptions().clear());
    assertThrows(UnsupportedOperationException.class, () -> filter.ignoreExceptions().clear());
    classes.add(null);
    assertThrows(
        NullPointerException.class,
        () -> new CircuitBreakerExceptionFilter(classes, List.of(), null));
    assertThrows(
        NullPointerException.class,
        () -> new CircuitBreakerExceptionFilter(List.of(), classes, null));
    Class<? extends Throwable>[] record = exceptionClasses(IllegalArgumentException.class);
    Class<? extends Throwable>[] ignore = exceptionClasses(IllegalStateException.class);
    CircuitBreakerExceptionFilter fromArrays = CircuitBreakerExceptionFilter.of(record, ignore);
    record[0] = RuntimeException.class;
    ignore[0] = RuntimeException.class;
    assertEquals(RECORDED, fromArrays.classify(new IllegalArgumentException()));
    assertEquals(IGNORED, fromArrays.classify(new IllegalStateException()));
    assertEquals(NOT_RECORDED, fromArrays.classify(new RuntimeException()));
    assertEquals(
        CircuitBreakerExceptionFilter.RECORD_ALL, CircuitBreakerExceptionFilter.of(null, null));
  }

  @Test
  void ignoreMatchDoesNotEvaluatePredicate() {
    CircuitBreakerExceptionFilter filter =
        new CircuitBreakerExceptionFilter(
            List.of(),
            List.of(IllegalArgumentException.class),
            t -> {
              fail("Ignored exceptions must bypass the predicate");
              return true;
            });
    assertEquals(IGNORED, filter.classify(new IllegalArgumentException()));
  }

  @SafeVarargs
  private static Class<? extends Throwable>[] exceptionClasses(
      Class<? extends Throwable>... classes) {
    return classes;
  }
}
