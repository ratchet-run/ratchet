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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.tck.store.JpaContainerFixture.NativeQueryEvent;

class JpaFailureInjectionTest {
  private final JpaContainerFixture fixture = mock(JpaContainerFixture.class, CALLS_REAL_METHODS);

  @Test
  void preservesTheCauseWhenWorkFailsBeforeAdvancing() {
    var original = new IllegalStateException("database unavailable");
    var failure =
        assertThrows(
            AssertionError.class,
            () ->
                fixture.failBeforeRecurringArchiveAfterAdvance(
                    () -> {
                      throw original;
                    }));
    assertEquals("No earlier-master UPDATE was executed", failure.getMessage());
    assertSame(original, failure.getCause());
    assertObserverReset();
  }

  @Test
  void preservesAnAssertionFailureBeforeAdvancing() {
    var original = new AssertionError("unexpected update count");
    var failure =
        assertThrows(
            AssertionError.class,
            () ->
                fixture.failBeforeRecurringArchiveAfterAdvance(
                    () -> {
                      throw original;
                    }));
    assertSame(original, failure.getCause());
    assertObserverReset();
  }

  @Test
  void rejectsNormalReturnWithoutAnAdvance() {
    var failure =
        assertThrows(
            AssertionError.class, () -> fixture.failBeforeRecurringArchiveAfterAdvance(() -> {}));
    assertEquals("No earlier-master UPDATE was executed", failure.getMessage());
    assertObserverReset();
  }

  @Test
  void propagatesInjectedFailureAfterAnAdvanceAndResetsObserver() {
    var failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                fixture.failBeforeRecurringArchiveAfterAdvance(
                    () -> {
                      ArgumentCaptor<BiConsumer<String, NativeQueryEvent>> observer =
                          ArgumentCaptor.captor();
                      verify(fixture).observeNativeQueries(observer.capture());
                      observer
                          .getValue()
                          .accept(
                              "UPDATE scheduler_recurring_job SET next_fire = ?",
                              new NativeQueryEvent("executeUpdate", true, 1));
                      observer
                          .getValue()
                          .accept(
                              "INSERT INTO scheduler_recurring_job_archive",
                              new NativeQueryEvent("executeUpdate", false, null));
                    }));
    assertEquals(
        "Injected failure while archiving the later recurring master", failure.getMessage());
    assertObserverReset();
  }

  private void assertObserverReset() {
    ArgumentCaptor<BiConsumer<String, NativeQueryEvent>> observers = ArgumentCaptor.captor();
    verify(fixture, times(2)).observeNativeQueries(observers.capture());
    assertDoesNotThrow(
        () ->
            observers
                .getValue()
                .accept(
                    "INSERT INTO scheduler_recurring_job_archive",
                    new NativeQueryEvent("executeUpdate", false, null)));
  }
}
