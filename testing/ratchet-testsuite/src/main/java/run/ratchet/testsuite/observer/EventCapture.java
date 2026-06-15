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
package run.ratchet.testsuite.observer;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import run.ratchet.api.event.AbstractJobSchedulerEvent;

/**
 * CDI event observer that captures all scheduler events for test verification.
 *
 * <p>Thread-safe — events can arrive from any thread. Provides {@link #awaitEvent(Class, Duration)}
 * for Awaitility-style waiting.
 */
@ApplicationScoped
public class EventCapture {

  private final CopyOnWriteArrayList<AbstractJobSchedulerEvent> events =
      new CopyOnWriteArrayList<>();
  private final Object awaitLock = new Object();
  private CountDownLatch latch = new CountDownLatch(1);
  private Class<? extends AbstractJobSchedulerEvent> expectedType;

  public void onEvent(@Observes AbstractJobSchedulerEvent event) {
    events.add(event);
    synchronized (awaitLock) {
      if (expectedType != null && expectedType.isInstance(event)) {
        latch.countDown();
      }
    }
  }

  public List<AbstractJobSchedulerEvent> getEvents() {
    return List.copyOf(events);
  }

  @SuppressWarnings("unchecked")
  public <T extends AbstractJobSchedulerEvent> List<T> getEvents(Class<T> type) {
    return events.stream().filter(type::isInstance).map(e -> (T) e).toList();
  }

  public boolean awaitEvent(Class<? extends AbstractJobSchedulerEvent> type, Duration timeout)
      throws InterruptedException {
    CountDownLatch awaitLatch;
    synchronized (awaitLock) {
      // atomically check + arm to avoid missed events
      if (events.stream().anyMatch(type::isInstance)) {
        return true;
      }

      this.expectedType = type;
      this.latch = new CountDownLatch(1);
      awaitLatch = this.latch;
    }

    return awaitLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void clear() {
    events.clear();
    synchronized (awaitLock) {
      expectedType = null;
      latch = new CountDownLatch(1);
    }
  }
}
