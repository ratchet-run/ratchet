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
package run.ratchet.tck.jakarta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import jakarta.transaction.UserTransaction;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.tck.api.RatchetTckRuntime;
import run.ratchet.tck.api.TckJobs;

/**
 * TCK contract: {@code addEventListener} and {@code removeEventListener} on {@link
 * run.ratchet.api.JobSchedulerService} carry transaction attribute {@code NOT_SUPPORTED}. They MUST
 * NOT participate in any transaction. Listener state is in-memory and MUST NOT be rolled back by a
 * surrounding transaction.
 *
 * <ul>
 *   <li>A listener registered inside a rolled-back transaction MUST still be registered.
 *   <li>A listener removed inside a rolled-back transaction MUST remain unregistered.
 * </ul>
 */
public abstract class AbstractTxNotSupportedContract {

  @Inject protected UserTransaction tx;

  /** Listener installed by the current test. Cleaned up in {@code @AfterEach} if not null. */
  private Consumer<Object> testListener;

  @AfterEach
  void clearAfterEach() {
    if (testListener != null) {
      runtime().scheduler().removeEventListener(testListener);
      testListener = null;
    }
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void addEventListener_insideRolledBackTx_listenerStillFires() throws Exception {
    AtomicInteger eventCount = new AtomicInteger();
    testListener = e -> eventCount.incrementAndGet();

    tx.begin();
    runtime().scheduler().addEventListener(testListener);
    tx.rollback();

    JobHandle handle = runtime().scheduler().enqueueNow(TckJobs::noop);
    runtime().probe().track(handle);
    assertTrue(runtime().probe().awaitCompleted(handle, defaultTimeout()));

    assertTrue(
        eventCount.get() > 0,
        "Listener registered inside a rolled-back TX must remain registered. addEventListener "
            + "is NOT_SUPPORTED — the registration must commit immediately, independent of any "
            + "surrounding TX. eventCount=0 means the implementation rolled back the registration "
            + "with the TX, which violates the NOT_SUPPORTED contract.");
  }

  @Test
  void removeEventListener_insideRolledBackTx_listenerDoesNotFire() throws Exception {
    AtomicInteger eventCount = new AtomicInteger();
    testListener = e -> eventCount.incrementAndGet();

    runtime().scheduler().addEventListener(testListener);

    tx.begin();
    runtime().scheduler().removeEventListener(testListener);
    tx.rollback();
    // Removal persists despite rollback (NOT_SUPPORTED) — clear testListener so @AfterEach
    // does not attempt a double-remove.
    testListener = null;

    JobHandle handle = runtime().scheduler().enqueueNow(TckJobs::noop);
    runtime().probe().track(handle);
    assertTrue(runtime().probe().awaitCompleted(handle, defaultTimeout()));

    assertEquals(
        0,
        eventCount.get(),
        "Listener removed inside a rolled-back TX must remain unregistered. removeEventListener "
            + "is NOT_SUPPORTED — the removal must commit immediately, independent of any "
            + "surrounding TX. eventCount>0 means the implementation un-removed the listener "
            + "on rollback, which violates the NOT_SUPPORTED contract.");
  }

  protected abstract RatchetTckRuntime runtime();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(10);
  }
}
