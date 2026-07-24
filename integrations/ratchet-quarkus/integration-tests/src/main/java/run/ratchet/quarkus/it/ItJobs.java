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
package run.ratchet.quarkus.it;

import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.logging.Logger;
import run.ratchet.api.Recurring;

/**
 * A trivial CDI bean exercising both Ratchet job paths under Quarkus: a method reference submitted
 * on demand ({@link #recordRun()}), and a {@code @Recurring}-annotated method registered at
 * startup. The recurring registration only happens once {@link
 * run.ratchet.ri.cdi.RatchetRuntimeStart} fires (see {@code RecurringJobProcessor.onRuntimeStart}),
 * so this bean firing at all is proof that the deferred-start wiring works end to end on a live
 * Quarkus boot, not just in a unit test.
 *
 * <p>{@code @RegisterForReflection} keeps these methods reflectively invokable in a GraalVM native
 * image: Ratchet resolves the bean and calls the method via reflection at execution time.
 */
@RegisterForReflection
@ApplicationScoped
public class ItJobs {

  private static final Logger LOG = Logger.getLogger(ItJobs.class);

  private final AtomicReference<String> executedOnThread = new AtomicReference<>();
  private final AtomicBoolean recurringExecuted = new AtomicBoolean();

  /** Job body. Submitted as the method reference {@code itJobs::recordRun}. */
  public void recordRun() {
    String thread = Thread.currentThread().getName();
    executedOnThread.set(thread);
    LOG.infof("ItJobs.recordRun executed on thread %s", thread);
  }

  public boolean hasExecuted() {
    return executedOnThread.get() != null;
  }

  /** Registered via onRuntimeStart() at Quarkus boot; fires every second. */
  @Recurring(id = "it-recurring-job", cron = "0/1 * * * * ?")
  public void recurringRun() {
    recurringExecuted.set(true);
    LOG.info("ItJobs.recurringRun executed");
  }

  public boolean hasRecurringExecuted() {
    return recurringExecuted.get();
  }
}
