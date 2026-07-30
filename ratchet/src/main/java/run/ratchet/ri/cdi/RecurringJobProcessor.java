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
package run.ratchet.ri.cdi;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import run.ratchet.ri.core.internal.RecurringMethodRegistrar;

/**
 * CDI lifecycle adapter for portable recurring-method registration.
 *
 * <p>The registration, retry, reconciliation, and cleanup behavior lives in {@link
 * RecurringMethodRegistrar}. This bean retains the existing CDI observer topology, including the
 * distinct application-start and deferred runtime-ready entry points.
 */
@ApplicationScoped
public class RecurringJobProcessor {

  private static final Logger log = Logger.getLogger(RecurringJobProcessor.class);

  private final RecurringMethodRegistrar registrar;

  protected RecurringJobProcessor() {
    this.registrar = null;
  }

  @Inject
  public RecurringJobProcessor(RecurringMethodRegistrar registrar) {
    this.registrar = registrar;
  }

  void onStartup(
      @Observes
          @Priority(RatchetRuntimeStart.PRIORITY_RECURRING_REGISTRATION)
          @Initialized(ApplicationScoped.class) Object init) {
    if (RatchetRuntimeStart.logIfDeferred(
        log,
        "@Recurring registration deferred pending RatchetRuntimeStart event; if this runtime"
            + " never fires that event, recurring jobs will never register")) {
      return;
    }
    registerFromApplicationStart();
  }

  void onRuntimeStart(
      @Observes @Priority(RatchetRuntimeStart.PRIORITY_RECURRING_REGISTRATION)
          RatchetRuntimeStart event) {
    registerFromRuntimeStart();
  }

  /** Requests registration from the normal CDI application-scope startup path. */
  public void registerFromApplicationStart() {
    registrar.registerFromApplicationStart();
  }

  /** Requests registration from the deferred CDI runtime-ready event. */
  public void registerFromRuntimeStart() {
    registrar.registerFromRuntimeStart();
  }

  /** Cancels deferred registration work for the current runtime generation. */
  public void cancelRegistration() {
    registrar.cancel();
  }
}
