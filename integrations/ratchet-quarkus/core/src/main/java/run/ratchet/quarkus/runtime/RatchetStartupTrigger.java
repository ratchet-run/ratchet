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
package run.ratchet.quarkus.runtime;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import run.ratchet.ri.cdi.RatchetRuntimeStart;

/**
 * Starts the Ratchet engine at RUNTIME_INIT on Quarkus.
 *
 * <p>The engine's own {@code @Observes @Initialized(ApplicationScoped.class)} startup observers fire
 * during STATIC_INIT — before Hibernate's {@code Session} exists (and, for native, at image-build
 * time). The extension's build step sets {@code ratchet.lifecycle.defer-auto-start=true} so those
 * observers no-op, and this bean fires {@link RatchetRuntimeStart} from {@link StartupEvent} —
 * which the engine also observes — to start it once the persistence unit and JTA are ready.
 */
@ApplicationScoped
public class RatchetStartupTrigger {

  @Inject Event<RatchetRuntimeStart> startEvent;

  void onStart(@Observes StartupEvent event) {
    startEvent.fire(new RatchetRuntimeStart());
  }
}
