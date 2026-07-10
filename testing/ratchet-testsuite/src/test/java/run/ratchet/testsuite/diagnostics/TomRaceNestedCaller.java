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
package run.ratchet.testsuite.diagnostics;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.util.UUID;
import run.ratchet.store.spi.JobCrudStore;

/**
 * Nested transactional caller for {@link UtxTomRaceStressIT}.
 *
 * <p>This bean's class-level {@link Transactional} interceptor sets the executor thread frame's
 * TransactionOperationsManager to {@code NOT_ALLOWED} before {@link #poke()} calls into the store
 * bean's own shared {@code @ApplicationScoped} transactional interceptor, producing the nesting
 * needed by the race described in {@link UtxTomRaceStressIT}.
 */
@ApplicationScoped
@Transactional
public class TomRaceNestedCaller {

  @Inject JobCrudStore store;

  public void poke() {
    store.findById(UUID.randomUUID());
  }
}
