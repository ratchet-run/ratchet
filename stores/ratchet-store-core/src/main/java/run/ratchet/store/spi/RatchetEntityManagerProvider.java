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
package run.ratchet.store.spi;

import jakarta.persistence.EntityManager;
import run.ratchet.api.Incubating;

/**
 * Provides the {@link EntityManager} used by SQL store implementations.
 *
 * <p>Applications with multiple persistence units can override this SPI with a CDI alternative that
 * injects the desired {@code @PersistenceContext(unitName = "...")}. The store modules' default
 * providers keep the unnamed {@code @PersistenceContext} behavior.
 */
@Incubating
public interface RatchetEntityManagerProvider {

  EntityManager getEntityManager();
}
