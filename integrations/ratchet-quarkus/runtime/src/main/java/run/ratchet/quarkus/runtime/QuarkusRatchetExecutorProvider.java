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

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;
import run.ratchet.ri.cdi.StandaloneExecutorProvider;

/**
 * Quarkus has no Jakarta Concurrency managed executor, so Ratchet's JNDI-free {@link
 * StandaloneExecutorProvider} backs job execution. It is {@code @Alternative} with no priority (so
 * it is not auto-enabled); this subclass adds {@code @Priority(APPLICATION)} so the extension
 * enables it without the application needing {@code quarkus.arc.selected-alternatives}. The lazy
 * pools it inherits are native-image-safe (no threads created at bean construction).
 *
 * <p>An application that wants context propagation can override this with its own {@code @Alternative
 * @Priority(APPLICATION + 1)} provider backed by a SmallRye {@code ManagedExecutor}.
 */
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class QuarkusRatchetExecutorProvider extends StandaloneExecutorProvider {}
