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
package run.ratchet.testsuite.concurrency;

import jakarta.enterprise.concurrent.ManagedExecutorDefinition;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Declares a virtual-thread {@code ManagedExecutorService} as an <b>application</b> component (it
 * lands in {@code WEB-INF/classes}), following the standard Jakarta Concurrency 3.1 pattern:
 * {@code @ManagedExecutorDefinition(virtual = true)} on an {@code @ApplicationScoped} class.
 *
 * <p>Ratchet ships no executor definition of its own (the library stays EE-agnostic), so the
 * application owning the executor is the supported pattern. This is what a real deployment does:
 * declare the executor, then set {@code ratchet.worker.job-executor-jndi} to its name. {@link
 * VirtualThreadOptionsProducer} does exactly that for the test. Declaring it as a WAR class also
 * avoids depending on whether a container scans {@code WEB-INF/lib} for resource definitions
 * (GlassFish does not).
 */
@ManagedExecutorDefinition(name = "java:app/concurrent/RatchetTestVirtualExecutor", virtual = true)
@ApplicationScoped
public class VirtualThreadTestExecutor {}
