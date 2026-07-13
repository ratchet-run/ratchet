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

/**
 * Job lifecycle events published by Ratchet, and the contract for observing them.
 *
 * <p>Every event in this package reaches application code two ways:
 *
 * <ul>
 *   <li><b>CDI observers</b> — declare an observer method such as {@code void on(@Observes
 *       JobCompletedEvent event)} on any managed bean. The container delivers only the event types
 *       a method declares.
 *   <li><b>Programmatic listeners</b> — register a {@link java.util.function.Consumer} through
 *       {@link run.ratchet.api.JobSchedulerService#addEventListener}. A listener receives every
 *       event type and is the right choice outside CDI (Spring, Micronaut, plain Java).
 * </ul>
 *
 * <h2>Delivery contract</h2>
 *
 * <p>The delivery contract:
 *
 * <ul>
 *   <li><b>Synchronous.</b> Observers run on the publishing thread, which is typically the job
 *       execution thread. Ratchet publishes through the synchronous CDI {@code fire} path only, so
 *       an {@code @ObservesAsync} observer is never notified. To move work off the publishing
 *       thread, hand it to your own executor from inside a synchronous observer.
 *   <li><b>After commit.</b> A state-change event raised inside a transaction is delivered after
 *       that transaction commits. A rollback suppresses the event, and an observer cannot roll the
 *       source transaction back. When no transaction is active, delivery is immediate. If Ratchet
 *       cannot register an after-commit callback for an existing transaction, it logs the failure
 *       and suppresses the event rather than risk publishing before the transaction outcome is
 *       known.
 *   <li><b>Contained failures.</b> A programmatic listener runs in its own guard: one that throws
 *       is logged, and the remaining listeners still run. CDI observers follow standard synchronous
 *       {@code fire} semantics — a {@code @Observes} method that throws aborts delivery to the
 *       remaining CDI observers for that event. In both cases the failure stays with the observers
 *       and never rolls back or otherwise disturbs the committed transaction; a CDI observer that
 *       must not block its peers should avoid throwing.
 *   <li><b>Offload heavyweight work.</b> Because delivery sits on the job hot path, any observer
 *       that does I/O, network calls, or other slow work MUST offload it to its own thread pool. A
 *       slow or blocking observer adds latency to job execution and can stall the scheduler.
 * </ul>
 */
package run.ratchet.api.event;
