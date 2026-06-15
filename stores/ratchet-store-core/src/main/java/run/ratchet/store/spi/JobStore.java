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

import java.util.Optional;
import run.ratchet.api.Incubating;

/**
 * Mandatory core store contract — the only store interface every conforming implementation must
 * provide. It composes the job lifecycle surface: CRUD, claiming, terminal transitions, retry,
 * pause, status CAS, bulk operations, node heartbeat/crash-recovery ({@link NodeStore}), and tag
 * <em>writes</em> ({@link TagStore}).
 *
 * <p>Capabilities a store may legitimately lack — recurring scheduling, batch fan-out, signals,
 * resource permits, distributed locks, archiving, query/analytics/audit reporting, and dead-letter
 * alerting — are <strong>not</strong> extended here. A store advertises an optional capability by
 * additionally implementing its interface; callers probe for it through {@link #capability}.
 *
 * <p>Implementations must be thread-safe.
 */
@Incubating
public interface JobStore
    extends JobCrudStore,
        JobClaimStore,
        JobTerminalStore,
        JobRetryStore,
        JobPauseStore,
        JobBatchStatusStore,
        JobBulkStore,
        NodeStore,
        TagStore {

  /**
   * Returns this store's view as {@code type} when the store advertises that optional capability,
   * otherwise {@link Optional#empty()}.
   *
   * <p>This is the runtime probe seam for optional store capabilities. The reference engine calls
   * it to decide whether a capability-dependent feature (recurring scheduling, batch fan-out,
   * signals, archiving, …) is available, rather than assuming every store implements every
   * capability interface.
   *
   * <p>Java type membership is the single normative source of truth for capability advertisement: a
   * store advertises an optional capability by — and only by — implementing that capability's
   * interface, and the default implementation reflects exactly that. Dependency-injection runtimes
   * discover capabilities from the bean's Java type closure rather than by calling this method; the
   * reference engine, for instance, resolves each optional capability through a CDI {@code
   * Instance<T>}. Because both views derive from the same type membership, the probe and the
   * container agree by construction.
   *
   * <p>Overriding this method to diverge from {@code instanceof} is
   * <strong>non-conforming</strong>: reporting a capability the store does not implement as a type
   * misleads probe-based callers while leaving DI-based callers unable to inject it, and hiding a
   * capability the store does implement does the reverse. Either way the two discovery paths
   * desynchronize. An implementation must never report a capability whose methods are partially
   * implemented or unsupported, since callers treat a present capability as fully usable; the TCK
   * verifies that {@code capability(X).isPresent()} matches {@code X.isInstance(store)} for every
   * capability.
   *
   * @param type the capability interface to probe for; never {@code null}
   * @param <T> the capability type
   * @return the store viewed as {@code type}, or empty when the capability is not advertised
   */
  default <T> Optional<T> capability(Class<T> type) {
    return type.isInstance(this) ? Optional.of(type.cast(this)) : Optional.empty();
  }
}
