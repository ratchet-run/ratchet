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

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.NodeEntity;

/** Cluster node registration and health monitoring operations. */
@Incubating
public interface NodeStore {

  /**
   * Inserts or updates a node heartbeat. Transaction attribute: {@code REQUIRED}.
   *
   * @param nodeId stable identity of the heartbeating node; never {@code null} or blank
   * @param ts heartbeat timestamp recorded against the row; never {@code null}
   */
  void upsertHeartbeat(String nodeId, Instant ts);

  /**
   * Finds a node by id. Transaction attribute: {@code SUPPORTS}.
   *
   * @param nodeId stable identity of the node to look up
   * @return matching node, or {@link Optional#empty()} when no row exists for {@code nodeId}
   */
  Optional<NodeEntity> findNodeById(String nodeId);

  /**
   * Finds inactive nodes. Transaction attribute: {@code SUPPORTS}.
   *
   * @param cutoff nodes whose last heartbeat is strictly before this instant are considered
   *     inactive; never {@code null}
   * @return inactive node rows, never {@code null}
   */
  List<NodeEntity> findInactiveNodesSince(Instant cutoff);

  /**
   * Deletes inactive node rows. Transaction attribute: {@code REQUIRED}.
   *
   * @param cutoff nodes whose last heartbeat is strictly before this instant are deleted; never
   *     {@code null}
   * @return number of node rows deleted
   */
  int deleteInactiveNodesSince(Instant cutoff);

  /**
   * Deletes the resolved inactive node rows by id. Transaction attribute: {@code REQUIRED}.
   *
   * <p>Implementors MUST override this method; it is abstract precisely because a silent {@code
   * UnsupportedOperationException} default would defer the failure from compile time to
   * orphan-recovery runtime.
   *
   * @param nodeIds resolved inactive node identities to delete; never {@code null}, may be empty
   * @return number of node rows deleted (zero when {@code nodeIds} is empty)
   */
  int deleteInactiveNodesByIds(Collection<String> nodeIds);

  /**
   * Returns the current database server time for clock skew detection. Transaction attribute:
   * {@code SUPPORTS}.
   *
   * @return database server time at the moment of the call; never {@code null}
   */
  Instant getDatabaseTime();
}
