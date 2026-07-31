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
package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import run.ratchet.api.ClusterQueryService;
import run.ratchet.api.JobPage;
import run.ratchet.api.NodeStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.NodeStore;

/** Default {@link ClusterQueryService} implementation backed by the node store SPI. */
@ApplicationScoped
public class DefaultClusterQueryService implements ClusterQueryService {

  private static final int NODE_ROSTER_LIMIT = 1000;

  private final NodeStore nodeStore;
  private final NodeIdentityProvider nodeIdentityProvider;
  private final RatchetOptions options;
  private final Clock clock;

  /** No-arg constructor required by CDI normal-scope proxying. Not for direct use. */
  protected DefaultClusterQueryService() {
    this.nodeStore = null;
    this.nodeIdentityProvider = null;
    this.options = null;
    this.clock = null;
  }

  @Inject
  DefaultClusterQueryService(
      NodeStore nodeStore,
      NodeIdentityProvider nodeIdentityProvider,
      RatchetOptions options,
      Clock clock) {
    this.nodeStore = Objects.requireNonNull(nodeStore, "nodeStore must not be null");
    this.nodeIdentityProvider =
        Objects.requireNonNull(nodeIdentityProvider, "nodeIdentityProvider must not be null");
    this.options = Objects.requireNonNull(options, "options must not be null");
    this.clock = clock;
  }

  /**
   * {@inheritDoc}
   *
   * <p>{@code totalCount} on the returned page reflects the number of rows returned, not a
   * cluster-wide total; {@link NodeStore} has no count query. {@code hasMore} signals whether the
   * roster was capped at {@value #NODE_ROSTER_LIMIT}.
   */
  @Override
  public JobPage<NodeStatus> getNodes() {
    Instant cutoff = effective().instant().minusSeconds(options.node().orphanGraceSeconds());
    String localId = nodeIdentityProvider.getNodeId();
    List<NodeEntity> rows = nodeStore.findAllNodes(NODE_ROSTER_LIMIT);
    List<NodeStatus> items =
        rows.stream()
            .map(
                node ->
                    new NodeStatus(
                        node.getId(),
                        node.getStartedAt(),
                        node.getLastHeartbeat(),
                        !node.getLastHeartbeat().isBefore(cutoff),
                        node.getId().equals(localId)))
            .toList();
    boolean hasMore = rows.size() >= NODE_ROSTER_LIMIT;
    return new JobPage<>(items, items.size(), NODE_ROSTER_LIMIT, 0, hasMore, null);
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}
