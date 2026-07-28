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
package run.ratchet.api;

/**
 * Read-only query API for scheduler cluster state.
 *
 * <p>This is a dedicated read-only sibling of {@link JobQueryService}; callers that only need to
 * observe the node roster should depend on this interface instead of job-centric query APIs.
 */
@Incubating
public interface ClusterQueryService {

  /**
   * Returns the full persisted node roster.
   *
   * <p>The roster includes active nodes plus stale nodes that have not yet been swept. Each result
   * carries a point-in-time computed active flag. Items are ordered by newest heartbeat first and
   * capped; the returned {@link JobPage} carries the cap and truncation metadata.
   *
   * @return node roster page, never {@code null}
   */
  JobPage<NodeStatus> getNodes();
}
