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
package run.ratchet.spi;

import run.ratchet.api.Incubating;

/**
 * Returns the identifier of this scheduler node. The runtime uses it for heartbeats, job claiming,
 * and cluster coordination, whichever provider supplies it.
 *
 * <p>The identifier must be unique among live nodes and must not change while the node runs. It may
 * change on restart. A node that restarts under the same identifier recovers its previous claims
 * and resource permits as it starts; under a new identifier, orphan recovery reclaims them once the
 * old identifier's heartbeat is older than the orphan grace period.
 *
 * @since 0.1
 */
@Incubating
public interface NodeIdentityProvider {

  /**
   * Returns the non-null node identifier. It must not change for the provider's lifetime and must
   * be unique among live nodes.
   */
  String getNodeId();
}
