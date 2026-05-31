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
 * Returns the stable, unique identifier for this scheduler node. Used for heartbeats, job claiming,
 * and cluster coordination.
 *
 * @since 0.1
 */
@Incubating
public interface NodeIdentityProvider {

  /** Returns the non-null node identifier. Must be immutable for the provider lifecycle. */
  String getNodeId();
}
