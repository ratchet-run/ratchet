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

import java.time.Instant;

/**
 * Point-in-time view of a scheduler node's persisted heartbeat state.
 *
 * @param nodeId stable scheduler node identity
 * @param startedAt timestamp recorded when the node row was first created
 * @param lastHeartbeat most recent heartbeat timestamp persisted for the node
 * @param active point-in-time liveness; true when the heartbeat is within the orphan grace window
 * @param local true when this row represents this JVM's node
 */
@Incubating
public record NodeStatus(
    String nodeId, Instant startedAt, Instant lastHeartbeat, boolean active, boolean local) {}
