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

import java.util.UUID;

/**
 * A durable partition set. The latest batch may have been archived.
 *
 * @param key caller partition key
 * @param name batch name
 * @param partitionCount number of partitions
 * @param latestGeneration latest generation number
 * @param latestBatchId latest batch parent id
 * @param open whether the latest generation is not sealed
 */
@Incubating
public record PartitionSetSummary(
    String key,
    String name,
    int partitionCount,
    int latestGeneration,
    UUID latestBatchId,
    boolean open) {}
