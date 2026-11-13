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
 * The latest run of a partition.
 *
 * @param index position in the original range list
 * @param range keys processed
 * @param generation last generation containing this partition
 * @param childJobId child id for existing job operations
 * @param status sealed outcome or live status
 * @param progress live progress, if available
 * @param lastError sanitized last error
 */
@Incubating
public record BatchPartition(
    int index,
    KeyRange range,
    int generation,
    @Nullable UUID childJobId,
    PartitionStatus status,
    @Nullable JobProgress progress,
    @Nullable String lastError) {}
