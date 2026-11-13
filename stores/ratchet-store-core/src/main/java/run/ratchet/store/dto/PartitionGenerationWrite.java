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
package run.ratchet.store.dto;

import java.util.List;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.PartitionEntity;
import run.ratchet.store.entity.PartitionSetEntity;

/** Atomic creation or advancement of a partition generation and its ordinary batch parent. */
public record PartitionGenerationWrite(
    PartitionSetEntity set,
    Integer expectedVersion,
    List<PartitionEntity> rows,
    JobEntity parent,
    BatchEntity batch) {}
