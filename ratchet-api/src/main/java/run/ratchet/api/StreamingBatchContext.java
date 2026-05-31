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
 * Progress snapshot during the streaming phase of batch construction (before job execution). Unlike
 * {@link BatchContext}, the total item count is unknown because the stream has not been fully
 * consumed.
 *
 * @param batchId UUIDv7 id of the batch parent job
 * @param processedItems cumulative items read from the stream
 * @param chunksInserted number of bulk insert operations performed
 * @see BatchContext
 * @see StreamingBatchBuilder#onProgress(java.util.function.Consumer)
 */
public record StreamingBatchContext(UUID batchId, int processedItems, int chunksInserted) {}
