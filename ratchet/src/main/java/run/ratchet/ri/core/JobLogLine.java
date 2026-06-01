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

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import run.ratchet.store.entity.JobLogEntity.LogLevel;

/** Log entry produced during job execution. Carries the MDC snapshot for diagnostic context. */
record JobLogLine(
    UUID jobId, Instant timestamp, LogLevel level, String message, Map<String, Object> mdc)
    implements Serializable {}
