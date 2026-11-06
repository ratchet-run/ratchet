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
 * The last reported progress. Totals are estimates; readers clamp done to total for display. A
 * reportedAt before executionStartTime means an earlier execution. A continued job keeps the
 * previous run's progress until it reports again.
 */
@Incubating
public record JobProgress(long done, long total, Instant reportedAt) {}
