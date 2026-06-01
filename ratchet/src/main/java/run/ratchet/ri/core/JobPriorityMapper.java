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

import run.ratchet.api.JobPriority;

/**
 * Maps stored priority ordinals back to {@link JobPriority}. Recurring masters carry the ordinal as
 * an int so the SPI doesn't drag the enum into store-core; reading code has to clamp because the
 * persisted value may predate enum additions or, in pathological cases, be corrupt.
 */
final class JobPriorityMapper {

  private static final int MAX_ORDINAL = JobPriority.values().length - 1;

  private JobPriorityMapper() {}

  static JobPriority fromOrdinal(int priority) {
    int clamped = Math.max(0, Math.min(priority, MAX_ORDINAL));
    return JobPriority.values()[clamped];
  }
}
