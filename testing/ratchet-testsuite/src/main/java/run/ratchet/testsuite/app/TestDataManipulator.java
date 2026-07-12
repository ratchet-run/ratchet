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
package run.ratchet.testsuite.app;

import java.time.Instant;
import java.util.UUID;

/** Backend-specific test data mutations not in store SPI. */
public interface TestDataManipulator {

  void setJobUpdatedAt(UUID jobId, Instant updatedAt);

  void setJobPickedAt(UUID jobId, Instant pickedAt);

  void setArchivedAt(UUID archiveId, Instant archivedAt);
}
