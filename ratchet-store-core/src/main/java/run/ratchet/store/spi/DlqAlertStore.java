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
package run.ratchet.store.spi;

import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.DlqAlertEntity;

/** Dead Letter Queue alert persistence operations. */
@Incubating
public interface DlqAlertStore {

  /**
   * Persists a DLQ alert. Transaction attribute: {@code REQUIRED}.
   *
   * @param alert alert row to persist; never {@code null}
   * @return persisted alert (with generated identifiers populated); never {@code null}
   */
  DlqAlertEntity saveDlqAlert(DlqAlertEntity alert);

  /**
   * Checks recent alert suppression state. Transaction attribute: {@code SUPPORTS}.
   *
   * @param jobId job id whose alert history is being checked; never {@code null}
   * @param errorHash stable hash of the alerting error message (caller-computed dedupe key); never
   *     {@code null} or blank
   * @param cutoff window lower bound; only alerts at or after this instant count as "recent". Never
   *     {@code null}.
   * @return {@code true} when at least one alert for the same {@code jobId} / {@code errorHash}
   *     pair has been recorded at or after {@code cutoff}, {@code false} otherwise
   */
  boolean existsRecentDlqAlert(UUID jobId, String errorHash, Instant cutoff);
}
