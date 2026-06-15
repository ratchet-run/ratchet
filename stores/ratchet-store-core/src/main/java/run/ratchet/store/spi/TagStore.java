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

import java.util.List;
import java.util.UUID;
import run.ratchet.api.Incubating;

/**
 * Job tag write operations.
 *
 * <p>Tag <em>writes</em> sit on the core submit path ({@code insertTags} runs during job creation),
 * so they stay on the mandatory contract. Tag <em>reads</em> are split off: id lookup lives on
 * {@link JobQueryStore} and the per-tag aggregate counts live on {@link JobAnalyticsStore}, both
 * optional reporting capabilities.
 */
@Incubating
public interface TagStore {

  /**
   * Inserts tags for one job. Transaction attribute: {@code REQUIRED}.
   *
   * @param jobId job id receiving the tags; never {@code null}
   * @param tags tag names to attach; never {@code null}, may be empty (no-op when empty)
   */
  void insertTags(UUID jobId, List<String> tags);

  /**
   * Deletes tags for one job. Transaction attribute: {@code REQUIRED}.
   *
   * @param jobId job id whose tags should be removed; never {@code null}
   * @return number of tag rows deleted
   */
  int deleteTagsByJobId(UUID jobId);
}
