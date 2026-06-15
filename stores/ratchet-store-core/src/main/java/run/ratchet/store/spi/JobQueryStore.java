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
import run.ratchet.api.JobFilter;
import run.ratchet.store.entity.JobEntity;

/**
 * Read-only search and filter operations for dashboard and admin queries.
 *
 * <p>Implementations MUST NOT modify any job state. All methods are read-only.
 *
 * <p>SQL implementations should build parameterized WHERE clauses from the filter fields; no string
 * concatenation of user-supplied values is permitted.
 */
@Incubating
public interface JobQueryStore {

  /**
   * Returns jobs matching the given filter, ordered by the filter's sort field.
   *
   * @param filter filter criteria; null fields are ignored (no constraint)
   * @param limit maximum number of results
   * @param offset zero-based starting position
   * @return matching jobs, possibly empty; never null
   *     <p>Transaction attribute: {@code SUPPORTS}.
   */
  List<JobEntity> searchJobs(JobFilter filter, int limit, int offset);

  /**
   * Returns the total number of jobs matching the given filter, regardless of limit/offset.
   *
   * @param filter filter criteria; null fields are ignored
   * @return count of matching jobs
   *     <p>Transaction attribute: {@code SUPPORTS}.
   */
  long countJobs(JobFilter filter);

  /**
   * Finds job ids for a tag. Transaction attribute: {@code SUPPORTS}.
   *
   * @param tag tag name to look up; never {@code null} or blank
   * @param limit maximum number of ids to return; must be positive
   * @param offset zero-based pagination offset; must be non-negative
   * @return ordered job ids carrying the tag, never {@code null}
   */
  List<UUID> findJobIdsByTag(String tag, int limit, int offset);
}
