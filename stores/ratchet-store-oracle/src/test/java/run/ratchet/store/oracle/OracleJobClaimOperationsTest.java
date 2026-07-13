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
package run.ratchet.store.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OracleJobClaimOperationsTest {

  @Test
  void claimSelectColumnOrderMatchesNamedRowMapping() {
    List<String> columns = OracleJobClaimOperations.claimSelectColumnNames();

    assertEquals(
        List.of(
            "job_id",
            "status",
            "job_type",
            "priority",
            "scheduled_time",
            "version",
            "timeout_sec",
            "picked_by",
            "picked_at",
            "business_key",
            "attempts",
            "max_retries",
            "execution_target",
            "depends_on"),
        columns);
    assertEquals(
        Map.ofEntries(
            Map.entry("job_id", 0),
            Map.entry("status", 1),
            Map.entry("job_type", 2),
            Map.entry("priority", 3),
            Map.entry("scheduled_time", 4),
            Map.entry("version", 5),
            Map.entry("timeout_sec", 6),
            Map.entry("picked_by", 7),
            Map.entry("picked_at", 8),
            Map.entry("business_key", 9),
            Map.entry("attempts", 10),
            Map.entry("max_retries", 11),
            Map.entry("execution_target", 12),
            Map.entry("depends_on", 13)),
        OracleJobClaimOperations.claimSelectColumnIndexes());
  }

  @Test
  void claimSelectProjectsDependsOnFromColdMetadata() {
    assertEquals(expectedClaimSelectClause(), OracleJobClaimOperations.claimSelectClause());
  }

  private static String expectedClaimSelectClause() {
    return "job_id, status, job_type, priority, scheduled_time, version, timeout_sec, picked_by,"
        + " picked_at, business_key, attempts, max_retries, execution_target,"
        + " (SELECT cold_job.depends_on FROM scheduler_job cold_job"
        + " WHERE cold_job.job_id = scheduler_job_queue.job_id) AS depends_on";
  }
}
