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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.store.entity.JobExecutionType;

class OracleJobRowMapperTest {

  private static final UUID JOB_ID = UUID.fromString("019ae3d1-3f82-7e18-9f09-a9f000000403");

  @Test
  void hydrateWrapsInvalidEnumWithColumnContext() {
    Object[] row = liveRow();
    row[1] = "UNKNOWN_TYPE";

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> new OracleJobRowMapper().hydrateJobEntity(row));

    assertTrue(thrown.getMessage().contains(JOB_ID.toString()));
    assertTrue(thrown.getMessage().contains("job_type"));
    assertTrue(thrown.getMessage().contains("UNKNOWN_TYPE"));
    assertInstanceOf(IllegalArgumentException.class, thrown.getCause());
  }

  @Test
  void hydrateRejectsRowsWithNoEffectiveStatus() {
    Object[] row = liveRow();
    row[OracleJobRowMapper.IDX_Q_STATUS] = null;

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> new OracleJobRowMapper().hydrateJobEntity(row));

    assertTrue(thrown.getMessage().contains("no live or terminal status"));
  }

  private static Object[] liveRow() {
    Object[] row = new Object[OracleJobRowMapper.HYDRATION_COL_COUNT];
    Instant now = Instant.parse("2026-05-12T14:30:00Z");
    row[0] = JOB_ID.toString();
    row[1] = JobExecutionType.SINGLE.name();
    row[2] = JobPriority.NORMAL.ordinal();
    row[3] = 3;
    row[4] = BackoffPolicy.NONE.name();
    row[5] = 0;
    row[6] = 60;
    row[11] = "example.Job"; // target_class
    row[12] = "run"; // method_name
    row[20] = now; // created_at
    row[OracleJobRowMapper.IDX_Q_STATUS] = "PENDING";
    row[OracleJobRowMapper.IDX_Q_STATUS + 1] = now; // q.scheduled_time
    row[OracleJobRowMapper.IDX_Q_STATUS + 2] = 0; // q.attempts
    row[OracleJobRowMapper.IDX_Q_STATUS + 7] = 0; // q.version
    row[OracleJobRowMapper.IDX_Q_STATUS + 8] = now; // q.updated_at
    return row;
  }
}
