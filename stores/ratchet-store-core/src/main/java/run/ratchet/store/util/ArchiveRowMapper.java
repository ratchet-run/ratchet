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
package run.ratchet.store.util;

import static run.ratchet.store.util.RowValues.longOrNull;
import static run.ratchet.store.util.RowValues.stringOrNull;
import static run.ratchet.store.util.RowValues.uuidOrNull;

import java.time.Instant;
import java.util.function.Function;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobExecutionType;

/** Maps explicit archive native-query projections into {@link ArchivedJobEntity} instances. */
public final class ArchiveRowMapper {

  public static final int COLUMN_COUNT = 33;

  private ArchiveRowMapper() {}

  /**
   * Maps one archive projection row.
   *
   * @throws IllegalStateException when the projection shape or column values do not match the
   *     archive row contract
   */
  public static ArchivedJobEntity map(Object[] row, Function<Object, Instant> instantMapper) {
    assertColumnCount(row);
    RowCursor cursor = new RowCursor(row);
    ArchivedJobEntity archive = new ArchivedJobEntity();
    archive.setId(uuidOrNull(cursor.next("id")));
    archive.setOriginalJobId(uuidOrNull(cursor.next("original_job_id")));
    archive.setFinalStatus(enumValue(cursor.next("final_status"), "final_status", JobStatus.class));
    archive.setJobType(enumValue(cursor.next("job_type"), "job_type", JobExecutionType.class));
    archive.setPriority(priorityValue(cursor.next("priority"), "priority"));
    archive.setTotalAttempts(((Number) cursor.next("total_attempts")).intValue());
    archive.setMaxRetries(((Number) cursor.next("max_retries")).intValue());
    archive.setBackoffPolicy(
        enumValue(cursor.next("backoff_policy"), "backoff_policy", BackoffPolicy.class));
    archive.setBackoffParamMs(((Number) cursor.next("backoff_param_ms")).intValue());
    archive.setTimeoutSec(((Number) cursor.next("timeout_sec")).intValue());
    archive.setTargetClass(stringOrNull(cursor.next("target_class")));
    archive.setMethodName(stringOrNull(cursor.next("method_name")));
    archive.setBusinessKey(stringOrNull(cursor.next("business_key")));
    archive.setCronExpr(stringOrNull(cursor.next("cron_expr")));
    archive.setZoneId(stringOrNull(cursor.next("zone_id")));
    archive.setOriginalScheduledTime(instantMapper.apply(cursor.next("original_scheduled_time")));
    archive.setOriginalCreatedAt(instantMapper.apply(cursor.next("original_created_at")));
    archive.setFirstExecutionTime(instantMapper.apply(cursor.next("first_execution_time")));
    archive.setCompletionTime(instantMapper.apply(cursor.next("completion_time")));
    archive.setTotalExecutionTimeMs(longOrNull(cursor.next("total_execution_time_ms")));
    archive.setQueueWaitMs(longOrNull(cursor.next("queue_wait_ms")));
    archive.setArchivedAt(instantMapper.apply(cursor.next("archived_at")));
    archive.setArchivedBy(stringOrNull(cursor.next("archived_by")));
    archive.setArchiveReason(stringOrNull(cursor.next("archive_reason")));
    archive.setJobResult(stringOrNull(cursor.next("job_result")));
    archive.setResultType(stringOrNull(cursor.next("result_type")));
    archive.setFinalError(stringOrNull(cursor.next("final_error")));
    archive.setPayloadSummary(stringOrNull(cursor.next("payload_summary")));
    archive.setDependedOn(uuidOrNull(cursor.next("depended_on")));
    archive.setSupersededBy(uuidOrNull(cursor.next("superseded_by")));
    archive.setTags(stringOrNull(cursor.next("tags")));
    archive.setProperties(stringOrNull(cursor.next("properties")));
    archive.setExtensionState(stringOrNull(cursor.next("extension_state")));
    return archive;
  }

  private static void assertColumnCount(Object[] row) {
    if (row == null || row.length != COLUMN_COUNT) {
      throw new IllegalStateException(
          "Expected archive row with "
              + COLUMN_COUNT
              + " columns but got "
              + (row == null ? "null" : row.length));
    }
  }

  private static <E extends Enum<E>> E enumValue(
      Object value, String columnName, Class<E> enumType) {
    if (value == null) {
      throw new IllegalStateException("Archive row column " + columnName + " must not be null");
    }
    String name = value.toString();
    try {
      return Enum.valueOf(enumType, name);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          "Archive row column "
              + columnName
              + " has invalid "
              + enumType.getSimpleName()
              + " value: "
              + name,
          e);
    }
  }

  private static JobPriority priorityValue(Object value, String columnName) {
    if (value == null) {
      throw new IllegalStateException("Archive row column " + columnName + " must not be null");
    }
    int ordinal = ((Number) value).intValue();
    JobPriority[] priorities = JobPriority.values();
    if (ordinal < 0 || ordinal >= priorities.length) {
      throw new IllegalStateException(
          "Archive row column "
              + columnName
              + " has invalid JobPriority ordinal "
              + ordinal
              + "; expected 0.."
              + (priorities.length - 1));
    }
    return priorities[ordinal];
  }

  private static final class RowCursor {
    private final Object[] row;
    private int column;

    private RowCursor(Object[] row) {
      this.row = row;
    }

    private Object next(String columnName) {
      if (column >= row.length) {
        throw new IllegalStateException("Archive row ended before column " + columnName);
      }
      return row[column++];
    }
  }
}
