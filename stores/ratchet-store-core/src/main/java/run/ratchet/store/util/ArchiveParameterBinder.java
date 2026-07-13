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

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Function;
import run.ratchet.store.entity.ArchivedJobEntity;

/**
 * Shared archive-insert column list and positional parameter binder for the SQL stores.
 *
 * <p>The column list, the value placeholders, and the 33-parameter binding order are identical
 * across dialects; only the four UUID columns differ in encoding (MySQL stores {@code BINARY(16)},
 * PostgreSQL stores native {@code uuid}). Callers pass an {@code idEncoder} that maps a {@link
 * UUID} to its stored form.
 */
public final class ArchiveParameterBinder {

  public static final String ARCHIVE_COLUMNS =
      """
      archive_id, original_job_id, final_status, job_type, priority, total_attempts,
      max_retries, backoff_policy, backoff_param_ms, timeout_sec, target_class,
      method_name, business_key, cron_expr, zone_id, original_scheduled_time,
      original_created_at, first_execution_time, completion_time,
      total_execution_time_ms, queue_wait_ms, archived_at, archived_by, archive_reason,
      job_result, result_type, final_error, payload_summary, depended_on, superseded_by,
      tags, properties, extension_state
      """;

  public static final String ARCHIVE_VALUE_PLACEHOLDERS =
      "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private ArchiveParameterBinder() {}

  /**
   * Binds the 33 archive columns in schema order onto {@code query}, starting at positional index
   * {@code parameter}. The four UUID columns ({@code archive_id}, {@code original_job_id}, {@code
   * depended_on}, {@code superseded_by}) are passed through {@code idEncoder}.
   *
   * @return the next free positional parameter index
   */
  public static int bind(
      Query query, ArchivedJobEntity archive, int parameter, Function<UUID, Object> idEncoder) {
    query.setParameter(parameter++, idEncoder.apply(archive.getId()));
    query.setParameter(parameter++, idEncoder.apply(archive.getOriginalJobId()));
    query.setParameter(parameter++, archive.getFinalStatus().name());
    query.setParameter(parameter++, archive.getJobType().name());
    query.setParameter(parameter++, archive.getPriority().persistedCode());
    query.setParameter(parameter++, archive.getTotalAttempts());
    query.setParameter(parameter++, archive.getMaxRetries());
    query.setParameter(parameter++, archive.getBackoffPolicy().name());
    query.setParameter(parameter++, archive.getBackoffParamMs());
    query.setParameter(parameter++, archive.getTimeoutSec());
    query.setParameter(parameter++, archive.getTargetClass());
    query.setParameter(parameter++, archive.getMethodName());
    query.setParameter(parameter++, archive.getBusinessKey());
    query.setParameter(parameter++, archive.getCronExpr());
    query.setParameter(parameter++, archive.getZoneId());
    query.setParameter(parameter++, timestampOrNull(archive.getOriginalScheduledTime()));
    query.setParameter(parameter++, timestampOrNull(archive.getOriginalCreatedAt()));
    query.setParameter(parameter++, timestampOrNull(archive.getFirstExecutionTime()));
    query.setParameter(parameter++, timestampOrNull(archive.getCompletionTime()));
    query.setParameter(parameter++, archive.getTotalExecutionTimeMs());
    query.setParameter(parameter++, archive.getQueueWaitMs());
    query.setParameter(parameter++, timestampOrNull(archive.getArchivedAt()));
    query.setParameter(parameter++, archive.getArchivedBy());
    query.setParameter(parameter++, archive.getArchiveReason());
    query.setParameter(parameter++, archive.getJobResult());
    query.setParameter(parameter++, archive.getResultType());
    query.setParameter(parameter++, archive.getFinalError());
    query.setParameter(parameter++, archive.getPayloadSummary());
    query.setParameter(parameter++, idEncoder.apply(archive.getDependedOn()));
    query.setParameter(parameter++, idEncoder.apply(archive.getSupersededBy()));
    query.setParameter(parameter++, archive.getTags());
    query.setParameter(parameter++, archive.getProperties());
    query.setParameter(parameter++, archive.getExtensionState());
    return parameter;
  }

  private static Timestamp timestampOrNull(Instant instant) {
    return instant == null ? null : Timestamp.from(instant);
  }
}
