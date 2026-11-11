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
package run.ratchet.ri.core.internal;

import java.time.Instant;
import run.ratchet.api.event.AbstractJobSchedulerEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.store.dto.OutboxEvent;
import run.ratchet.store.dto.OutboxEventType;
import run.ratchet.store.id.UuidV7Factory;

/** Converts terminal events without depending on an application payload serializer. */
public final class TerminalEventMapper {
  private TerminalEventMapper() {}

  public static OutboxEvent toRow(AbstractJobSchedulerEvent event, Instant createdAt) {
    OutboxEventType type;
    Long duration = null;
    String error = null;
    int attempt = 0;
    if (event instanceof JobCompletedEvent completed) {
      type = OutboxEventType.JOB_COMPLETED;
      duration = completed.getExecutionTimeMs();
    } else if (event instanceof JobFailedEvent failed) {
      type = OutboxEventType.JOB_FAILED;
      error = failed.getErrorMessage();
      attempt = failed.getRetryAttempt();
    } else if (event instanceof JobDlqEvent dlq) {
      type = OutboxEventType.JOB_DLQ;
      error = dlq.getErrorMessage();
      attempt = dlq.getRetryAttempt();
    } else {
      throw new IllegalArgumentException("Not a durable terminal event: " + event.getClass());
    }
    return new OutboxEvent(
        event.getEventId(),
        type,
        event.getJobId(),
        event.getBusinessKey(),
        event.getRecurringMasterId(),
        event.getJobType(),
        event.getPriority(),
        event.getNodeId(),
        event.getTimestamp(),
        duration,
        error,
        attempt,
        createdAt);
  }

  public static AbstractJobSchedulerEvent withDurableId(AbstractJobSchedulerEvent event) {
    OutboxEvent row = toRow(event, event.getTimestamp());
    return toEvent(
        new OutboxEvent(
            UuidV7Factory.create(),
            row.eventType(),
            row.jobId(),
            row.businessKey(),
            row.recurringMasterId(),
            row.jobType(),
            row.priority(),
            row.nodeId(),
            row.eventTimestamp(),
            row.executionTimeMs(),
            row.errorMessage(),
            row.retryAttempt(),
            row.createdAt()));
  }

  public static AbstractJobSchedulerEvent toEvent(OutboxEvent row) {
    return toEvent(row, row.executionTimeMs());
  }

  public static AbstractJobSchedulerEvent toEvent(OutboxEvent row, Long duration) {
    return switch (row.eventType()) {
      case JOB_COMPLETED ->
          new JobCompletedEvent(
              row.eventId(),
              row.jobId(),
              row.businessKey(),
              row.recurringMasterId(),
              row.jobType(),
              row.priority(),
              row.nodeId(),
              row.eventTimestamp(),
              duration);
      case JOB_FAILED ->
          new JobFailedEvent(
              row.eventId(),
              row.jobId(),
              row.businessKey(),
              row.recurringMasterId(),
              row.jobType(),
              row.priority(),
              row.nodeId(),
              row.eventTimestamp(),
              row.errorMessage(),
              row.retryAttempt());
      case JOB_DLQ ->
          new JobDlqEvent(
              row.eventId(),
              row.jobId(),
              row.businessKey(),
              row.recurringMasterId(),
              row.jobType(),
              row.priority(),
              row.nodeId(),
              row.eventTimestamp(),
              row.errorMessage(),
              row.retryAttempt());
    };
  }
}
